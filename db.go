package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
)

const (
	seqIdKey     = "seq.id"
	fileLockName = "bitcask.lock"
)

// DB bitcask 存储引擎
type DB struct {
	options            Options
	mu                 *sync.RWMutex
	activeFile         *data.File            // 活跃数据文件, 可以用于写入
	olderFiles         map[uint32]*data.File // 旧数据文件, 只能用于读取
	index              index.Indexer         // 内存索引
	seqId              uint64                // 事务序列号，全局递增
	isMerging          bool                  // 是否正在合并数据文件
	isInitial          bool                  // 是否已经初始化
	isSeqIdFileNotExit bool                  // 存储事务最大 id 的文件是否不存在
	fileLock           *flock.Flock          // 文件锁, 防止多个进程同时打开数据库
	bytesWrite         uint                  // 未执行 sync 前，累计写入的字节数
	reclaimSize        int64                 // 表示有多少数据是无效的
}

func fileLockPath(dirPath string) string {
	return filepath.Join(dirPath, fileLockName)
}

// Open 打开 bitcask 数据库存储引擎
func Open(options Options) (*DB, error) {
	if err := checkOptions(&options); err != nil {
		return nil, err
	}

	var isInitial bool
	// 判断数据目录是否存在，如果不存在的话就创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
		isInitial = true
	}

	// 判断当前文件是否在正在使用
	fileLock := flock.New(fileLockPath(options.DirPath))
	if hold, err := fileLock.TryLock(); err != nil {
		return nil, err
	} else if !hold {
		return nil, ErrDatabaseIsUsing
	}

	if entries, err := os.ReadDir(options.DirPath); err != nil {
		return nil, err
	} else if len(entries) == 0 {
		isInitial = true
	}

	db := DB{
		options:    options,
		mu:         &sync.RWMutex{},
		olderFiles: make(map[uint32]*data.File),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	var (
		err     error
		fileIds []int
	)
	if fileIds, err = db.loadDataFiles(); err != nil {
		return nil, err
	}

	// B+ 树不需要从数据文件中加载索引
	if options.IndexType == BPlusTree {
		// 取出当前事务序列号
		if err = db.loadSeqId(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOffset = size
		}
	} else {
		// 从 hint 文件中加载索引
		if err = db.loadIndexFromHintFile(); err != nil {
			return nil, err
		} else {
			// 跳过 hint 文件中加载过的 id
			var nonMergeFileId, err = db.getNonMergeFileId(db.options.DirPath)
			if err != nil {
				return nil, err
			}
			var newFileIds []int
			for _, id := range fileIds {
				if id < int(nonMergeFileId) {
					// 小于 nonMergeFileId 的数据记录，均在 hint 文件加载过了
					continue
				}
				newFileIds = append(newFileIds, id)
			}
			fileIds = newFileIds
		}

		// 从数据文件中加载索引
		if err = db.loadIndexFromDataFiles(fileIds); err != nil {
			return nil, err
		}

		// 重置 io 类型为标准文件 IO (如果实现了 mmap 的 write 和 sync 方法的话，也可不重置)
		if db.options.MMapAtStartup {
			if err = db.resetIOType(); err != nil {
				return nil, err
			}
		}
	}

	return &db, nil
}

// Put 写入 key-value 数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//
	record := data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqId),
		Value: value,
		Type:  data.LogRecordTypeNormal,
	}
	pos, err := db.appendLogRecordWithLock(&record)
	if err != nil {
		return err
	}
	// 更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	// 从内存数据结构中取出 key 对应的索引信息
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPosition(pos)
}

// getValueByPosition 根据索引信息读取 value
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	// 根据文件 Id 找到对应的数据文件
	var file *data.File
	if db.activeFile.Id == pos.Fid {
		file = db.activeFile
	} else {
		file = db.olderFiles[pos.Fid]
	}
	if file == nil {
		return nil, ErrFileNotFound
	}

	// 根据偏移量读取数据
	record, _, err := file.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if record.Type == data.LogRecordTypeDelete {
		return nil, ErrFileNotFound
	}
	return record.Value, nil
}

// ListKeys 列出数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())
	var i int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[i] = iterator.Key()
		i++
	}
	return keys
}

// Fold 遍历数据库中的所有 key-value, fn 返回 true 时继续遍历，返回 false 时停止遍历
func (db *DB) Fold(fn func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		pos := iterator.Value()
		value, err := db.getValueByPosition(pos)
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) saveSeqIdToFile() error {
	seqIdFile, err := data.OpenSeqIdFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := data.LogRecord{
		Key:   []byte(seqIdKey),
		Value: []byte(strconv.FormatUint(db.seqId, 10)),
		Type:  data.LogRecordTypeSeqId,
	}
	encodeRecord, _ := data.EncodeLogRecord(&record)
	if err = seqIdFile.Write(encodeRecord); err != nil {
		return err
	}
	if err = seqIdFile.Sync(); err != nil {
		return err
	}
	if err = seqIdFile.Close(); err != nil {
		return err
	}
	return err
}

func (db *DB) loadSeqId() error {
	if data.IsSeqIdFileNotExit(db.options.DirPath) {
		db.isSeqIdFileNotExit = true
		return nil
	}
	seqIdFile, err := data.OpenSeqIdFile(db.options.DirPath)
	if err != nil {
		return err
	}
	r, _, err := seqIdFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	id, err := strconv.ParseInt(string(r.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqId = uint64(id)
	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("unlock file lock failed: %s", err))
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.index.Close(); err != nil {
		return err
	}

	// 保存当前事务序列号
	if err := db.saveSeqIdToFile(); err != nil {
		return err
	}

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

// Delete 根据 key 删除对应的数据
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先检查 key 是否存在，如果不存在的话就直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造删除数据的记录
	record := data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqId),
		Type: data.LogRecordTypeDelete,
	}
	// 写入到数据文件中
	pos, err := db.appendLogRecordWithLock(&record)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)

	// 从内存索引中将对应的 key 删除
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) shouldSync() bool {
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPreSync > 0 && db.bytesWrite > db.options.BytesPreSync {
		needSync = true
	}
	return needSync
}

// appendLogRecord 追加写数据到活跃数据文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {

	// 判断当前活跃数据文件是否存在
	if db.activeFile == nil {
		if err := db.setActivateDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encodedRecord, size := data.EncodeLogRecord(record)
	// 如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOffset+size > db.options.MaxFileSize {
		// 先持久化数据文件，保证已有数据持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧文件
		db.olderFiles[db.activeFile.Id] = db.activeFile

		// 打开新的数据文件
		if err := db.setActivateDataFile(); err != nil {
			return nil, err
		}
	}

	writeOffset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encodedRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)
	if db.shouldSync() {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 清空累计写入值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.Id,
		Offset: writeOffset,
	}
	return pos, nil
}

// appendLogRecordWithLock 追加写数据到活跃数据文件中
func (db *DB) appendLogRecordWithLock(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(record)
}

// setActivateDataFile 设置当前活跃数据文件
// 在访问此方法时，需要持有互斥锁
func (db *DB) setActivateDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.Id + 1
	}
	// 打开新的数据文件
	file, err := data.OpenFile(db.options.DirPath, initialFileId, fio.FIOStandar)
	if err != nil {
		return err
	}
	db.activeFile = file
	return nil
}
func (db *DB) loadDataFiles() ([]int, error) {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return nil, err
	}
	var fileIds []int
	// 遍历数据目录下的文件，找到所有以 .data 结尾的数据文件
	for _, entry := range dirEntries {
		if !strings.HasSuffix(entry.Name(), data.FileNameSuffix) {
			continue
		}
		// 00000001.data
		splitNames := strings.Split(entry.Name(), ".")
		fileId, err := strconv.Atoi(splitNames[0])
		if err != nil {
			// 数据目录有可能被损坏了
			return nil, ErrDataDirectoryCorrupted
		}
		fileIds = append(fileIds, fileId)
	}

	// 对文件 Id 进行排序，从小到大依次加载
	sort.Ints(fileIds)

	// 遍历文件 Id，加载数据文件
	for i, fileId := range fileIds {
		ioType := fio.FIOStandar
		if db.options.MMapAtStartup {
			ioType = fio.FIOMemoryMap
		}
		file, err := data.OpenFile(db.options.DirPath, uint32(fileId), ioType)
		if err != nil {
			return nil, err
		}
		if i == len(fileIds)-1 {
			// 最后一个文件作为活跃文件
			db.activeFile = file
		} else {
			// 其他文件作为旧文件
			db.olderFiles[file.Id] = file
		}
	}
	return fileIds, nil
}

// loadIndexFromDataFiles 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles(fileIds []int) error {
	if len(fileIds) == 0 {
		return nil
	}

	var updateIndex = func(key []byte, tp data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if tp == data.LogRecordTypeDelete {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentTransactionId uint64 = nonTransactionSeqId

	// 遍历所有的数据文件
	for _, fid := range fileIds {
		var fileId = uint32(fid)
		var file *data.File
		if fileId == db.activeFile.Id {
			file = db.activeFile
		} else {
			file = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			record, size, err := file.ReadLogRecord(offset)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}

			// 构造内存索引信息
			pos := &data.LogRecordPos{
				Fid:    file.Id,
				Offset: offset,
				Size:   uint32(size),
			}

			// 解析 key, 拿到事务序列号
			realKey, seqId := parsedLogRecordKey(record.Key)
			if seqId == nonTransactionSeqId {
				// 非事务记录，直接更新索引
				updateIndex(realKey, record.Type, pos)
			} else {
				if record.Type == data.LogRecordTypeTransactionFinished {
					for _, r := range transactionRecords[seqId] {
						updateIndex(r.Record.Key, r.Record.Type, r.Pos)
					}
					delete(transactionRecords, seqId)
				} else {
					record.Key = realKey
					transactionRecords[seqId] = append(transactionRecords[seqId], &data.TransactionRecord{
						Record: record,
						Pos:    pos,
					})
				}
			}

			// 更新事务序列号
			if seqId > currentTransactionId {
				currentTransactionId = seqId
			}

			//
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 writeOffset
		if fileId == db.activeFile.Id {
			db.activeFile.WriteOffset = offset
		}
	}

	// 更新当前事务序列号
	db.seqId = currentTransactionId
	return nil
}

// resetIOType 将数据文件的 io 类型重置为标准文件 IO
func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.FIOStandar); err != nil {
		return err
	}

	for _, file := range db.olderFiles {
		if err := file.SetIOManager(db.options.DirPath, fio.FIOStandar); err != nil {
			return err
		}
	}
	return nil
}

func checkOptions(options *Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.MaxFileSize <= 0 {
		return errors.New("database data file must be greater than 0")
	}
	return nil
}
