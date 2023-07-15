package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎
type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.File            // 活跃数据文件, 可以用于写入
	olderFiles map[uint32]*data.File // 旧数据文件, 只能用于读取
	index      index.Indexer         // 内存索引
	seqId      uint64                // 事务序列号，全局递增
}

// Open 打开 bitcask 数据库存储引擎
func Open(options Options) (*DB, error) {
	if err := checkOptions(&options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在的话就创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := DB{
		options:    options,
		mu:         &sync.RWMutex{},
		olderFiles: make(map[uint32]*data.File),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	var (
		err     error
		fileIds []int
	)
	if fileIds, err = db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err = db.loadIndexFromDataFiles(fileIds); err != nil {
		return nil, err
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
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecordWithLock(&record)
	if err != nil {
		return err
	}
	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
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

	if record.Type == data.LogRecordDelete {
		return nil, ErrFileNotFound
	}
	return record.Value, nil
}

// ListKeys 列出数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
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

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

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
		Type: data.LogRecordDelete,
	}
	// 写入到数据文件中
	_, err := db.appendLogRecordWithLock(&record)
	if err != nil {
		return err
	}
	// 从内存索引中将对应的 key 删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
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

	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
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
	file, err := data.OpenFile(db.options.DirPath, initialFileId)
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
		file, err := data.OpenFile(db.options.DirPath, uint32(fileId))
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
		var ok bool
		if tp == data.LogRecordDelete {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
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
			}

			// 解析 key, 拿到事务序列号
			realKey, seqId := parsedLogRecordKey(record.Key)
			if seqId == nonTransactionSeqId {
				// 非事务记录，直接更新索引
				updateIndex(realKey, record.Type, pos)
			} else {
				if record.Type == data.LogRecordTransactionFinished {
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

func checkOptions(options *Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.MaxFileSize <= 0 {
		return errors.New("database data file must be greater than 0")
	}
	return nil
}
