package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"os"
	"sync"
)

// DB bitcask 存储引擎
type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.File            // 活跃数据文件, 可以用于写入
	olderFiles map[uint32]*data.File // 旧数据文件, 只能用于读取
	index      index.Indexer         // 内存索引
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

	return &db, nil
}

// Put 写入 key-value 数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//
	record := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecord(&record)
	if err != nil {
		return err
	}
	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// appendLogRecord 追加写数据到活跃数据文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃数据文件是否存在
	if db.activeFile == nil {
		if err := db.setActivateDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encodedRecord, size := data.EncodingRecord(record)
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
	record, err := file.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if record.Type == data.LogRecordDelete {
		return nil, ErrFileNotFound
	}
	return record.Value, nil
}

func (db *DB) loadDataFiles() error {
	panic("implement me")
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