package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqId = 0

var keyTransactionFinished = []byte("$txn_fin$")

// WriteBatch 原子批量写
type WriteBatch struct {
	options       WriteBatchOption
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 待写入的数据
}

func (db *DB) NewWriteBatch(options WriteBatchOption) *WriteBatch {
	return &WriteBatch{
		options:       options,
		mu:            &sync.Mutex{},
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 添加待批量写入的数据
func (w *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	// 暂存待写入的数据
	record := data.LogRecord{
		Key:   key,
		Value: value,
	}
	w.pendingWrites[string(key)] = &record
	return nil
}

// Delete 添加待批量删除的数据
func (w *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	// 数据不存在，直接返回
	pos := w.db.index.Get(key)
	if pos == nil {
		if w.pendingWrites[string(key)] != nil {
			delete(w.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存待删除的数据
	record := data.LogRecord{
		Key:  key,
		Type: data.LogRecordTypeDelete,
	}
	w.pendingWrites[string(key)] = &record
	return nil
}

// Commit 提交事务，将暂存数据写入数据文件，并更新内存索引
func (w *WriteBatch) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.pendingWrites) == 0 {
		return nil
	}

	if uint(len(w.pendingWrites)) > w.options.MaxBatchSize {
		return ErrExceedMaxBatchSize
	}

	// 加锁保证事务提交串行化
	w.db.mu.Lock()
	defer w.db.mu.Unlock()
	// 获取当前最新的事务序列号
	seqId := atomic.AddUint64(&w.db.seqId, 1)

	// write
	var positions = make(map[string]*data.LogRecordPos)
	for _, record := range w.pendingWrites {
		pos, err := w.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqId),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = pos
	}

	// 写入一条标识事务完成的数据
	finishedRecord := data.LogRecord{
		Key:  logRecordKeyWithSeq(keyTransactionFinished, seqId),
		Type: data.LogRecordTypeTransactionFinished,
	}
	if _, err := w.db.appendLogRecord(&finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否立即刷新数据文件
	if w.options.SyncWrites && w.db.activeFile != nil {
		if err := w.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range w.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordTypeDelete {
			w.db.index.Delete(record.Key)
		} else if record.Type == data.LogRecordTypeNormal {
			w.db.index.Put(record.Key, pos)
		}
	}

	// 清空待写入数据
	w.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// logRecordKeyWithSeq key + logRecordSeqId => encodeKey
func logRecordKeyWithSeq(key []byte, seqId uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqId)

	encodeKey := make([]byte, len(key)+n)
	copy(encodeKey[:n], seq[:n])
	copy(encodeKey[n:], key)

	return encodeKey
}

// parsedLogRecordKey 解析事务日志记录的key，返回原始key和事务序列号
func parsedLogRecordKey(key []byte) ([]byte, uint64) {
	seqId, n := binary.Uvarint(key)
	return key[n:], seqId
}
