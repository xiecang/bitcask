package bitcask_go

import (
	"bytes"
	"github.com/xiecang/bitcask/index"
)

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator  // 索引迭代器
	db        *DB             // 数据库
	option    *IteratorOption // 迭代器选项
}

func (db *DB) NewIterator(opt *IteratorOption) *Iterator {
	indexIter := db.index.Iterator(opt.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		option:    opt,
	}
}

func (i *Iterator) Rewind() {
	i.indexIter.Rewind()
	i.skipToNext()
}

func (i *Iterator) Seek(key []byte) {
	i.indexIter.Seek(key)
	i.skipToNext()
}

func (i *Iterator) Next() {
	i.indexIter.Next()
	i.skipToNext()
}

func (i *Iterator) Valid() bool {
	return i.indexIter.Valid()
}

func (i *Iterator) Key() []byte {
	return i.indexIter.Key()
}

func (i *Iterator) Value() ([]byte, error) {
	pos := i.indexIter.Value()
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	i.db.mu.RLock()
	defer i.db.mu.RUnlock()
	return i.db.getValueByPosition(pos)
}

func (i *Iterator) Close() {
	i.indexIter.Close()
}

func (i *Iterator) skipToNext() {
	var prefixLen = len(i.option.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; i.indexIter.Valid(); i.indexIter.Next() {
		key := i.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(key[:prefixLen], i.option.Prefix) == 0 {
			break
		}
	}
	return
}
