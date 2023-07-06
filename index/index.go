package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 抽象索引接口
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(Key []byte, pos *data.LogRecordPos) bool
	// Get 从索引中获取 key 对应的数据位置信息
	Get(Key []byte) *data.LogRecordPos
	// Delete 从索引中删除 key 对应的数据位置信息
	Delete(key []byte) bool
}

type IndexType = int8

const (
	Btree IndexType = iota + 1

	ART // Adaptive Radix Tree 自适应基数树索引
)

// NewIndexer 根据类型初始化索引
func NewIndexer(tp IndexType) Indexer {
	switch tp {
	case Btree:
		return NewBTree()
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	Key []byte
	Pos *data.LogRecordPos
}

func (i *Item) Less(b btree.Item) bool {
	return bytes.Compare(i.Key, b.(*Item).Key) == -1
}
