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
	// Size 返回索引中元素的数量
	Size() int
	// Iterator 返回一个迭代器，用于遍历索引中的所有元素
	Iterator(reverse bool) Iterator
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

// Iterator 通用的索引迭代器
type Iterator interface {
	Rewind()                   // 重置迭代器，使其指向索引中的第一个元素
	Seek(key []byte)           // 重置迭代器，使其指向索引中第一个大于(或小于) key 的元素
	Next()                     // 使迭代器指向索引中的下一个元素
	Valid() bool               // 判断迭代器是否有效
	Key() []byte               // 遍历位置的 key
	Value() *data.LogRecordPos // 遍历位置的 value
	Close()                    // 关闭迭代器
}
