package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/xiecang/bitcask/data"
)

// Indexer 抽象索引接口
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息, 返回旧的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	// Get 从索引中获取 key 对应的数据位置信息
	Get(key []byte) *data.LogRecordPos
	// Delete 从索引中删除 key 对应的数据位置信息, 返回旧的数据位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)
	// Size 返回索引中元素的数量
	Size() int
	// Iterator 返回一个迭代器，用于遍历索引中的所有元素
	Iterator(reverse bool) Iterator
	// Close 关闭索引
	Close() error
}

type IndexType = int8

const (
	Btree IndexType = iota + 1

	ART // Adaptive Radix Tree 自适应基数树索引
	BPT // B+ 树索引
)

// NewIndexer 根据类型初始化索引
func NewIndexer(tp IndexType, dirPath string, sync bool) Indexer {
	switch tp {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPT:
		return NewBPlusTree(dirPath, sync)
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
