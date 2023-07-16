package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTree 索引，封装了 google btree
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: &sync.RWMutex{},
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	i := Item{
		Key: key,
		Pos: pos,
	}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(&i)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	i := Item{
		Key: key,
	}
	btreeItem := bt.tree.Get(&i)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).Pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := Item{
		Key: key,
	}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(&it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	return nil
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// bTreeIterator BTree 索引迭代器
type bTreeIterator struct {
	currIndex int     // 当前遍历到的索引位置
	reverse   bool    // 是否逆序遍历
	values    []*Item // 遍历的元素(key + 位置索引信息)
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *bTreeIterator {
	var index int
	var values = make([]*Item, tree.Len())

	var saveValues = func(item btree.Item) bool {
		values[index] = item.(*Item)
		index++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &bTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}
func (b *bTreeIterator) Rewind() {
	b.currIndex = 0
}

func (b *bTreeIterator) Seek(key []byte) {
	if b.reverse {
		b.currIndex = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].Key, key) <= 0
		})
	} else {
		b.currIndex = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].Key, key) >= 0
		})
	}
}

func (b *bTreeIterator) Next() {
	b.currIndex++
}

func (b *bTreeIterator) Valid() bool {
	return b.currIndex < len(b.values)
}

func (b *bTreeIterator) Key() []byte {
	return b.values[b.currIndex].Key
}

func (b *bTreeIterator) Value() *data.LogRecordPos {
	return b.values[b.currIndex].Pos
}

func (b *bTreeIterator) Close() {
	b.values = nil
}
