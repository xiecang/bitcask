package index

import (
	"bitcask-go/data"
	"github.com/google/btree"
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

func (bt *BTree) Put(Key []byte, pos *data.LogRecordPos) bool {
	i := Item{
		Key: Key,
		Pos: pos,
	}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(&i)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(Key []byte) *data.LogRecordPos {
	i := Item{
		Key: Key,
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
