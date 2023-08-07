package index

import (
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"github.com/xiecang/bitcask/data"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基树索引
// 主要封装了 https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	defer art.lock.Unlock()
	oldValue, _ := art.tree.Insert(key, pos)
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	v, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return v.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	defer art.lock.Unlock()
	oldValue, deleted := art.tree.Delete(key)
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return art.tree.Size()
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// artIterator ART 索引迭代器
type artIterator struct {
	currIndex int     // 当前遍历到的索引位置
	reverse   bool    // 是否逆序遍历
	values    []*Item // 遍历的元素(key + 位置索引信息)
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var index int
	if reverse {
		index = tree.Size() - 1
	}
	var values = make([]*Item, tree.Size())

	var saveValues = func(node goart.Node) bool {
		values[index] = &Item{
			Key: node.Key(),
			Pos: node.Value().(*data.LogRecordPos),
		}
		if reverse {
			index--
		} else {
			index++
		}
		return true
	}
	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (ai *artIterator) Rewind() {
	ai.currIndex = 0
}

func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].Key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].Key, key) >= 0
		})
	}
}

func (ai *artIterator) Next() {
	ai.currIndex++
}

func (ai *artIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

func (ai *artIterator) Key() []byte {
	return ai.values[ai.currIndex].Key
}

func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].Pos
}

func (ai *artIterator) Close() {
	ai.values = nil
}
