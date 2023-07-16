package index

import (
	"bitcask-go/data"
	"fmt"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bPlusTreeIndexFileName = "bplustree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree B+ 树索引，封装了
// 主要封装了 https://github.com/etcd-io/bbolt
type BPlusTree struct {
	tree *bbolt.DB
}

func getBPlusTreeIndexFilePath(dirPath string) string {
	return filepath.Join(dirPath, bPlusTreeIndexFileName)
}
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	var p = getBPlusTreeIndexFilePath(dirPath)
	var opts = bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	tree, err := bbolt.Open(p, 0644, opts)
	if err != nil {
		panic("failed to open bplustree index")
	}

	// 创建 bucket
	if err = tree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bplustree index")
	}

	return &BPlusTree{
		tree: tree,
	}

}

func (b *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if key == nil {
		return false
	}
	if err := b.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		var v []byte
		if pos != nil {
			v = data.EncodeLogRecordPos(pos)
		}
		return bucket.Put(key, v)
	}); err != nil {
		panic(fmt.Errorf("failed to put value in bplustree index: %w", err))
	}
	return true
}

func (b *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var value *data.LogRecordPos
	if err := b.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		v := bucket.Get(key)
		if len(v) != 0 {
			value = data.DecodeLogRecordPos(v)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bplustree index")
	}
	return value
}

func (b *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := b.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bplustree index")
	}
	return ok
}

func (b *BPlusTree) Size() int {
	var size int
	if err := b.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bplustree index")
	}
	return size
}

func (b *BPlusTree) Iterator(reverse bool) Iterator {
	return newBPlusTreeIterator(b.tree, reverse)
}

func (b *BPlusTree) Close() error {
	return b.tree.Close()
}

// bPlusTreeIterator b+ 树迭代器
type bPlusTreeIterator struct {
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor
	reverse bool

	currentKey   []byte
	currentValue []byte
}

func newBPlusTreeIterator(tree *bbolt.DB, reverse bool) *bPlusTreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin tx in bplustree iterator")
	}
	b := &bPlusTreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	b.Rewind()
	return b
}

func (b *bPlusTreeIterator) Rewind() {
	if b.reverse {
		b.currentKey, b.currentValue = b.cursor.Last()
	} else {
		b.currentKey, b.currentValue = b.cursor.First()
	}
}

func (b *bPlusTreeIterator) Seek(key []byte) {
	b.currentKey, b.currentValue = b.cursor.Seek(key)
}

func (b *bPlusTreeIterator) Next() {
	if b.reverse {
		b.currentKey, b.currentValue = b.cursor.Prev()
	} else {
		b.currentKey, b.currentValue = b.cursor.Next()
	}
}

func (b *bPlusTreeIterator) Valid() bool {
	return b.currentKey != nil
}

func (b *bPlusTreeIterator) Key() []byte {
	return b.currentKey
}

func (b *bPlusTreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(b.currentValue)
}

func (b *bPlusTreeIterator) Close() {
	_ = b.tx.Rollback()
}
