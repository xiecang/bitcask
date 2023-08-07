package main

import (
	"fmt"
	bitcask "github.com/xiecang/bitcask"
)

func main() {
	var options = bitcask.Options{
		DirPath:     "/tmp/bitcask-go",
		MaxFileSize: 256 * 1024 * 1024,
		SyncWrites:  false,
		IndexType:   bitcask.BTree,
	}
	db, err := bitcask.Open(options)
	if err != nil {
		panic(err)
	}

	var (
		key   = []byte("key")
		value = []byte("bitcask-go")
	)

	// write data
	if err = db.Put(key, value); err != nil {
		panic(err)
	}

	// read data
	v, err := db.Get(key)
	if err != nil {
		panic(err)
	}

	fmt.Printf("set value: %s, get value: %s\n", value, v)

	// delete data
	if err = db.Delete(key); err != nil {
		panic(err)
	}

	// read data
	v, err = db.Get(key)
	fmt.Printf("got error: %v\n", err)
}
