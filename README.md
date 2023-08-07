# Bitcask

This is a key-value (KV) database based on Bitcask implemented in Golang.

## Introduction

Bitcask is a log-structured storage engine that writes all data to an append-only log file and uses an in-memory index for improved read performance.

The database offers the following features:

- High Performance: With the Bitcask storage engine, it provides fast read and write operations.
- Easy to Use: It offers a simple API that allows users to store and retrieve key-value pairs easily.
- Persistent Storage: Data is persisted to disk to prevent data loss.
- Concurrency Safety: It supports concurrent read and write operations and ensures data consistency using locking mechanisms.
- Transaction Support: It provides a transaction mechanism (WriteBatch) to ensure data consistency.
- Backup and Merge Support: It offers backup and merge mechanisms, and the merged data generates hint files for faster startup.
- Multiple Memory Index Support: It supports multiple memory indexes, including B-tree, Adaptive Radix Tree (ART), B+ tree, etc.
- HTTP API: It provides an HTTP API for connecting to Bitcask using an HTTP client.
- Redis Protocol Compatibility: It supports the Redis protocol, allowing connection to Bitcask using a Redis client.

## Installation

```bash
go get github.com/xiecang/bitcask
```

## Usage Example

The following example demonstrates how to use the Bitcask storage engine.

```go
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
    key = []byte("key")
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
```

## Contribution

Contributions to this project are welcome! If you find any issues or have suggestions for improvements, please raise an issue or submit a pull request.

## License

GNU General Public License v3.0
