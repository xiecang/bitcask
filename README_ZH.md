# Bitcask

这是一个使用 Golang 实现的基于 [Bitcask](https://riak.com/assets/bitcask-intro.pdf) 的键值（KV）数据库。

## 简介

Bitcask 是一种基于日志结构的存储引擎，它将所有数据写入一个追加日志文件，并使用内存索引以提高读取性能。

该数据库具有以下特性：

- 高性能：使用 Bitcask 存储引擎，提供快速的读写操作。
- 简单易用：提供简单的 API，使用户可以轻松地存储和检索键值对。
- 持久化存储：数据会被持久化到磁盘，以防止数据丢失。
- 并发安全：支持并发读写操作，并使用锁机制确保数据的一致性。
- 支持事务：提供事务机制 (WriteBatch)，确保数据的一致性。
- 支持备份和合并：提供备份和合并机制，合并后的数据生成 hint 文件，加速启动。
- 支持多种内存索引：支持多种内存索引，包括 B 树、自适应基树（ART）、B+树等。
- 提供 HTTP API：提供 HTTP API，可使用 HTTP 客户端连接 Bitcask
- 兼容 Redis 协议：支持 Redis 协议，可使用 Redis 客户端连接 Bitcask。

## 安装

```bash
go get github.com/xiecang/bitcask
```

## 使用示例

以下示例演示了如何使用 Bitcask 存储引擎。

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

## 贡献

欢迎对该项目进行贡献！如果你发现了问题或者有改进的建议，请提出 issue 或者提交 pull 请求。

## 许可证

[GNU General Public License v3.0](./LICENSE)
