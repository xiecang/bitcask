package bitcask_go

type Options struct {
	DirPath     string // 数据库数据目录
	MaxFileSize int64  // 数据文件大小

	SyncWrites bool // 是否同步写入，true 时每次写入都会持久化到磁盘当中

	IndexType IndexType // 索引类型
}

type IteratorOption struct {
	Prefix  []byte // 遍历前缀为指定 key 的值，默认为空，即遍历所有 key
	Reverse bool   // 是否反向迭代
}

type IndexType = int8

const (
	Btree IndexType = iota + 1 // B+ 树索引
	ART                        // Adaptive Radix Tree 自适应基数树索引
)
