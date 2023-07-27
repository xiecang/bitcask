package bitcask_go

type Options struct {
	DirPath string // 数据库数据目录

	MaxFileSize int64 // 数据文件大小

	SyncWrites bool // 是否同步写入，true 时每次写入都会持久化到磁盘当中

	BytesPreSync uint // 每写入指定字节数后同步到磁盘

	IndexType IndexType // 索引类型

	MMapAtStartup bool // 是否在启动时将索引文件映射到内存当中

	DataFileMergeThreshold float32 // 数据文件合并阈值, 无效数据文件占总数据文件大小的比例超过该阈值时触发合并
}

type IteratorOption struct {
	Prefix  []byte // 遍历前缀为指定 key 的值，默认为空，即遍历所有 key
	Reverse bool   // 是否反向迭代
}

// WriteBatchOption 批量写入配置项
type WriteBatchOption struct {
	MaxBatchSize uint // 最大批量写入大小

	SyncWrites bool // 是否同步写入，true 时每次写入都会持久化到磁盘当中
}
type IndexType = int8

const (
	Btree     IndexType = iota + 1 // B+ 树索引
	ART                            // Adaptive Radix Tree 自适应基数树索引
	BPlusTree                      // B+ 树索引, 将索引数据存储在磁盘当中
)
