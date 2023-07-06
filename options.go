package bitcask_go

type Options struct {
	DirPath     string // 数据库数据目录
	MaxFileSize int64  // 数据文件大小

	SyncWrites bool // 是否同步写入，true 时每次写入都会持久化到磁盘当中
}
