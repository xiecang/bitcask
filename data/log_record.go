package data

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id
	Offset uint32 // 文件中的偏移量
}
