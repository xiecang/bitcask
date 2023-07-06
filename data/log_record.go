package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// crc type keysSize valueSize key value
// 4   1    5(max)   5(max)    n    m
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 1 + 4

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据是追加写入的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id
	Offset int64  // 文件中的偏移量
}

// EncodingRecord 对 LogRecord 进行编码，返回编码后的字节数组和字节数组的长度
func EncodingRecord(record *LogRecord) ([]byte, int64) {
	panic("implement me")
}
