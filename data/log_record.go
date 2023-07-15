package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordTypeNormal LogRecordType = iota
	LogRecordTypeDelete
	LogRecordTypeTransactionFinished
	LogRecordTypeHint
)

// crc type keySize valueSize key value
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

// TransactionRecord 暂存事务记录
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对 LogRecord 进行编码，返回编码后的字节数组和字节数组的长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节存储 Type
	header[4] = byte(record.Type)
	var index = 5
	// 5 字节之后存储 key、value 的长度
	index += binary.PutVarint(header[index:], int64(len(record.Key)))
	index += binary.PutVarint(header[index:], int64(len(record.Value)))

	var size = int64(index) + int64(len(record.Key)) + int64(len(record.Value))
	encoded := make([]byte, size)

	// 将 header 拷贝到 encoded 中
	copy(encoded[:index], header[:index])
	// 将 key 拷贝到 encoded 中
	copy(encoded[index:], record.Key)
	// 将 value 拷贝到 encoded 中
	copy(encoded[index+len(record.Key):], record.Value)

	// 计算 crc 校验值
	crc := crc32.ChecksumIEEE(encoded[4:])
	// 小端序存储 crc 校验值
	binary.LittleEndian.PutUint32(encoded[:4], crc)
	return encoded, size
}

// 对字节数组中的 Header 信息进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	var n = crc32.Size
	if len(buf) <= n {
		return nil, 0
	}
	header := logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:n]),
		recordType: buf[n],
	}
	var index = 5
	// 读取 key 和 value 的长度
	var keySize, keyLen = binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += keyLen
	//
	var valueSize, valueLen = binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += valueLen

	return &header, int64(index)
}

func getLogRecordCRC(record *LogRecord, header []byte) uint32 {
	if record == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, record.Key)
	crc = crc32.Update(crc, crc32.IEEETable, record.Value)
	return crc
}

// EncodeLogRecordPos 对 LogRecordPos 进行编码，返回编码后的字节数组和字节数组的长度
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutUvarint(buf[index:], uint64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	//return buf[:index], int64(index)
	return buf[:index]
}

// DecodeLogRecordPos 对字节数组进行解码，返回 LogRecordPos
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fid, n := binary.Uvarint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(fid),
		Offset: offset,
	}
}
