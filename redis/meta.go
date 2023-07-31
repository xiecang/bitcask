package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)

type metadata struct {
	dataType DataType // 数据类型
	expire   int64    // 过期时间
	version  int64    // 版本号
	size     uint32   // 数据量
	head     uint64   // List 数据结构专用
	tail     uint64   // List 数据结构专用
}

func (m *metadata) encode() []byte {
	var size = maxMetadataSize
	if m.dataType == List {
		size += extraListMetaSize
	}

	buf := make([]byte, size)

	buf[0] = m.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], m.expire)
	index += binary.PutVarint(buf[index:], m.version)
	index += binary.PutVarint(buf[index:], int64(m.size))

	if m.dataType == List {
		index += binary.PutUvarint(buf[index:], m.head)
		index += binary.PutUvarint(buf[index:], m.tail)
	}

	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64
	var tail uint64

	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, n = binary.Uvarint(buf[index:])
		index += n
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte // hash key
	version int64  // 版本，固定长度，占8位
	filed   []byte // hash filed
}

func (k *hashInternalKey) encode() []byte {
	var size = len(k.key) + 8 + len(k.filed)
	buf := make([]byte, size)

	var index = 0

	// key
	index += copy(buf[index:], k.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(k.version))
	index += 8

	// field
	copy(buf[index:], k.filed)

	return buf
}