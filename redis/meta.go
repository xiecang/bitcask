package redis

import (
	"bitcask-go/utils"
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

type setInternalKey struct {
	key     []byte // key
	version int64  // 版本，固定长度，占 8 字节
	member  []byte // 值
	// member size 占据 4 字节
}

func (k *setInternalKey) encode() []byte {
	var size = len(k.key) + 8 + len(k.member) + 4
	buf := make([]byte, size)

	var index = 0

	// key
	index += copy(buf[index:], k.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(k.version))
	index += 8

	// member
	copy(buf[index:index+len(k.member)], k.member)
	index += len(k.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(len(k.member)))

	return buf
}

type listInternalKey struct {
	key     []byte // key
	version int64  // 版本，固定长度，占 8 字节
	index   uint64 // 索引, 8 字节
}

func (l *listInternalKey) encode() []byte {
	var size = len(l.key) + 8 + 8
	buf := make([]byte, size)

	var index = 0

	// key
	index += copy(buf[index:], l.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(l.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buf[index:index+8], l.index)
	index += 8

	return buf
}

type zsetInternalKey struct {
	key     []byte // key
	version int64  // 版本，固定长度，占 8 字节
	member  []byte // 值
	// member size 占据 4 字节
	score float64 // 分数
}

func (k *zsetInternalKey) encodeWithMember() []byte {
	var size = len(k.key) + len(k.member) + 8
	buf := make([]byte, size)

	var index = 0

	// key
	index += copy(buf[index:], k.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(k.version))
	index += 8

	// member
	copy(buf[index:index+len(k.member)], k.member)

	return buf
}

func (k *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(k.score)
	var size = len(k.key) + len(k.member) + len(scoreBuf) + 8 + 4
	buf := make([]byte, size)

	var index = 0

	// key
	index += copy(buf[index:], k.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(k.version))
	index += 8

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buf[index:index+len(k.member)], k.member)
	index += len(k.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(len(k.member)))

	return buf
}
