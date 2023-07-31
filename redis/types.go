package redis

import (
	bitcask "bitcask-go"
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the kind of value")
)

type DataType = byte

const (
	String DataType = iota
	Hash
	Set
	List
	ZSet
)

// DataStructure redis 数据结构
type DataStructure struct {
	db *bitcask.DB
}

func NewDataStructure(options bitcask.Options) (*DataStructure, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &DataStructure{
		db: db,
	}, nil
}

// ================================ String 数据结构 ============================

func (d *DataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码： value: type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encodeValue := make([]byte, index+len(value))
	copy(encodeValue[:index], buf[:index])
	copy(encodeValue[index:], value)

	// 调用存储引擎接口，写入数据
	return d.db.Put(key, encodeValue)
}

func (d *DataStructure) Get(key []byte) ([]byte, error) {
	encodeValue, err := d.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	dType := encodeValue[0]
	if dType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encodeValue[index:])
	index += n
	// 判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	return encodeValue[index:], nil
}
