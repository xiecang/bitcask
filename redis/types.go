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

// ================================ Hash 数据结构 ============================

// findMetadata 查找元数据, 如果不存在/过期则创建，如果存在但类型不匹配则返回错误
func (d *DataStructure) findMetadata(key []byte, dataType DataType) (*metadata, error) {
	metaBuf, err := d.db.Get(key)
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)

		//
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}

		//
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

func (d *DataStructure) HSet(key, field, value []byte) (bool, error) {
	if len(key) == 0 {
		// redis 本身是支持 key 为空的
		// 但是当前 bitcask 实现不支持 Get 空 key, 所以暂不支持 HSet 空 key
		return false, errors.New("key or field is nil")
	}
	meta, err := d.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造 Hash 数据部分的 key
	hkey := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}

	encodedKey := hkey.encode()

	// 先查找是否存在
	var exist = true
	if _, err = d.db.Get(encodedKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	wb := d.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	// 不存在则更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encodedKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (d *DataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := d.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, err
	}

	//
	hkey := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}

	value, err := d.db.Get(hkey.encode())
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		// redis 中 hash 不存在的 field 返回 nil
		return nil, nil
	}
	return value, err
}

func (d *DataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := d.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, err
	}
	//
	hkey := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}

	encodeKey := hkey.encode()

	var exist = true
	if _, err = d.db.Get(encodeKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}
	if exist {
		wb := d.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encodeKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}

// ================================ Set 数据结构 ============================

func (d *DataStructure) SAdd(key []byte, members ...[]byte) (int64, error) {
	// 查找元数据
	meta, err := d.findMetadata(key, Set)
	if err != nil {
		return 0, err
	}

	var count int64

	for _, member := range members {
		// 构造 Set 数据部分的 key
		skey := &setInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
		}
		if _, err = d.db.Get(skey.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
			// 不存在则更新元数据
			meta.size++
			wb := d.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)

			_ = wb.Put(key, meta.encode())
			_ = wb.Put(skey.encode(), nil)
			if err = wb.Commit(); err != nil {
				return 0, err
			}
			count++
		}

	}

	return count, nil

}

func (d *DataStructure) SIsMember(key []byte, member []byte) (bool, error) {
	// 查找元数据
	meta, err := d.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	// 构造 Set 数据部分的 key
	skey := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = d.db.Get(skey.encode())
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}
	return true, err
}
func (d *DataStructure) SRem(key []byte, member []byte) (bool, error) {
	// 查找元数据
	meta, err := d.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	// 构造 Set 数据部分的 key
	skey := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	encodeKey := skey.encode()

	if _, err = d.db.Get(encodeKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}

	//
	wb := d.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(encodeKey)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
