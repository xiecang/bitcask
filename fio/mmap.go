package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO, 内存映射文件 IO
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManager 初始化内存映射文件 IO
func NewMMapIOManager(filename string) (*MMap, error) {
	_, err := os.OpenFile(filename, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

func (m *MMap) Read(bytes []byte, i int64) (int, error) {
	return m.readerAt.ReadAt(bytes, i)
}

func (m *MMap) Write(bytes []byte) (int, error) {
	panic("not implemented")
}

func (m *MMap) Sync() error {
	panic("not implemented")
}

func (m *MMap) Close() error {
	return m.readerAt.Close()
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}
