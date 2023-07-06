package data

import "bitcask-go/fio"

const FileNameSuffix = ".data"

// File 数据文件
type File struct {
	// Id 文件 id
	Id uint32
	// WriteOffset 文件写入的偏移量
	WriteOffset int64
	// IOManager IO 读写管理器
	IOManager fio.IOManager
}

func OpenFile(dirPath string, fileId uint32) (*File, error) {
	panic("implement me")
}

func (f *File) Write(buf []byte) error {
	panic("implement me")
}

func (f *File) Sync() error {
	panic("implement me")
}

func (f *File) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	panic("implement me")
}
