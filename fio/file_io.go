package fio

import "os"

// FileIo 标准系统文件 IO
type FileIo struct {
	// fd 系统文件描述符
	fd *os.File
}

// NewFileManager 初始化标准文件 IO
func NewFileManager(filename string) (*FileIo, error) {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIo{
		fd: fd,
	}, nil
}

func (fio *FileIo) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIo) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIo) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIo) Close() error {
	return fio.fd.Close()
}
