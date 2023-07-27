package utils

import (
	"os"
	"path/filepath"
	"syscall"
)

// DirSize 获取指定目录占据磁盘的大小
func DirSize(dir string) (size int64, err error) {
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info == nil {
			return nil
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return
}

// AvailableDiskSpace 获取当前磁盘可用空间
func AvailableDiskSpace() (size uint64, err error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	fs := syscall.Statfs_t{}
	err = syscall.Statfs(wd, &fs)
	size = fs.Bavail * uint64(fs.Bsize)
	return
}
