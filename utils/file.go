package utils

import (
	"os"
	"path/filepath"
	"strings"
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

// CopyDir 拷贝目录
func CopyDir(src, dest string, exclude []string) error {
	// 目标文件夹不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		filename := strings.Replace(path, src, "", 1)
		if filename == "" {
			return nil
		}

		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, filename), os.ModePerm)
		}

		data, err := os.ReadFile(filepath.Join(src, filename))
		if err != nil {
			return err
		}

		return os.WriteFile(filepath.Join(dest, filename), data, info.Mode())
	})

	return err
}
