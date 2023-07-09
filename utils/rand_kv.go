package utils

import (
	"bitcask-go/data"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

var (
	letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
)

// GetTestKey 获取测试用的 key
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-go-key-%010d", i))
}

// RandomValue 生成随机长度的 value，用于测试
func RandomValue(length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return b
}

func CleanDBFile(path string) error {
	//err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
	//	if err != nil {
	//		return err
	//	}
	//
	//	// 判断是否是 .a 后缀的文件
	//	if !info.IsDir() && filepath.Ext(path) == FileNameSuffix {
	//      path := filepath.Join(root, file.Name())
	//		if err = os.Remove(path); err != nil {
	//			return err
	//		}
	//		fmt.Printf("Deleted file: %s\n", path)
	//	}
	//
	//	return nil
	//})
	files, err := os.ReadDir(path)
	if err != nil {
		fmt.Printf("Error reading directory %q: %v\n", path, err)
		return err
	}
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == data.FileNameSuffix {
			p := filepath.Join(path, file.Name())
			if err = os.Remove(p); err != nil {
				return err
			}
			fmt.Printf("Deleted file: %s\n", p)
		}
	}
	return err
}
