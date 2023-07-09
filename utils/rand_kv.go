package utils

import (
	"fmt"
	"math/rand"
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
