package benchmark

import (
	"errors"
	bitcask "github.com/xiecang/bitcask"
	"github.com/xiecang/bitcask/utils"
	"golang.org/x/exp/rand"
	"testing"
	"time"
)

var (
	db *bitcask.DB
)

func init() {
	var err error
	options := bitcask.DefaultOptions

	db, err = bitcask.Open(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	b.Run("Put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			if err != nil {
				b.Errorf("Put() error = %v", err)
			}
		}
	})

}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		if err != nil {
			b.Errorf("Put() error = %v", err)
		}
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := db.Get(utils.GetTestKey(rand.Int()))
			if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
				b.Errorf("Get() error = %v", err)
			}
		}
	})
}

func Benchmark_Delete(b *testing.B) {
	rand.Seed(uint64(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()
	b.Run("Delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := db.Delete(utils.GetTestKey(rand.Int()))
			if err != nil {
				b.Errorf("Delete() error = %v", err)
			}
		}
	})

}
