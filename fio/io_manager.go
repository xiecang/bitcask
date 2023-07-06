package fio

const DataFilePerm = 0644

// IOManager 抽象文件 IO 接口， 可以接入不同的文件 IO 类型，目前支持标准文件 IO
type IOManager interface {
	// Read 从文件的给定位置读取数据
	Read([]byte, int64) (int, error)
	// Write 写入字节数组到文件中
	Write([]byte) (int, error)
	// Sync 持久化数据
	Sync() error
	// Close 关闭文件
	Close() error

	// Size 返回文件大小
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager, 当前仅支持标准文件 IO
func NewIOManager(filePath string) (IOManager, error) {
	return NewFileManager(filePath)
}
