package data

import (
	"bitcask-go/fio"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const FileNameSuffix = ".data"

var (
	ErrInvalidCRC = fmt.Errorf("invalid crc value, log record maybe corrupted")
)

// File 数据文件
type File struct {
	// Id 文件 id
	Id uint32
	// WriteOffset 文件写入的偏移量
	WriteOffset int64
	// IOManager IO 读写管理器
	IOManager fio.IOManager
}

// logRecordHeader LogRecord 的 Header 信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keysSize   uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

func (l *logRecordHeader) empty() bool {
	return l.crc == 0 && l.keysSize == 0 && l.valueSize == 0
}

func filePath(dirPath string, fileId uint32) string {
	name := fmt.Sprintf("%010d%s", fileId, FileNameSuffix)
	p := filepath.Join(dirPath, name)
	return p
}

// OpenFile 打开数据文件
func OpenFile(dirPath string, fileId uint32) (*File, error) {
	p := filePath(dirPath, fileId)
	ioManager, err := fio.NewIOManager(p)
	if err != nil {
		return nil, err
	}
	return &File{
		Id:          fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

func (f *File) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := f.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var readHeaderSize int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		// 如果剩余的文件大小不足以读取一个 LogRecord 的 Header 信息，则直接读取剩余的文件大小
		readHeaderSize = fileSize - offset
	}
	// 读取 Header 信息
	headerBytes, err := f.readNBytes(readHeaderSize, offset)
	if err != nil {
		return nil, 0, err
	}

	// 下面两个条件表示已经读取到了文件末尾，直接返回 EOF 错误
	header, headerSize := decodeLogRecordHeader(headerBytes)
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.empty() {
		return nil, 0, io.EOF
	}

	// 读取 LogRecord 的 key 和 value 的长度
	keySize, valueSize := int64(header.keysSize), int64(header.valueSize)
	var totalSize = headerSize + keySize + valueSize

	var logRecord = &LogRecord{
		Type: header.recordType,
	}
	// 读取 LogRecord 的 key 和 value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := f.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		// 解析 LogRecord 的 key 和 value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 计算 crc 校验值
	crc := getLogRecordCRC(logRecord, headerBytes[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, totalSize, nil
}

func (f *File) Write(buf []byte) error {
	n, err := f.IOManager.Write(buf)
	if err != nil {
		return err
	}
	f.WriteOffset += int64(n)
	return nil
}

func (f *File) Sync() error {
	return f.IOManager.Sync()
}

func (f *File) Close() error {
	return f.IOManager.Close()
}

func (f *File) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := f.IOManager.Read(b, offset)
	return b, err
}

// 对字节数组中的 Header 信息进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	panic("implement me")
}

func getLogRecordCRC(record *LogRecord, header []byte) uint32 {
	panic("implement me")
}
