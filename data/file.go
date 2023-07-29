package data

import (
	"bitcask-go/fio"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	FileNameSuffix        = ".data"
	FileNameHint          = "hint-index"
	FileNameMergeFinished = "merge-finished"
	FileNameSeqId         = "seq-id"
)

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
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

func (l *logRecordHeader) empty() bool {
	return l.crc == 0 && l.keySize == 0 && l.valueSize == 0
}

func GetFilePath(dirPath string, fileId uint32) string {
	name := fmt.Sprintf("%010d%s", fileId, FileNameSuffix)
	p := filepath.Join(dirPath, name)
	return p
}

func newFile(fileName string, fileId uint32, ioType fio.FileIOType) (*File, error) {
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &File{
		Id:          fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

// OpenFile 打开数据文件
func OpenFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*File, error) {
	p := GetFilePath(dirPath, fileId)
	return newFile(p, fileId, ioType)
}

func GetHintFileName(dirPath string) string {
	p := filepath.Join(dirPath, FileNameHint)
	return p
}

// OpenHintFile 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*File, error) {
	p := GetHintFileName(dirPath)
	return newFile(p, 0, fio.FIOStandar)
}

func MergeFinishedFileName(dirPath string) string {
	p := filepath.Join(dirPath, FileNameMergeFinished)
	return p
}

// OpenMergeFinishedFile 打开 Merge 完成标识文件
func OpenMergeFinishedFile(dirPath string) (*File, error) {
	p := MergeFinishedFileName(dirPath)
	return newFile(p, 0, fio.FIOStandar)
}

func seqIdFileName(dirPath string) string {
	p := filepath.Join(dirPath, FileNameSeqId)
	return p
}

func IsSeqIdFileNotExit(dirPath string) bool {
	name := seqIdFileName(dirPath)
	if _, err := os.Stat(name); os.IsNotExist(err) {
		return true
	}
	return false
}

// OpenSeqIdFile 打开存储事务序列号的文件
func OpenSeqIdFile(dirPath string) (*File, error) {
	p := seqIdFileName(dirPath)
	return newFile(p, 0, fio.FIOStandar)
}

// ReadLogRecord 根据 offset 读取 LogRecord
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
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
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

// WriteHintRecord 写入索引信息到 Hint 索引文件
func (f *File) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	var record = &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
		Type:  LogRecordTypeHint,
	}

	encodeRecord, _ := EncodeLogRecord(record)
	return f.Write(encodeRecord)
}

func (f *File) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := f.IOManager.Read(b, offset)
	return b, err
}

func (f *File) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := f.IOManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetFilePath(dirPath, f.Id), ioType)
	if err != nil {
		return err
	}
	f.IOManager = ioManager
	return nil
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
		if !file.IsDir() && filepath.Ext(file.Name()) == FileNameSuffix {
			p := filepath.Join(path, file.Name())
			if err = os.Remove(p); err != nil {
				return err
			}
			fmt.Printf("Deleted file: %s\n", p)
		}
	}
	return err
}
