package data

import (
	"bitcask-go/fio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func cleanTmpDataFile(path string) error {
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 判断是否是 .a 后缀的文件
		if !info.IsDir() && filepath.Ext(path) == FileNameSuffix {
			if err = os.Remove(path); err != nil {
				return err
			}
			fmt.Printf("Deleted file: %s\n", path)
		}

		return nil
	})
	return err
}
func TestFile_Close(t *testing.T) {
	type fields struct {
		dirPath string
		id      uint32
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "test close",
			fields: fields{
				id:      1,
				dirPath: os.TempDir(),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := OpenFile(tt.fields.dirPath, tt.fields.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err = f.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFile_ReadLogRecord(t *testing.T) {
	type fields struct {
		Id          uint32
		WriteOffset int64
		IOManager   fio.IOManager
	}
	type args struct {
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *LogRecord
		want1   int64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &File{
				Id:          tt.fields.Id,
				WriteOffset: tt.fields.WriteOffset,
				IOManager:   tt.fields.IOManager,
			}
			got, got1, err := f.ReadLogRecord(tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadLogRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadLogRecord() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("ReadLogRecord() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestFile_Sync(t *testing.T) {
	type fields struct {
		dirPath string
		id      uint32
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "test sync",
			fields: fields{
				id:      1,
				dirPath: os.TempDir(),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := OpenFile(tt.fields.dirPath, tt.fields.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err = f.Sync(); (err != nil) != tt.wantErr {
				t.Errorf("Sync() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFile_Write(t *testing.T) {
	type fields struct {
		dirPath string
		id      uint32
	}
	type args struct {
		buf []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "write file",
			fields: fields{
				id:      1,
				dirPath: os.TempDir(),
			},
			args: args{
				buf: []byte("hello world"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := OpenFile(tt.fields.dirPath, tt.fields.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err = f.Write(tt.args.buf); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			readBytes, err := f.readNBytes(int64(len(tt.args.buf)), 0)
			if (err != nil) != tt.wantErr {
				t.Errorf("readNBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if bytes.Compare(readBytes, tt.args.buf) != 0 {
				t.Errorf("readNBytes() got = %v, want %v", string(readBytes), string(tt.args.buf))
			}
		})
	}
}

func TestFile_readNBytes(t *testing.T) {
	type fields struct {
		dirPath    string
		id         uint32
		writeBytes []byte
	}
	type args struct {
		n      int64
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "test read n bytes",
			fields: fields{
				dirPath:    os.TempDir(),
				id:         1,
				writeBytes: []byte("hello bitcask-go, test by xc"),
			},
			args: args{
				n:      11,
				offset: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = cleanTmpDataFile(tt.fields.dirPath)
			f, err := OpenFile(tt.fields.dirPath, tt.fields.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			err = f.Write(tt.fields.writeBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, err := f.readNBytes(tt.args.n, tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("readNBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			want := tt.fields.writeBytes[tt.args.offset : tt.args.offset+tt.args.n]
			if !reflect.DeepEqual(got, want) {
				t.Errorf("readNBytes() got = %v, want %v, path: %v", string(got), string(want), tt.fields.dirPath)
			}
		})
	}
}

func TestOpenFile(t *testing.T) {
	type args struct {
		dirPath string
		fileId  uint32
	}

	tmpDir := os.TempDir()
	ioManager1, _ := fio.NewFileManager(filePath(tmpDir, 1))
	tests := []struct {
		name    string
		args    args
		want    *File
		wantErr bool
	}{
		{
			name: "test open file",
			args: args{
				dirPath: tmpDir,
				fileId:  1,
			},
			want: &File{
				Id:          1,
				WriteOffset: 0,
				IOManager:   ioManager1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OpenFile(tt.args.dirPath, tt.args.fileId)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			//if !reflect.DeepEqual(got, tt.want) {
			//	t.Errorf("OpenFile() got = %v, want %v", got, tt.want)
			//}
			t.Logf("got: %v", got)
		})
	}
}

func Test_decodeLogRecordHeader(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		name  string
		args  args
		want  *logRecordHeader
		want1 int64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := decodeLogRecordHeader(tt.args.buf)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeLogRecordHeader() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("decodeLogRecordHeader() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_getLogRecordCRC(t *testing.T) {
	type args struct {
		record *LogRecord
		header []byte
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getLogRecordCRC(tt.args.record, tt.args.header); got != tt.want {
				t.Errorf("getLogRecordCRC() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_logRecordHeader_empty(t *testing.T) {
	type fields struct {
		crc        uint32
		recordType LogRecordType
		keysSize   uint32
		valueSize  uint32
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &logRecordHeader{
				crc:        tt.fields.crc,
				recordType: tt.fields.recordType,
				keysSize:   tt.fields.keysSize,
				valueSize:  tt.fields.valueSize,
			}
			if got := l.empty(); got != tt.want {
				t.Errorf("empty() = %v, want %v", got, tt.want)
			}
		})
	}
}
