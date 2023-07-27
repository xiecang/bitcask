package data

import (
	"bitcask-go/fio"
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

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
			f, err := OpenFile(tt.fields.dirPath, tt.fields.id, fio.FIOStandar)
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
		dirPath    string
		id         uint32
		preRecords []*LogRecord
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
		{
			name: "test read log record",
			fields: fields{
				id:      1,
				dirPath: os.TempDir(),
			},
			args: args{
				offset: 0,
			},
			want: &LogRecord{
				Key:   []byte("key"),
				Value: []byte("value"),
			},
			want1:   15,
			wantErr: false,
		},
		{
			name: "record key empty",
			fields: fields{
				id:      1,
				dirPath: os.TempDir(),
			},
			args: args{
				offset: 0,
			},
			want: &LogRecord{
				Key:   []byte(""),
				Value: []byte("value"),
			},
			want1:   12,
			wantErr: false,
		},
		{
			name: "read with offset",
			fields: fields{
				id:      1,
				dirPath: os.TempDir(),
				preRecords: []*LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
				},
			},
			args: args{
				offset: 15,
			},
			want: &LogRecord{
				Key:   []byte("key"),
				Value: []byte("value"),
			},
			want1:   15,
			wantErr: false,
		},
		{
			name: "被删除的数据在数据文件的末尾",
			fields: fields{
				id:      1,
				dirPath: os.TempDir(),
			},
			args: args{
				offset: 0,
			},
			want: &LogRecord{
				Key:   []byte("key"),
				Value: []byte(""),
				Type:  LogRecordTypeDelete,
			},
			want1:   10,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				err := CleanDBFile(tt.fields.dirPath)
				if err != nil {
					t.Errorf("CleanDBFile() error = %v", err)
				}
			}()
			f, err := OpenFile(tt.fields.dirPath, tt.fields.id, fio.FIOStandar)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// write pre-records
			for _, record := range tt.fields.preRecords {
				recordBytes, _ := EncodeLogRecord(record)
				err = f.Write(recordBytes)
				if (err != nil) != tt.wantErr {
					t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}

			recordBytes, length := EncodeLogRecord(tt.want)
			err = f.Write(recordBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			err = f.Sync()
			if (err != nil) != tt.wantErr {
				t.Errorf("Sync() error = %v, wantErr %v", err, tt.wantErr)
				return
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
			if got1 != length {
				t.Errorf("ReadLogRecord() got1 = %v, length %v", got1, length)
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
			f, err := OpenFile(tt.fields.dirPath, tt.fields.id, fio.FIOStandar)
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
			f, err := OpenFile(tt.fields.dirPath, tt.fields.id, fio.FIOStandar)
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
			_ = CleanDBFile(tt.fields.dirPath)
			f, err := OpenFile(tt.fields.dirPath, tt.fields.id, fio.FIOStandar)
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
	ioManager1, _ := fio.NewFileManager(GetFilePath(tmpDir, 1))
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
			got, err := OpenFile(tt.args.dirPath, tt.args.fileId, fio.FIOStandar)
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

func Test_filePath(t *testing.T) {
	type args struct {
		dirPath string
		fileId  uint32
	}
	var dir = os.TempDir()
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test file path",
			args: args{
				dirPath: dir,
				fileId:  1,
			},
			want: filepath.Join(dir, "0000000001.data"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFilePath(tt.args.dirPath, tt.args.fileId); got != tt.want {
				t.Errorf("GetFilePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_logRecordHeader_empty(t *testing.T) {
	type fields struct {
		crc        uint32
		recordType LogRecordType
		keySize    uint32
		valueSize  uint32
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "test empty",
			fields: fields{
				crc:        0,
				recordType: LogRecordTypeNormal,
				keySize:    0,
			},
			want: true,
		},
		{
			name: "test not empty",
			fields: fields{
				crc:        1,
				recordType: LogRecordTypeDelete,
				keySize:    1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &logRecordHeader{
				crc:        tt.fields.crc,
				recordType: tt.fields.recordType,
				keySize:    tt.fields.keySize,
				valueSize:  tt.fields.valueSize,
			}
			if got := l.empty(); got != tt.want {
				t.Errorf("empty() = %v, want %v", got, tt.want)
			}
		})
	}
}
