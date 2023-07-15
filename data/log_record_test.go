package data

import (
	"reflect"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	type args struct {
		record *LogRecord
	}
	tests := []struct {
		name  string
		args  args
		want  []byte
		want1 int64
	}{
		{
			name: "normal",
			args: args{
				record: &LogRecord{
					Key:   []byte("key"),
					Value: []byte("val"),
				},
			},
			want:  []byte{214, 163, 254, 14, 0, 6, 6, 107, 101, 121, 118, 97, 108},
			want1: 13,
		},
		{
			name: "value empty",
			args: args{
				record: &LogRecord{
					Key:   []byte("key"),
					Value: []byte(""),
				},
			},
			want:  []byte{184, 38, 83, 75, 0, 6, 0, 107, 101, 121},
			want1: 10,
		},
		{
			name: "key empty",
			args: args{
				record: &LogRecord{
					Key:   []byte(""),
					Value: []byte("val"),
				},
			},
			want:  []byte{8, 157, 55, 252, 0, 0, 6, 118, 97, 108},
			want1: 10,
		},
		{
			name: "type delete",
			args: args{
				record: &LogRecord{
					Key:   []byte("key"),
					Value: []byte("val"),
					Type:  LogRecordTypeDelete,
				},
			},
			want:  []byte{149, 183, 133, 25, 1, 6, 6, 107, 101, 121, 118, 97, 108},
			want1: 13,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := EncodeLogRecord(tt.args.record)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeLogRecord() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("EncodeLogRecord() got1 = %v, want %v", got1, tt.want1)
			}
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
		{
			name: "normal",
			args: args{
				buf: []byte{214, 163, 254, 14, 0, 6, 6, 107, 101, 121, 118, 97, 108},
			},
			want: &logRecordHeader{
				crc:        251569110,
				recordType: LogRecordTypeNormal,
				keySize:    3,
				valueSize:  3,
			},
			want1: 7,
		},
		{
			name: "value empty",
			args: args{
				buf: []byte{184, 38, 83, 75, 0, 6, 0, 107, 101, 121},
			},
			want: &logRecordHeader{
				crc:        1263740600,
				recordType: LogRecordTypeNormal,
				keySize:    3,
				valueSize:  0,
			},
			want1: 7,
		},
		{
			name: "key empty",
			args: args{
				buf: []byte{8, 157, 55, 252, 0, 0, 6, 118, 97, 108},
			},
			want: &logRecordHeader{
				crc:        4231503112,
				recordType: LogRecordTypeNormal,
				keySize:    0,
				valueSize:  3,
			},
			want1: 7,
		},
		{
			name: "type delete",
			args: args{
				buf: []byte{149, 183, 133, 25, 1, 6, 6, 107, 101, 121, 118, 97, 108},
			},
			want: &logRecordHeader{
				crc:        428193685,
				recordType: LogRecordTypeDelete,
				keySize:    3,
				valueSize:  3,
			},
			want1: 7,
		},
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
		{
			name: "normal",
			args: args{
				record: &LogRecord{
					Key:   []byte("key"),
					Value: []byte("val"),
				},
				header: []byte{0, 6, 6},
			},
			want: 251569110,
		},
		{
			name: "value empty",
			args: args{
				record: &LogRecord{
					Key:   []byte("key"),
					Value: []byte(""),
				},
				header: []byte{0, 6, 0},
			},
			want: 1263740600,
		},
		{
			name: "key empty",
			args: args{
				record: &LogRecord{
					Key:   []byte(""),
					Value: []byte("val"),
				},
				header: []byte{0, 0, 6},
			},
			want: 4231503112,
		},
		{
			name: "type delete",
			args: args{
				record: &LogRecord{
					Key:   []byte("key"),
					Value: []byte("val"),
					Type:  LogRecordTypeDelete,
				},
				header: []byte{1, 6, 6},
			},
			want: 428193685,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getLogRecordCRC(tt.args.record, tt.args.header); got != tt.want {
				t.Errorf("getLogRecordCRC() = %v, want %v", got, tt.want)
			}
		})
	}
}
