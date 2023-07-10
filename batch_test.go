package bitcask_go

import (
	"reflect"
	"testing"
)

func Test_parsedLogRecordKey(t *testing.T) {
	type args struct {
		key []byte
	}
	tests := []struct {
		name  string
		args  args
		want  []byte
		want1 uint64
	}{
		{
			name: "number key",
			args: args{
				key: []byte("\u0001123"),
			},
			want:  []byte("123"),
			want1: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := parsedLogRecordKey(tt.args.key)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parsedLogRecordKey() got = %v, want %v", string(got), string(tt.want))
			}
			if got1 != tt.want1 {
				t.Errorf("parsedLogRecordKey() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_logRecordKeyWithSeq(t *testing.T) {
	type args struct {
		key   []byte
		seqId uint64
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "number key",
			args: args{
				key:   []byte("123"),
				seqId: 1,
			},
			want: []byte("\u0001123"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := logRecordKeyWithSeq(tt.args.key, tt.args.seqId); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("logRecordKeyWithSeq() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}
