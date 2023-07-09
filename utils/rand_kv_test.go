package utils

import (
	"reflect"
	"testing"
)

func TestGetTestKey(t *testing.T) {
	type args struct {
		i int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "test get test key",
			args: args{
				i: 1,
			},
			want: []byte("bitcask-go-key-0000000001"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTestKey(tt.args.i); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetTestKey() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func TestRandomValue(t *testing.T) {
	type args struct {
		length int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test random value",
			args: args{
				length: 10,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RandomValue(tt.args.length)
			t.Logf("RandomValue() = %v", got)
			if len(got) != tt.args.length {
				t.Errorf("RandomValue() = %v, want %v", len(got), tt.args.length)
			}
		})
	}
}
