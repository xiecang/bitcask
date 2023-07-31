package redis

import (
	bitcask "bitcask-go"
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func destroyTestData(d *DataStructure, options bitcask.Options) {
	_ = d.db.Close()
	err := os.RemoveAll(options.DirPath)
	if err != nil {
		panic(err)
	}
	fmt.Printf("destroy test data: %s\n", options.DirPath)
}

func TestDataStructure_Get(t *testing.T) {
	type kv struct {
		key   []byte
		value []byte
		ttl   time.Duration
	}
	type fields struct {
		option bitcask.Options
		values []kv
		sleep  time.Duration
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						value: []byte("value"),
					},
				},
			},
			args: args{
				key: []byte("key"),
			},
			want:    []byte("value"),
			wantErr: false,
		},
		{
			name: "key not exists",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						value: []byte("value"),
					},
				},
			},
			args: args{
				key: []byte("key2"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "key expired",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						value: []byte("value"),
						ttl:   time.Second,
					},
				},
				sleep: 2 * time.Second,
			},
			args: args{
				key: []byte("key"),
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := NewDataStructure(tt.fields.option)
			if err != nil {
				t.Errorf("NewDataStructure() error = %v", err)
				return
			}
			defer destroyTestData(d, tt.fields.option)
			for _, value := range tt.fields.values {
				err = d.Set(value.key, value.ttl, value.value)
				if err != nil {
					t.Errorf("Set() value==%+v, error=%v", value, err)
					return
				}
			}
			if tt.fields.sleep > 0 {
				time.Sleep(tt.fields.sleep)
			}
			got, err := d.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataStructure_Set(t *testing.T) {
	type fields struct {
		option bitcask.Options
		sleep  time.Duration
	}
	type args struct {
		key   []byte
		ttl   time.Duration
		value []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "ttl = 0",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:   []byte("key"),
				ttl:   0,
				value: []byte("value"),
			},
		},
		{
			name: "expire",
			fields: fields{
				option: bitcask.DefaultOptions,
				sleep:  3 * time.Second,
			},
			args: args{
				key:   []byte("key"),
				ttl:   1 * time.Second,
				value: []byte("value"),
			},
		},
		{
			name: "ttl > 0",
			fields: fields{
				option: bitcask.DefaultOptions,
				sleep:  1 * time.Second,
			},
			args: args{
				key:   []byte("key"),
				ttl:   5 * time.Second,
				value: []byte("value"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := NewDataStructure(tt.fields.option)
			if err != nil {
				t.Errorf("NewDataStructure() error = %v", err)
				return
			}
			defer destroyTestData(d, tt.fields.option)
			if err = d.Set(tt.args.key, tt.args.ttl, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.fields.sleep > 0 {
				time.Sleep(tt.fields.sleep)
			}
			got, err := d.Get(tt.args.key)
			if err != nil {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.args.ttl > 0 && tt.args.ttl < tt.fields.sleep {
				// expired
				if got != nil {
					t.Errorf("Set() got = %v, want nil", got)
				}
			} else {
				if bytes.Compare(got, tt.args.value) != 0 {
					t.Errorf("Set() got = %v, want %v", got, tt.args.value)
				}
			}
		})
	}
}
