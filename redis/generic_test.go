package redis

import (
	bitcask "bitcask-go"
	"testing"
	"time"
)

func TestDataStructure_Del(t *testing.T) {
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
			wantErr: false,
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
			if err = d.Del(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("Del() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDataStructure_Type(t *testing.T) {
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
		want    DataType
		wantErr bool
	}{
		{
			name: "string",
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
			want:    String,
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
			got, err := d.Type(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Type() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Type() got = %v, want %v", got, tt.want)
			}
		})
	}
}
