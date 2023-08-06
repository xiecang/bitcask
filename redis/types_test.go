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

func TestDataStructure_HDel(t *testing.T) {
	type kv struct {
		key   []byte
		field []byte
		value []byte
	}
	type fields struct {
		option bitcask.Options
		values []kv
	}
	type args struct {
		key   []byte
		field []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte("field"),
						value: []byte("value"),
					},
				},
			},
			args: args{
				key:   []byte("key"),
				field: []byte("field"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "empty key",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte("field"),
						value: []byte("value"),
					},
				},
			},
			args: args{
				key:   []byte(""),
				field: []byte("field"),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "empty field",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte(""),
						value: []byte("value"),
					},
				},
			},
			args: args{
				key:   []byte("key"),
				field: []byte(""),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "empty value",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte("field"),
						value: []byte(""),
					},
				},
			},
			args: args{
				key:   []byte("key"),
				field: []byte("field"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "not exist key",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte("field"),
						value: []byte(""),
					},
				},
			},
			args: args{
				key:   []byte("key2"),
				field: []byte("field"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "not exist value",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte("field"),
						value: []byte(""),
					},
				},
			},
			args: args{
				key:   []byte("key"),
				field: []byte("field2"),
			},
			want:    false,
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
			for _, v := range tt.fields.values {
				if _, err = d.HSet(v.key, v.field, v.value); err != nil {
					t.Errorf("HSet() error = %v", err)
					return
				}
			}
			got, err := d.HDel(tt.args.key, tt.args.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("HDel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("HDel() got = %v, want %v", got, tt.want)
			}

			// 重新 get 一次，确认是否删除成功
			gotValue, err := d.HGet(tt.args.key, tt.args.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("HDel() got = %v, want nil", err)
			}
			if err == nil && gotValue != nil {
				t.Errorf("HDel() got = %v, want nil", gotValue)
			}
		})
	}
}

func TestDataStructure_HGet(t *testing.T) {
	type kv struct {
		key   []byte
		field []byte
		value []byte
	}
	type fields struct {
		option bitcask.Options
		values []kv
	}
	type args struct {
		key   []byte
		field []byte
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
						field: []byte("field"),
						value: []byte("value"),
					},
				},
			},
			args: args{
				key:   []byte("key"),
				field: []byte("field"),
			},
			want:    []byte("value"),
			wantErr: false,
		},
		{
			name: "empty key",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte("field"),
						value: []byte("value"),
					},
				},
			},
			args: args{
				key:   []byte(""),
				field: []byte("field"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "empty field",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte(""),
						value: []byte("value"),
					},
				},
			},
			args: args{
				key:   []byte("key"),
				field: []byte(""),
			},
			want:    []byte("value"),
			wantErr: false,
		},
		{
			name: "empty value",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte("field"),
						value: []byte(""),
					},
				},
			},
			args: args{
				key:   []byte("key"),
				field: []byte("field"),
			},
			want:    []byte(""),
			wantErr: false,
		},
		{
			name: "not exist key",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte("field"),
						value: []byte(""),
					},
				},
			},
			args: args{
				key:   []byte("key2"),
				field: []byte("field"),
			},
			want:    []byte(""),
			wantErr: false,
		},
		{
			name: "not exist value",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte("field"),
						value: []byte(""),
					},
				},
			},
			args: args{
				key:   []byte("key"),
				field: []byte("field2"),
			},
			want:    []byte(""),
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
			for _, v := range tt.fields.values {
				if _, err = d.HSet(v.key, v.field, v.value); err != nil {
					t.Errorf("HSet() error = %v", err)
					return
				}
			}
			got, err := d.HGet(tt.args.key, tt.args.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("HGet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !bytes.Equal(got, tt.want) {
				t.Errorf("HGet() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataStructure_HSet(t *testing.T) {
	type kv struct {
		key   []byte
		field []byte
		value []byte
	}
	type fields struct {
		option bitcask.Options
		values []kv
	}
	type args struct {
		key   []byte
		field []byte
		value []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:   []byte("key"),
				field: []byte("field"),
				value: []byte("value"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "same field",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:   []byte("key"),
						field: []byte("field"),
						value: []byte("value"),
					},
				},
			},
			args: args{
				key:   []byte("key"),
				field: []byte("field"),
				value: []byte("value2"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "empty key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:   []byte(""),
				field: []byte("field"),
				value: []byte("value"),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "empty field",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:   []byte("key"),
				field: []byte(""),
				value: []byte("value"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "empty value",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:   []byte("key"),
				field: []byte("field"),
				value: []byte(""),
			},
			want:    true,
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
			for _, v := range tt.fields.values {
				if _, err = d.HSet(v.key, v.field, v.value); err != nil {
					t.Errorf("HSet() error = %v", err)
					return
				}
			}
			got, err := d.HSet(tt.args.key, tt.args.field, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("HSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("HSet() got = %v, want %v", got, tt.want)
			}
			gotValue, err := d.HGet(tt.args.key, tt.args.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("HGet() error = %v", err)
				return
			}
			if err == nil {
				if bytes.Compare(gotValue, tt.args.value) != 0 {
					t.Errorf("HGet() got = %v, want %v", gotValue, tt.args.value)
				}
			}
		})
	}
}

func TestDataStructure_SAdd(t *testing.T) {
	type kv struct {
		key     []byte
		members [][]byte
	}
	type fields struct {
		option bitcask.Options
		values []kv
	}
	type args struct {
		key     []byte
		members [][]byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:     []byte("key"),
				members: [][]byte{[]byte("member1"), []byte("member2")},
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "same member",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:     []byte("key"),
						members: [][]byte{[]byte("member1"), []byte("member2")},
					},
				},
			},
			args: args{
				key:     []byte("key"),
				members: [][]byte{[]byte("member1"), []byte("member2")},
			},
			want:    0,
			wantErr: false,
		},
		{
			name: "empty key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:     []byte(""),
				members: [][]byte{[]byte("member1"), []byte("member2")},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "empty member",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:     []byte("key"),
				members: [][]byte{[]byte(""), []byte("member2")},
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "duplicate member",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:     []byte("key"),
				members: [][]byte{[]byte("member1"), []byte("member1")},
			},
			want:    1,
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
				_, err = d.SAdd(value.key, value.members...)
				if err != nil {
					t.Errorf("SAdd() error = %v", err)
					return
				}
			}
			got, err := d.SAdd(tt.args.key, tt.args.members...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SAdd() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SAdd() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataStructure_SIsMember(t *testing.T) {
	type kv struct {
		key     []byte
		members [][]byte
	}
	type fields struct {
		option bitcask.Options
		values []kv
	}
	type args struct {
		key    []byte
		member []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:     []byte("key"),
						members: [][]byte{[]byte("member1"), []byte("member2")},
					},
				},
			},
			args: args{
				key:    []byte("key"),
				member: []byte("member1"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "not exist member",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:     []byte("key"),
						members: [][]byte{[]byte("member1"), []byte("member2")},
					},
				},
			},
			args: args{
				key:    []byte("key"),
				member: []byte("member3"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "empty key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:    []byte(""),
				member: []byte("member1"),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "empty member",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:    []byte("key"),
				member: []byte(""),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "not exist key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:    []byte("key"),
				member: []byte("member1"),
			},
			want:    false,
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
				_, err = d.SAdd(value.key, value.members...)
				if err != nil {
					t.Errorf("SAdd() error = %v", err)
					return
				}
			}
			got, err := d.SIsMember(tt.args.key, tt.args.member)
			if (err != nil) != tt.wantErr {
				t.Errorf("SIsMember() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SIsMember() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataStructure_SRem(t *testing.T) {
	type kv struct {
		key     []byte
		members [][]byte
	}
	type fields struct {
		option bitcask.Options
		values []kv
	}
	type args struct {
		key    []byte
		member []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:     []byte("key"),
						members: [][]byte{[]byte("member1"), []byte("member2")},
					},
				},
			},
			args: args{
				key:    []byte("key"),
				member: []byte("member1"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "not exist member",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:     []byte("key"),
						members: [][]byte{[]byte("member1"), []byte("member2")},
					},
				},
			},
			args: args{
				key:    []byte("key"),
				member: []byte("member3"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "empty key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:    []byte(""),
				member: []byte("member1"),
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "empty member",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:     []byte("key"),
						members: [][]byte{[]byte("member1"), []byte("member2")},
					},
				},
			},
			args: args{
				key:    []byte("key"),
				member: []byte(""),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "empty member delete success",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:     []byte("key"),
						members: [][]byte{[]byte("member1"), []byte("")},
					},
				},
			},
			args: args{
				key:    []byte("key"),
				member: []byte(""),
			},
			want:    true,
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
				_, err = d.SAdd(value.key, value.members...)
				if err != nil {
					t.Errorf("SAdd() error = %v", err)
					return
				}
			}
			got, err := d.SRem(tt.args.key, tt.args.member)
			if (err != nil) != tt.wantErr {
				t.Errorf("SRem() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SRem() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataStructure_LPop(t *testing.T) {
	type kv struct {
		key    []byte
		values [][]byte
	}
	type fields struct {
		option bitcask.Options
		values []kv
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
						key:    []byte("key"),
						values: [][]byte{[]byte("value1"), []byte("value2")},
					},
				},
			},
			args: args{
				key: []byte("key"),
			},
			want:    []byte("value2"),
			wantErr: false,
		},
		{
			name: "empty key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key: []byte(""),
			},
			wantErr: true,
		},
		{
			name: "not exist key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key: []byte("key"),
			},
			wantErr: false,
		},
		{
			name: "empty list",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:    []byte("key"),
						values: [][]byte{},
					},
				},
			},
			args: args{
				key: []byte("key"),
			},
			wantErr: false,
		},
		{
			name: "empty list delete success",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:    []byte("key"),
						values: [][]byte{[]byte("")},
					},
				},
			},
			args: args{
				key: []byte("key"),
			},
			wantErr: false,
			want:    []byte(""),
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
				_, err = d.LPush(value.key, value.values...)
				if err != nil {
					t.Errorf("LPush() error = %v", err)
					return
				}
			}
			got, err := d.LPop(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("LPop() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LPop() got = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func TestDataStructure_LPush(t *testing.T) {
	type kv struct {
		key    []byte
		values [][]byte
	}
	type fields struct {
		option bitcask.Options
		values []kv
	}
	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:    []byte("key"),
						values: [][]byte{[]byte("value1"), []byte("value2")},
					},
				},
			},
			args: args{
				key: []byte("key"),
				values: [][]byte{
					[]byte("value3"),
					[]byte("value4"),
				},
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "empty key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key: []byte(""),
				values: [][]byte{
					[]byte("value3"),
					[]byte("value4"),
				},
			},
			wantErr: true,
		},
		{
			name: "empty value",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:    []byte("key"),
				values: [][]byte{},
			},
			want:    0,
			wantErr: false,
		},
		{
			name: "not exist key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key: []byte("key"),
			},
			want: 0,
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
				_, err = d.LPush(value.key, value.values...)
				if err != nil {
					t.Errorf("LPush() error = %v", err)
					return
				}
			}
			got, err := d.LPush(tt.args.key, tt.args.values...)
			if (err != nil) != tt.wantErr {
				t.Errorf("LPush() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LPush() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataStructure_RPop(t *testing.T) {
	type kv struct {
		key    []byte
		values [][]byte
	}
	type fields struct {
		option bitcask.Options
		values []kv
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
						key:    []byte("key"),
						values: [][]byte{[]byte("value1"), []byte("value2")},
					},
				},
			},
			args: args{
				key: []byte("key"),
			},
			want:    []byte("value1"),
			wantErr: false,
		},
		{
			name: "empty key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key: []byte(""),
			},
			wantErr: true,
		},
		{
			name: "not exist key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key: []byte("key"),
			},
			wantErr: false,
		},
		{
			name: "empty list",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:    []byte("key"),
						values: [][]byte{},
					},
				},
			},
			args: args{
				key: []byte("key"),
			},
			wantErr: false,
		},
		{
			name: "empty list delete success",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:    []byte("key"),
						values: [][]byte{[]byte("")},
					},
				},
			},
			args: args{
				key: []byte("key"),
			},
			wantErr: false,
			want:    []byte(""),
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
				_, err = d.LPush(value.key, value.values...)
				if err != nil {
					t.Errorf("LPush() error = %v", err)
					return
				}
			}
			got, err := d.RPop(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("RPop() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RPop() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataStructure_RPush(t *testing.T) {
	type kv struct {
		key    []byte
		values [][]byte
	}
	type fields struct {
		option bitcask.Options
		values []kv
	}
	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				option: bitcask.DefaultOptions,
				values: []kv{
					{
						key:    []byte("key"),
						values: [][]byte{[]byte("value1"), []byte("value2")},
					},
				},
			},
			args: args{
				key: []byte("key"),
				values: [][]byte{
					[]byte("value3"),
					[]byte("value4"),
				},
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "empty key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key: []byte(""),
				values: [][]byte{
					[]byte("value3"),
					[]byte("value4"),
				},
			},
			wantErr: true,
		},
		{
			name: "empty value",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key:    []byte("key"),
				values: [][]byte{},
			},
			want:    0,
			wantErr: false,
		},
		{
			name: "not exist key",
			fields: fields{
				option: bitcask.DefaultOptions,
			},
			args: args{
				key: []byte("key"),
			},
			want: 0,
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
				_, err = d.LPush(value.key, value.values...)
				if err != nil {
					t.Errorf("LPush() error = %v", err)
					return
				}
			}
			got, err := d.RPush(tt.args.key, tt.args.values...)
			if (err != nil) != tt.wantErr {
				t.Errorf("RPush() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("RPush() got = %v, want %v", got, tt.want)
			}
		})
	}
}
