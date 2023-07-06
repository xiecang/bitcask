package index

import (
	"bitcask-go/data"
	"github.com/google/btree"
	"reflect"
	"sync"
	"testing"
)

func TestBTree_Delete(t *testing.T) {
	type fields struct {
		tree *btree.BTree
		lock *sync.RWMutex
	}
	type args struct {
		key []byte
	}
	type pre struct {
		key []byte
		pos *data.LogRecordPos
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		pre    []pre
		want   bool
	}{
		{
			name: "test key nil",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			args: args{
				key: nil,
			},
			pre: []pre{
				{
					key: []byte("test"),
					pos: &data.LogRecordPos{
						Fid:    1,
						Offset: 1,
					},
				},
			},
			want: false,
		},
		{
			name: "test delete one",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			args: args{
				key: []byte("test"),
			},
			pre: []pre{
				{
					key: []byte("test"),
					pos: &data.LogRecordPos{
						Fid:    1,
						Offset: 1,
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := &BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			for _, p := range tt.pre {
				bt.Put(p.key, p.pos)
			}
			if got := bt.Delete(tt.args.key); got != tt.want {
				t.Errorf("Delete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBTree_Get(t *testing.T) {
	type fields struct {
		tree *btree.BTree
		lock *sync.RWMutex
	}
	type args struct {
		Key []byte
	}
	type pre struct {
		Key []byte
		Pos *data.LogRecordPos
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		pre    []pre
		want   *data.LogRecordPos
	}{
		{
			name: "test key nil",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			args: args{
				Key: nil,
			},
			pre: []pre{
				{
					Key: []byte("test"),
					Pos: &data.LogRecordPos{
						Fid:    1,
						Offset: 1,
					},
				},
			},
			want: nil,
		}, {
			name: "test get one",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			args: args{
				Key: []byte("test"),
			},
			pre: []pre{
				{
					Key: []byte("test"),
					Pos: &data.LogRecordPos{
						Fid:    1,
						Offset: 1,
					},
				},
			},
			want: &data.LogRecordPos{
				Fid:    1,
				Offset: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := &BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			for _, d := range tt.pre {
				bt.Put(d.Key, d.Pos)
			}
			if got := bt.Get(tt.args.Key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBTree_Put(t *testing.T) {
	type fields struct {
		tree *btree.BTree
		lock *sync.RWMutex
	}
	type args struct {
		Key []byte
		pos *data.LogRecordPos
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "test key nil",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			args: args{
				Key: nil,
				pos: nil,
			},
			want: true,
		},
		{
			name: "test put nil",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			args: args{
				Key: []byte("test"),
				pos: nil,
			},
			want: true,
		},
		{
			name: "test put1",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			args: args{
				Key: []byte("test"),
				pos: &data.LogRecordPos{
					Fid:    1,
					Offset: 1,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := &BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			if got := bt.Put(tt.args.Key, tt.args.pos); got != tt.want {
				t.Errorf("Put() = %v, want %v", got, tt.want)
			}
		})
	}
}
