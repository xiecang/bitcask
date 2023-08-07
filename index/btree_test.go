package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/xiecang/bitcask/data"
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
		name     string
		fields   fields
		args     args
		pre      []pre
		want     bool
		wantData *data.LogRecordPos
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
			wantData: &data.LogRecordPos{
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
			for _, p := range tt.pre {
				bt.Put(p.key, p.pos)
			}
			if wantData, got := bt.Delete(tt.args.key); got != tt.want {
				t.Errorf("Delete() = %v, want %v", got, tt.want)
			} else if !reflect.DeepEqual(wantData, tt.wantData) {
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
		want   *data.LogRecordPos
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
			want: nil,
		}, {
			name: "test get one",
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
				bt.Put(d.key, d.pos)
			}
			if got := bt.Get(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBTree_Put(t *testing.T) {
	type pre struct {
		key []byte
		pos *data.LogRecordPos
	}
	type fields struct {
		tree *btree.BTree
		lock *sync.RWMutex

		pre []pre
	}
	type args struct {
		key []byte
		pos *data.LogRecordPos
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *data.LogRecordPos
	}{
		{
			name: "test key nil",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			args: args{
				key: nil,
				pos: nil,
			},
			want: nil,
		},
		{
			name: "test put nil",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			args: args{
				key: []byte("test"),
				pos: nil,
			},
			want: nil,
		},
		{
			name: "test put1",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
				pre: []pre{
					{
						key: []byte("test"),
						pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
				},
			},
			args: args{
				key: []byte("test"),
				pos: &data.LogRecordPos{
					Fid:    2,
					Offset: 2,
				},
			},
			want: &data.LogRecordPos{
				Fid:    1,
				Offset: 1,
			},
		},
		{
			name: "test put exist",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			args: args{
				key: []byte("test"),
				pos: &data.LogRecordPos{
					Fid:    1,
					Offset: 1,
				},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := &BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			for _, d := range tt.fields.pre {
				bt.Put(d.key, d.pos)
			}
			if got := bt.Put(tt.args.key, tt.args.pos); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Put() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBTree_Size(t *testing.T) {
	type fields struct {
		tree *btree.BTree
		lock *sync.RWMutex
	}
	type preValues struct {
		key []byte
		pos *data.LogRecordPos
	}
	tests := []struct {
		name   string
		fields fields
		pre    []preValues
		want   int
	}{
		{
			name: "size",
			fields: fields{
				tree: btree.New(32),
				lock: &sync.RWMutex{},
			},
			pre: []preValues{
				{
					key: []byte("test"),
					pos: &data.LogRecordPos{
						Fid:    1,
						Offset: 1,
					},
				},
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := &BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			for _, d := range tt.pre {
				bt.Put(d.key, d.pos)
			}
			if got := bt.Size(); got != tt.want {
				t.Errorf("Size() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestBTree_Iterator(t *testing.T) {
	type fields struct {
		items []*Item
	}
	type args struct {
		reverse bool
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      Iterator
		wantValid bool
	}{
		{
			name: "empty iterator",
			args: args{
				reverse: false,
			},
			want: &bTreeIterator{
				currIndex: 0,
				reverse:   false,
				values:    []*Item{},
			},
			wantValid: false,
		},
		{
			name: "iterator",
			fields: fields{
				items: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 5,
						},
					},
				},
			},
			args: args{
				reverse: false,
			},
			want: &bTreeIterator{
				currIndex: 0,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 5,
						},
					},
				},
			},
			wantValid: true,
		},
		{
			name: "reverse",
			fields: fields{
				items: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 5,
						},
					},
				},
			},
			args: args{
				reverse: true,
			},
			want: &bTreeIterator{
				currIndex: 0,
				reverse:   true,
				values: []*Item{
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 5,
						},
					},
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
				},
			},
			wantValid: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := NewBTree()
			for _, item := range tt.fields.items {
				bt.Put(item.Key, item.Pos)
			}
			got := bt.Iterator(tt.args.reverse)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Iterator() = %v, want %v", got, tt.want)
			}
			if got.Valid() != tt.wantValid {
				t.Errorf("Valid() = %v, want %v", got.Valid(), tt.wantValid)
			}
		})
	}
}

func Test_bTreeIterator_Close(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "close",
			fields: fields{
				currIndex: 0,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := bTreeIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			b.Close()
			if b.values != nil {
				t.Errorf("values not nil, values: %+v", b.values)
			}
		})
	}
}

func Test_bTreeIterator_Key(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			name: "get key",
			fields: fields{
				currIndex: 0,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
				},
			},
			want: []byte("test"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := bTreeIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			if got := b.Key(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bTreeIterator_Next(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "next key",
			fields: fields{
				currIndex: 0,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
		},
		{
			name: "next key with index",
			fields: fields{
				currIndex: 1,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := bTreeIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			var index = b.currIndex
			var wantKey = b.values[index+1].Key
			b.Next()
			var currentKey = b.values[b.currIndex].Key
			if bytes.Compare(currentKey, wantKey) != 0 {
				t.Errorf("Next() = %v, want %v", currentKey, wantKey)
			}
		})
	}
}

func Test_bTreeIterator_Rewind(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "rewind",
			fields: fields{
				currIndex: 0,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
		},
		{
			name: "rewind with index",
			fields: fields{
				currIndex: 2,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := bTreeIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			b.Rewind()
			if bytes.Compare(b.values[b.currIndex].Key, tt.fields.values[0].Key) != 0 {
				t.Errorf("Rewind() = %v, want %v", b.values[b.currIndex].Key, tt.fields.values[0].Key)
			}
		})
	}
}

func Test_bTreeIterator_Seek(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantValid bool
	}{
		{
			name: "seek",
			fields: fields{
				currIndex: 0,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
			args: args{
				key: []byte("test2"),
			},
			wantValid: true,
		},
		{
			name: "seek reverse",
			fields: fields{
				currIndex: 0,
				reverse:   true,
				values: []*Item{
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},

					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},

					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
				},
			},
			args: args{
				key: []byte("test2"),
			},
			wantValid: true,
		},
		{
			name: "seek to max",
			fields: fields{
				currIndex: 0,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
			args: args{
				key: []byte("test8"),
			},
			wantValid: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := bTreeIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			b.Seek(tt.args.key)

			if tt.wantValid != b.Valid() {
				t.Errorf("Seek() = %v, want %v", b.Valid(), tt.wantValid)
			}
			if !b.Valid() {
				if b.currIndex != len(b.values) {
					t.Errorf("Seek() = %v, want %v", b.currIndex, len(b.values))
				}
				return
			}
			key := b.values[b.currIndex].Key

			if bytes.Compare(key, tt.args.key) != 0 {
				t.Errorf("Seek() = %v, want %v", b.values[b.currIndex].Key, tt.args.key)
			}
			if b.Valid() {
				b.Next()
				if tt.fields.reverse {
					if bytes.Compare(b.values[b.currIndex].Key, key) != -1 {
						t.Errorf("Seek() = %v, want %v", b.values[b.currIndex].Key, key)
					}
				} else {
					if bytes.Compare(b.values[b.currIndex].Key, key) != 1 {
						t.Errorf("Seek() = %v, want %v", b.values[b.currIndex].Key, key)
					}
				}
			}
		})
	}
}

func Test_bTreeIterator_Valid(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "valid",
			fields: fields{
				currIndex: 0,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "not valid",
			fields: fields{
				currIndex: 3,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := bTreeIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			if got := b.Valid(); got != tt.want {
				t.Errorf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bTreeIterator_Value(t *testing.T) {
	type fields struct {
		currIndex int
		reverse   bool
		values    []*Item
	}
	tests := []struct {
		name   string
		fields fields
		want   *data.LogRecordPos
	}{
		{
			name: "get value",
			fields: fields{
				currIndex: 0,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
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
			b := bTreeIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			if got := b.Value(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newBTreeIterator(t *testing.T) {
	type args struct {
		tree    *btree.BTree
		reverse bool
	}
	type fields struct {
		values []*Item
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *bTreeIterator
	}{
		{
			name: "new iterator",
			args: args{
				tree:    btree.New(32),
				reverse: false,
			},
			want: &bTreeIterator{
				currIndex: 0,
				reverse:   false,
				values:    []*Item{},
			},
		},
		{
			name: "new iterator with value",
			fields: fields{
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
			args: args{
				tree:    btree.New(32),
				reverse: false,
			},
			want: &bTreeIterator{
				currIndex: 0,
				reverse:   false,
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
		},
		{
			name: "new iterator reverse",
			fields: fields{
				values: []*Item{
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
				},
			},
			args: args{
				tree:    btree.New(32),
				reverse: true,
			},
			want: &bTreeIterator{
				currIndex: 0,
				reverse:   true,
				values: []*Item{
					{
						Key: []byte("test3"),
						Pos: &data.LogRecordPos{
							Fid:    3,
							Offset: 1,
						},
					},
					{
						Key: []byte("test2"),
						Pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 1,
						},
					},
					{
						Key: []byte("test"),
						Pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, value := range tt.fields.values {
				tt.args.tree.ReplaceOrInsert(value)
			}
			if got := newBTreeIterator(tt.args.tree, tt.args.reverse); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newBTreeIterator() = %v, want %v", got, tt.want)
			}
		})
	}
}
