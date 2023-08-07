package index

import (
	"bytes"
	"fmt"
	"github.com/xiecang/bitcask/data"
	"os"
	"reflect"
	"testing"
)

var dirPathForBPlusTreeTest = os.TempDir()

func deleteBPTTestFile() {
	p := getBPlusTreeIndexFilePath(dirPathForBPlusTreeTest)
	err := os.Remove(p)
	if err != nil {
		fmt.Printf("failed to delete bplustree index file in test: %v", err)
	}
}

func TestBPlusTree_Delete(t *testing.T) {
	type kv struct {
		key []byte
		pos *data.LogRecordPos
	}
	type fields struct {
		values []kv
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     bool
		wantData *data.LogRecordPos
	}{
		{
			name: "test key nil",
			fields: fields{
				values: []kv{
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
				key: nil,
			},
			want: false,
		},
		{
			name: "test delete one",
			fields: fields{
				values: []kv{
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
			b := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			for _, value := range tt.fields.values {
				b.Put(value.key, value.pos)
			}
			if gotData, got := b.Delete(tt.args.key); got != tt.want {
				t.Errorf("Delete() = %v, want %v", got, tt.want)
			} else if !reflect.DeepEqual(gotData, tt.wantData) {
				t.Errorf("Delete() = %v, want %v", gotData, tt.wantData)
			}
		})
	}
}

func TestBPlusTree_Get(t *testing.T) {
	type kv struct {
		key []byte
		pos *data.LogRecordPos
	}
	type fields struct {
		values []kv
	}
	type args struct {
		key []byte
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
				values: []kv{
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
				key: nil,
			},
			want: nil,
		},
		{
			name: "test get one",
			fields: fields{
				values: []kv{
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
			},
			want: &data.LogRecordPos{
				Fid:    1,
				Offset: 1,
			},
		},
		{
			name: "cover",
			fields: fields{
				values: []kv{
					{
						key: []byte("test"),
						pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
					{
						key: []byte("test"),
						pos: &data.LogRecordPos{
							Fid:    2,
							Offset: 2,
						},
					},
				},
			},
			args: args{
				key: []byte("test"),
			},
			want: &data.LogRecordPos{
				Fid:    2,
				Offset: 2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			for _, value := range tt.fields.values {
				b.Put(value.key, value.pos)
			}
			if got := b.Get(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBPlusTree_Put(t *testing.T) {
	type kv struct {
		key []byte
		pos *data.LogRecordPos
	}
	type fields struct {
		values []kv
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
			args: args{
				key: nil,
				pos: nil,
			},
			want: nil,
		},
		{
			name: "test put nil",
			args: args{
				key: []byte("test"),
				pos: nil,
			},
			want: nil,
		},
		{
			name: "test put1",
			args: args{
				key: []byte("test"),
				pos: &data.LogRecordPos{
					Fid:    1,
					Offset: 1,
				},
			},
			want: nil,
		},
		{
			name: "test put2",
			fields: fields{
				values: []kv{
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			for _, value := range tt.fields.values {
				b.Put(value.key, value.pos)
			}
			if got := b.Put(tt.args.key, tt.args.pos); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Put() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBPlusTree_Size(t *testing.T) {
	type kv struct {
		key []byte
		pos *data.LogRecordPos
	}
	type fields struct {
		values []kv
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			name: "test size",
			fields: fields{
				values: []kv{
					{
						key: []byte("test"),
						pos: &data.LogRecordPos{
							Fid:    1,
							Offset: 1,
						},
					},
				},
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			for _, value := range tt.fields.values {
				b.Put(value.key, value.pos)
			}
			if got := b.Size(); got != tt.want {
				t.Errorf("Size() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBPlusTree_Iterator(t *testing.T) {
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
		wantValid bool
	}{
		{
			name: "empty iterator",
			args: args{
				reverse: false,
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
			wantValid: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			for _, value := range tt.fields.items {
				b.Put(value.Key, value.Pos)
			}
			got := b.Iterator(tt.args.reverse)
			if got.Valid() != tt.wantValid {
				t.Errorf("Valid() = %v, want %v", got.Valid(), tt.wantValid)
			}

			for got.Rewind(); got.Valid(); got.Next() {
				t.Logf("key: %s, pos: %v", string(got.Key()), got.Value())
			}
		})
	}
}

func Test_bPlusTreeIterator_Close(t *testing.T) {
	type fields struct {
		reverse bool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name:   "test close",
			fields: fields{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpt := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			b := newBPlusTreeIterator(bpt.tree, tt.fields.reverse)
			b.Close()
		})
	}
}

func Test_bPlusTreeIterator_Key(t *testing.T) {
	type fields struct {
		reverse bool
		values  []*Item
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			name: "get key",
			fields: fields{
				reverse: false,
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
			defer deleteBPTTestFile()
			bpt := NewBPlusTree(dirPathForBPlusTreeTest, false)
			for _, value := range tt.fields.values {
				bpt.Put(value.Key, value.Pos)
			}
			b := newBPlusTreeIterator(bpt.tree, tt.fields.reverse)
			if got := b.Key(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bPlusTreeIterator_Next(t *testing.T) {
	type fields struct {
		reverse bool
		values  []*Item
	}
	tests := []struct {
		name      string
		fields    fields
		wantFiled Item
	}{
		{
			name: "next key",
			fields: fields{
				reverse: false,
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
			wantFiled: Item{
				Key: []byte("test2"),
				Pos: &data.LogRecordPos{
					Fid:    2,
					Offset: 1,
				},
			},
		},
		{
			name: "next key reverse",
			fields: fields{
				reverse: true,
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
					{
						Key: []byte("test4"),
						Pos: &data.LogRecordPos{
							Fid:    4,
							Offset: 1,
						},
					},
				},
			},
			wantFiled: Item{
				Key: []byte("test3"),
				Pos: &data.LogRecordPos{
					Fid:    3,
					Offset: 1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpt := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			for _, value := range tt.fields.values {
				bpt.Put(value.Key, value.Pos)
			}
			b := newBPlusTreeIterator(bpt.tree, tt.fields.reverse)
			b.Next()
			if got := b.Key(); !reflect.DeepEqual(got, tt.wantFiled.Key) {
				t.Errorf("Key() = %v, want %v", got, tt.wantFiled.Key)
			}
			if got := b.Value(); !reflect.DeepEqual(got, tt.wantFiled.Pos) {
				t.Errorf("Value() = %v, want %v", got, tt.wantFiled.Pos)
			}
		})
	}
}

func Test_bPlusTreeIterator_Rewind(t *testing.T) {
	type fields struct {
		reverse bool
		values  []*Item
	}
	tests := []struct {
		name        string
		fields      fields
		wantCurrent Item
	}{
		{
			name: "rewind",
			fields: fields{
				reverse: false,
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
			wantCurrent: Item{
				Key: []byte("test"),
				Pos: &data.LogRecordPos{
					Fid:    1,
					Offset: 1,
				},
			},
		},
		{
			name: "rewind reverse",
			fields: fields{
				reverse: true,
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
			wantCurrent: Item{
				Key: []byte("test3"),
				Pos: &data.LogRecordPos{
					Fid:    3,
					Offset: 1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpt := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			for _, value := range tt.fields.values {
				bpt.Put(value.Key, value.Pos)
			}
			b := newBPlusTreeIterator(bpt.tree, tt.fields.reverse)
			b.Rewind()
			if bytes.Compare(b.Key(), tt.wantCurrent.Key) != 0 {
				t.Errorf("Key() = %v, want %v", b.Key(), tt.wantCurrent.Key)
			}
			if !reflect.DeepEqual(b.Value(), tt.wantCurrent.Pos) {
				t.Errorf("Value() = %v, want %v", b.Value(), tt.wantCurrent.Pos)
			}
		})
	}
}

func Test_bPlusTreeIterator_Seek(t *testing.T) {
	type fields struct {
		reverse bool
		values  []*Item
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantValid bool
		wantCurr  Item
	}{
		{
			name: "seek",
			fields: fields{
				reverse: false,
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
			wantCurr: Item{
				Key: []byte("test2"),
				Pos: &data.LogRecordPos{
					Fid:    2,
					Offset: 1,
				},
			},
		},
		{
			name: "seek reverse",
			fields: fields{
				reverse: true,
				values: []*Item{
					{
						Key: []byte("test4"),
						Pos: &data.LogRecordPos{
							Fid:    4,
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
			wantCurr: Item{
				Key: []byte("test2"),
				Pos: &data.LogRecordPos{
					Fid:    2,
					Offset: 1,
				},
			},
		},
		{
			name: "seek to max",
			fields: fields{
				reverse: false,
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
			// btree will return the empty item
			wantCurr: Item{
				Key: []byte(""),
				Pos: &data.LogRecordPos{
					Fid:    0,
					Offset: 0,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpt := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			for _, value := range tt.fields.values {
				bpt.Put(value.Key, value.Pos)
			}
			b := newBPlusTreeIterator(bpt.tree, tt.fields.reverse)
			b.Seek(tt.args.key)

			if tt.wantValid != b.Valid() {
				t.Errorf("Seek() = %v, want %v", b.Valid(), tt.wantValid)
			}

			if bytes.Compare(b.Key(), tt.wantCurr.Key) != 0 {
				t.Errorf("Key() = %v, want %v", b.Key(), tt.wantCurr.Key)
			}

			if !reflect.DeepEqual(b.Value(), tt.wantCurr.Pos) {
				t.Errorf("Value() = %v, want %v", b.Value(), tt.wantCurr.Pos)
			}
		})
	}
}

func Test_bPlusTreeIterator_Valid(t *testing.T) {
	type fields struct {
		reverse bool
		values  []*Item
	}
	tests := []struct {
		name    string
		fields  fields
		seekKey []byte
		want    bool
	}{
		{
			name: "valid",
			fields: fields{
				reverse: false,
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
			seekKey: []byte("test"),
			want:    true,
		},
		{
			name: "not valid",
			fields: fields{
				reverse: false,
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
			seekKey: []byte("test8"),
			want:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpt := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			for _, value := range tt.fields.values {
				bpt.Put(value.Key, value.Pos)
			}
			b := newBPlusTreeIterator(bpt.tree, tt.fields.reverse)
			b.Seek(tt.seekKey)
			if got := b.Valid(); got != tt.want {
				t.Errorf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bPlusTreeIterator_Value(t *testing.T) {
	type fields struct {
		reverse bool
		values  []*Item
	}
	tests := []struct {
		name   string
		fields fields
		want   *data.LogRecordPos
	}{
		{
			name: "get value",
			fields: fields{
				reverse: false,
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
			bpt := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			for _, value := range tt.fields.values {
				bpt.Put(value.Key, value.Pos)
			}
			b := newBPlusTreeIterator(bpt.tree, tt.fields.reverse)
			if got := b.Value(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newBPlusTreeIterator(t *testing.T) {
	type args struct {
		reverse bool
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "new iterator",
			args: args{
				reverse: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpt := NewBPlusTree(dirPathForBPlusTreeTest, false)
			defer deleteBPTTestFile()
			newBPlusTreeIterator(bpt.tree, tt.args.reverse)
		})
	}
}
