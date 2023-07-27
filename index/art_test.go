package index

import (
	"bitcask-go/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"reflect"
	"sync"
	"testing"
)

func TestAdaptiveRadixTree_Delete(t *testing.T) {
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
			art := NewART()
			for _, value := range tt.fields.values {
				art.Put(value.key, value.pos)
			}
			if oldData, got := art.Delete(tt.args.key); got != tt.want {
				t.Errorf("Delete() = %v, want %v", got, tt.want)
			} else if !reflect.DeepEqual(oldData, tt.wantData) {
				t.Errorf("Delete() = %v, want %v", oldData, tt.wantData)
			}
		})
	}
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
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
			art := NewART()
			for _, value := range tt.fields.values {
				art.Put(value.key, value.pos)
			}
			if got := art.Get(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
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
			want: &artIterator{
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
			want: &artIterator{
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
			want: &artIterator{
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
			art := NewART()
			for _, item := range tt.fields.items {
				art.Put(item.Key, item.Pos)
			}
			got := art.Iterator(tt.args.reverse)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Iterator() = %v, want %v", got, tt.want)
			}
			if got.Valid() != tt.wantValid {
				t.Errorf("Valid() = %v, want %v", got.Valid(), tt.wantValid)
			}
		})
	}
}

func TestAdaptiveRadixTree_Put(t *testing.T) {
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
			art := NewART()
			for _, item := range tt.fields.values {
				art.Put(item.key, item.pos)
			}
			if got := art.Put(tt.args.key, tt.args.pos); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Put() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
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
			art := NewART()
			for _, value := range tt.fields.values {
				art.Put(value.key, value.pos)
			}
			if got := art.Size(); got != tt.want {
				t.Errorf("Size() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewART(t *testing.T) {
	tests := []struct {
		name string
		want *AdaptiveRadixTree
	}{
		{
			name: "test new art",
			want: &AdaptiveRadixTree{
				tree: goart.New(),
				lock: new(sync.RWMutex),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewART(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewART() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_artIterator_Close(t *testing.T) {
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
			ai := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			ai.Close()
			if ai.values != nil {
				t.Errorf("values not nil, values: %+v", ai.values)
			}
		})
	}
}

func Test_artIterator_Key(t *testing.T) {
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
			ai := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			if got := ai.Key(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_artIterator_Next(t *testing.T) {
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
			ai := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			var index = ai.currIndex
			var wantKey = ai.values[index+1].Key

			ai.Next()
			var currentKey = ai.values[ai.currIndex].Key
			if bytes.Compare(currentKey, wantKey) != 0 {
				t.Errorf("Next() = %v, want %v", currentKey, wantKey)
			}

		})
	}
}

func Test_artIterator_Rewind(t *testing.T) {
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
			ai := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			ai.Rewind()
			if bytes.Compare(ai.values[ai.currIndex].Key, tt.fields.values[0].Key) != 0 {
				t.Errorf("Rewind() = %v, want %v", ai.values[ai.currIndex].Key, tt.fields.values[0].Key)
			}
		})
	}
}

func Test_artIterator_Seek(t *testing.T) {
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
			ai := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			ai.Seek(tt.args.key)

			if tt.wantValid != ai.Valid() {
				t.Errorf("Seek() = %v, want %v", ai.Valid(), tt.wantValid)
			}
			if !ai.Valid() {
				if ai.currIndex != len(ai.values) {
					t.Errorf("Seek() = %v, want %v", ai.currIndex, len(ai.values))
				}
				return
			}
			key := ai.values[ai.currIndex].Key

			if bytes.Compare(key, tt.args.key) != 0 {
				t.Errorf("Seek() = %v, want %v", ai.values[ai.currIndex].Key, tt.args.key)
			}
			if ai.Valid() {
				ai.Next()
				if tt.fields.reverse {
					if bytes.Compare(ai.values[ai.currIndex].Key, key) != -1 {
						t.Errorf("Seek() = %v, want %v", ai.values[ai.currIndex].Key, key)
					}
				} else {
					if bytes.Compare(ai.values[ai.currIndex].Key, key) != 1 {
						t.Errorf("Seek() = %v, want %v", ai.values[ai.currIndex].Key, key)
					}
				}
			}
		})
	}
}

func Test_artIterator_Valid(t *testing.T) {
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
			ai := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			if got := ai.Valid(); got != tt.want {
				t.Errorf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_artIterator_Value(t *testing.T) {
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
			ai := &artIterator{
				currIndex: tt.fields.currIndex,
				reverse:   tt.fields.reverse,
				values:    tt.fields.values,
			}
			if got := ai.Value(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newARTIterator(t *testing.T) {
	type fields struct {
		values []*Item
	}
	type args struct {
		tree    goart.Tree
		reverse bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *artIterator
	}{
		{
			name: "new iterator",
			args: args{
				tree:    goart.New(),
				reverse: false,
			},
			want: &artIterator{
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
				tree:    goart.New(),
				reverse: false,
			},
			want: &artIterator{
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
				tree:    goart.New(),
				reverse: true,
			},
			want: &artIterator{
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
				tt.args.tree.Insert(value.Key, value.Pos)
			}

			if got := newARTIterator(tt.args.tree, tt.args.reverse); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newARTIterator() = %v, want %v", got, tt.want)
			}
		})
	}
}
