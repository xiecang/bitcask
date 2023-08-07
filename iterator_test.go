package bitcask_go

import (
	"github.com/xiecang/bitcask/data"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func defaultOptions() Options {
	return Options{
		DirPath:     filepath.Join(os.TempDir(), "bitcask-go"),
		MaxFileSize: 1 * 1024 * 1024,
		SyncWrites:  false,
		IndexType:   BTree,
	}
}
func defaultIteratorOption() *IteratorOption {
	return &IteratorOption{
		Prefix:  nil,
		Reverse: false,
	}
}

func destroyDB(db *DB) {
	if db != nil {
		_ = db.Close()
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestDB_NewIterator(t *testing.T) {
	type fields struct {
		options Options
	}
	type args struct {
		opt *IteratorOption
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantError bool
		wantValid bool
	}{
		{
			name: "test",
			fields: fields{
				options: defaultOptions(),
			},
			args: args{
				opt: defaultIteratorOption(),
			},
			wantError: false,
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.fields.options)
			if (err != nil) != tt.wantError {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantError)
				return
			}
			defer destroyDB(db)

			got := db.NewIterator(tt.args.opt)
			if got.Valid() != tt.wantValid {
				t.Errorf("NewIterator() got = %v, want %v", got.Valid(), tt.wantValid)
			}
		})
	}
}

func TestIterator_Close(t *testing.T) {
	type fields struct {
		options        Options
		iteratorOption *IteratorOption
	}
	tests := []struct {
		name      string
		fields    fields
		wantError bool
	}{
		{
			name: "test close",
			fields: fields{
				options:        defaultOptions(),
				iteratorOption: defaultIteratorOption(),
			},
			wantError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.fields.options)
			if (err != nil) != tt.wantError {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantError)
				return
			}
			defer destroyDB(db)
			i := db.NewIterator(tt.fields.iteratorOption)
			i.Close()
		})
	}
}

func TestIterator_Key(t *testing.T) {
	type fields struct {
		options        Options
		iteratorOption *IteratorOption
		values         []data.LogRecord
	}
	tests := []struct {
		name      string
		fields    fields
		want      []byte
		wantError bool
	}{
		{
			name: "test key",
			fields: fields{
				options:        defaultOptions(),
				iteratorOption: defaultIteratorOption(),
				values: []data.LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			want:      []byte("key"),
			wantError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.fields.options)
			if (err != nil) != tt.wantError {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantError)
				return
			}
			defer destroyDB(db)
			for _, value := range tt.fields.values {
				err = db.Put(value.Key, value.Value)
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}
			}
			i := db.NewIterator(tt.fields.iteratorOption)
			if got := i.Key(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIterator_Next(t *testing.T) {
	type fields struct {
		options        Options
		iteratorOption *IteratorOption
		values         []data.LogRecord
	}
	tests := []struct {
		name      string
		fields    fields
		next      data.LogRecord
		wantError bool
	}{
		{
			name: "test next",
			fields: fields{
				options:        defaultOptions(),
				iteratorOption: defaultIteratorOption(),
				values: []data.LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			next: data.LogRecord{
				Key:   []byte("key2"),
				Value: []byte("value2"),
			},
			wantError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.fields.options)
			if (err != nil) != tt.wantError {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantError)
				return
			}
			defer destroyDB(db)
			for _, value := range tt.fields.values {
				err = db.Put(value.Key, value.Value)
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}
			}
			i := db.NewIterator(tt.fields.iteratorOption)
			i.Next()
			if key := i.Key(); !reflect.DeepEqual(key, tt.next.Key) {
				t.Errorf("Key() = %v, want %v", key, tt.next.Key)
			}
			value, err := i.Value()
			if (err != nil) != tt.wantError {
				t.Errorf("Value() error = %v", err)
			}
			if !reflect.DeepEqual(value, tt.next.Value) {
				t.Errorf("Value() = %v, want %v", value, tt.next.Value)
			}
		})
	}
}

func TestIterator_Rewind(t *testing.T) {
	type fields struct {
		options        Options
		iteratorOption *IteratorOption
		values         []data.LogRecord
	}
	tests := []struct {
		name      string
		fields    fields
		wantData  data.LogRecord
		wantError bool
	}{
		{
			name: "test rewind",
			fields: fields{
				options:        defaultOptions(),
				iteratorOption: defaultIteratorOption(),
				values: []data.LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			wantData: data.LogRecord{
				Key:   []byte("key"),
				Value: []byte("value"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.fields.options)
			if (err != nil) != tt.wantError {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantError)
				return
			}
			defer destroyDB(db)
			for _, value := range tt.fields.values {
				err = db.Put(value.Key, value.Value)
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}
			}
			i := db.NewIterator(tt.fields.iteratorOption)
			i.Rewind()
			if key := i.Key(); !reflect.DeepEqual(key, tt.wantData.Key) {
				t.Errorf("Key() = %v, want %v", key, tt.wantData.Key)
			}
			value, err := i.Value()
			if (err != nil) != tt.wantError {
				t.Errorf("Value() error = %v", err)
			}
			if !reflect.DeepEqual(value, tt.wantData.Value) {
				t.Errorf("Value() = %v, want %v", value, tt.wantData.Value)
			}
		})
	}
}

func TestIterator_Seek(t *testing.T) {
	type fields struct {
		options        Options
		iteratorOption *IteratorOption
		values         []data.LogRecord
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantError bool
		wantCur   data.LogRecord
	}{
		{
			name: "test seek",
			fields: fields{
				options:        defaultOptions(),
				iteratorOption: defaultIteratorOption(),
				values: []data.LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			args: args{
				key: []byte("key2"),
			},
			wantCur: data.LogRecord{
				Key:   []byte("key2"),
				Value: []byte("value2"),
			},
			wantError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.fields.options)
			if (err != nil) != tt.wantError {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantError)
				return
			}
			defer destroyDB(db)
			for _, value := range tt.fields.values {
				err = db.Put(value.Key, value.Value)
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}
			}
			i := db.NewIterator(tt.fields.iteratorOption)
			i.Seek(tt.args.key)
			if key := i.Key(); !reflect.DeepEqual(key, tt.wantCur.Key) {
				t.Errorf("Key() = %v, want %v", key, tt.wantCur.Key)
			}
			value, err := i.Value()
			if (err != nil) != tt.wantError {
				t.Errorf("Value() error = %v", err)
			}
			if !reflect.DeepEqual(value, tt.wantCur.Value) {
				t.Errorf("Value() = %v, want %v", value, tt.wantCur.Value)
			}
		})
	}
}

func TestIterator_Valid(t *testing.T) {
	type fields struct {
		options        Options
		iteratorOption *IteratorOption
		values         []data.LogRecord
	}
	tests := []struct {
		name      string
		fields    fields
		want      bool
		wantError bool
	}{
		{
			name: "valid",
			fields: fields{
				options:        defaultOptions(),
				iteratorOption: defaultIteratorOption(),
				values: []data.LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			wantError: false,
			want:      true,
		},
		{
			name: "invalid",
			fields: fields{
				options:        defaultOptions(),
				iteratorOption: defaultIteratorOption(),
				values:         []data.LogRecord{},
			},
			wantError: false,
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.fields.options)
			if (err != nil) != tt.wantError {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantError)
				return
			}
			defer destroyDB(db)
			for _, value := range tt.fields.values {
				err = db.Put(value.Key, value.Value)
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}
			}
			i := db.NewIterator(tt.fields.iteratorOption)
			if got := i.Valid(); got != tt.want {
				t.Errorf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIterator_Value(t *testing.T) {
	type fields struct {
		options        Options
		iteratorOption *IteratorOption
		values         []data.LogRecord
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{
			name: "test value",
			fields: fields{
				options:        defaultOptions(),
				iteratorOption: defaultIteratorOption(),
				values: []data.LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			want:    []byte("value"),
			wantErr: false,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.fields.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer destroyDB(db)
			for _, value := range tt.fields.values {
				err = db.Put(value.Key, value.Value)
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}
			}
			i := db.NewIterator(tt.fields.iteratorOption)
			got, err := i.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func TestIterator_skipToNext(t *testing.T) {
	type fields struct {
		options        Options
		iteratorOption *IteratorOption
		values         []data.LogRecord
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		wantCur data.LogRecord
	}{
		{
			name: "test skipToNext",
			fields: fields{
				options:        defaultOptions(),
				iteratorOption: defaultIteratorOption(),
				values: []data.LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			wantErr: false,
			wantCur: data.LogRecord{
				Key:   []byte("key2"),
				Value: []byte("value2"),
			},
		},
		{
			name: "test skipToNext",
			fields: fields{
				options: defaultOptions(),
				iteratorOption: &IteratorOption{
					Reverse: true,
					Prefix:  []byte("key"),
				},
				values: []data.LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					{
						Key:   []byte("a"),
						Value: []byte("value2"),
					},
					{
						Key:   []byte("b"),
						Value: []byte("value2"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
			wantCur: data.LogRecord{
				Key:   []byte("key2"),
				Value: []byte("value2"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.fields.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer destroyDB(db)
			for _, value := range tt.fields.values {
				err = db.Put(value.Key, value.Value)
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}
			}
			i := db.NewIterator(tt.fields.iteratorOption)
			i.skipToNext()
			if valid := i.Valid(); !valid {
				t.Errorf("Valid() = %v, want %v", valid, true)
			}

		})
	}
}
