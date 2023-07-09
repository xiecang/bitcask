package bitcask_go

import (
	"bitcask-go/data"
	"reflect"
	"testing"
)

func TestDB_ListKeys(t *testing.T) {
	type fields struct {
		options Options
		values  []data.LogRecord
	}
	tests := []struct {
		name    string
		fields  fields
		want    [][]byte
		wantErr bool
	}{
		{
			name: "list keys",
			fields: fields{
				options: defaultOptions(),
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
			want: [][]byte{
				[]byte("key"),
				[]byte("key2"),
			},
			wantErr: false,
		},
		{
			name: "empty",
			fields: fields{
				options: defaultOptions(),
				values:  []data.LogRecord{},
			},
			want:    [][]byte{},
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

			if (err != nil) != tt.wantErr {
				t.Errorf("Open db error, err: %v", err)
				return
			}
			if got := db.ListKeys(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Fold(t *testing.T) {
	type fields struct {
		options Options
		values  []data.LogRecord
	}
	type args struct {
		fn func(key, value []byte) bool
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
				options: defaultOptions(),
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
				fn: func(key, value []byte) bool {
					return true
				},
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
			if (err != nil) != tt.wantErr {
				t.Errorf("Open db error, err: %v", err)
				return
			}
			if err = db.Fold(tt.args.fn); (err != nil) != tt.wantErr {
				t.Errorf("Fold() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_Close(t *testing.T) {
	type fields struct {
		options Options
		values  []data.LogRecord
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				options: defaultOptions(),
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
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(tt.fields.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for _, value := range tt.fields.values {
				err = db.Put(value.Key, value.Value)
				if err != nil {
					t.Errorf("Put() error = %v", err)
				}
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Open db error, err: %v", err)
				return
			}
			if err := db.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_Sync(t *testing.T) {
	type fields struct {
		options Options
		values  []data.LogRecord
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				options: defaultOptions(),
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
			if (err != nil) != tt.wantErr {
				t.Errorf("Open db error, err: %v", err)
				return
			}
			if err := db.Sync(); (err != nil) != tt.wantErr {
				t.Errorf("Sync() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
