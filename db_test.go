package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/utils"
	"fmt"
	"os"
	gopath "path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var indexTypesForTest = []IndexType{Btree, BPlusTree, ART}

func indexTypeString(t IndexType) string {
	switch t {
	case Btree:
		return "Btree"
	case ART:
		return "ART"
	case BPlusTree:
		return "BPlusTree"
	default:
		return "Unknown"
	}
}

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
		for _, indexType := range indexTypesForTest {
			name := fmt.Sprintf("%s-indexTYpe_%s", tt.name, indexTypeString(indexType))
			options := tt.fields.options
			options.IndexType = indexType
			t.Run(name, func(t *testing.T) {
				db, err := Open(options)
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
		for _, indexType := range indexTypesForTest {
			name := fmt.Sprintf("%s-indexTYpe_%s", tt.name, indexTypeString(indexType))
			options := tt.fields.options
			options.IndexType = indexType
			t.Run(name, func(t *testing.T) {
				db, err := Open(options)
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
		for _, indexType := range indexTypesForTest {
			name := fmt.Sprintf("%s-indexTYpe_%s", tt.name, indexTypeString(indexType))
			options := tt.fields.options
			options.IndexType = indexType

			t.Run(name, func(t *testing.T) {
				db, err := Open(options)
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
		for _, indexType := range indexTypesForTest {
			name := fmt.Sprintf("%s-indexTYpe_%s", tt.name, indexTypeString(indexType))
			options := tt.fields.options
			options.IndexType = indexType

			t.Run(name, func(t *testing.T) {
				db, err := Open(options)
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
				if err = db.Sync(); (err != nil) != tt.wantErr {
					t.Errorf("Sync() error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	}
}

func TestDB_Backup(t *testing.T) {
	type fields struct {
		options Options
		values  []data.LogRecord
	}
	type args struct {
		dir string
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
				dir: filepath.Join(os.TempDir(), "bitcask_backup"),
			},
			wantErr: false,
		},
		{
			name: "same dir",
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
				dir: filepath.Join(os.TempDir(), "bitcask-go"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		for _, indexType := range indexTypesForTest {
			name := fmt.Sprintf("%s-indexTYpe_%s", tt.name, indexTypeString(indexType))
			options := tt.fields.options
			options.IndexType = indexType

			t.Run(name, func(t *testing.T) {
				t.Logf("backup dir: %s", tt.args.dir)
				db, err := Open(options)
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
				if err = db.Backup(tt.args.dir); (err != nil) != tt.wantErr {
					t.Errorf("Backup() error = %v, wantErr %v", err, tt.wantErr)
				}

				defer func() {
					err = os.RemoveAll(tt.args.dir)
					if err != nil {
						t.Errorf("remove backup dir error: %v", err)
					}
				}()

				// check backup files
				err = filepath.Walk(tt.fields.options.DirPath, func(path string, info os.FileInfo, err error) error {
					if strings.Compare(path, tt.fields.options.DirPath) == 0 {
						return nil
					}
					destPath := filepath.Join(tt.args.dir, info.Name())
					filename := gopath.Base(path)

					if filename == fileLockName {
						return nil
					}

					_, err = os.Stat(destPath)
					if err != nil {
						return err
					}
					// 说明原文件里面的内容在备份文件中均存在

					// 对比文件内容 md5 ? 下次一定

					return err
				})
				if (err != nil) != tt.wantErr {
					t.Errorf("filepath.Walk() error = %v, wantErr %v", err, tt.wantErr)
				}

			})
		}
	}
}

func TestDB_Delete(t *testing.T) {
	type fields struct {
		options Options
		values  []data.LogRecord
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name              string
		fields            fields
		args              args
		valuesAfterDelete data.LogRecord
		wantErr           bool
		wantPutErr        bool
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
				key: []byte("key"),
			},
			valuesAfterDelete: data.LogRecord{
				Key:   []byte("key"),
				Value: []byte("value111"),
			},
			wantErr: false,
		},
		{
			name: "not exist",
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
				key: []byte("key3"),
			},
			valuesAfterDelete: data.LogRecord{
				Key:   []byte("key3"),
				Value: []byte("value333"),
			},
			wantErr:    false,
			wantPutErr: false,
		},
		{
			name: "empty key",
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
				key: []byte(""),
			},
			valuesAfterDelete: data.LogRecord{
				Key:   []byte(""),
				Value: []byte("value333"),
			},
			wantErr:    true,
			wantPutErr: true,
		},
		{
			name: "nil key",
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
				key: nil,
			},
			valuesAfterDelete: data.LogRecord{
				Key:   nil,
				Value: []byte("value333"),
			},
			wantErr:    true,
			wantPutErr: true,
		},
	}
	for _, tt := range tests {
		for _, indexType := range indexTypesForTest {
			name := fmt.Sprintf("%s-indexTYpe_%s", tt.name, indexTypeString(indexType))
			options := tt.fields.options
			options.IndexType = indexType

			t.Run(name, func(t *testing.T) {
				db, err := Open(options)
				if err != nil {
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
				if err = db.Delete(tt.args.key); (err != nil) != tt.wantErr {
					t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
				} else {
					_, err = db.Get(tt.args.key)
					if err == nil {
						t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
					}
				}

				err = db.Close()
				if err != nil {
					t.Errorf("Close() error = %v", err)
				}
				// 重新打开数据库，检查数据是否存在
				db, err = Open(options)
				if err != nil {
					t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				_, err = db.Get(tt.args.key)
				if err == nil {
					t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
				}

				// 删除后重新插入，检查数据是否存在
				err = db.Put(tt.valuesAfterDelete.Key, tt.valuesAfterDelete.Value)
				if (err != nil) != tt.wantPutErr {
					t.Errorf("Put() error = %v", err)
				} else if !tt.wantPutErr {
					value, err := db.Get(tt.valuesAfterDelete.Key)
					if err != nil {
						t.Errorf("Get() error = %v", err)
					}
					if !reflect.DeepEqual(value, tt.valuesAfterDelete.Value) {
						t.Errorf("Get() value = %v, want %v", value, tt.valuesAfterDelete.Value)
					}
				}
			})
		}
	}
}

func TestDB_Get(t *testing.T) {
	type fields struct {
		options Options
		values  []data.LogRecord
		// 注入大量数据, 触发存储到旧文件
		moreValues bool
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
				key: []byte("key"),
			},
			want:    []byte("value"),
			wantErr: false,
		},
		{
			name: "empty key",
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
				key: []byte(""),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "nil key",
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
				key: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "not found",
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
				key: []byte("key3"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "not found in old file",
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
				moreValues: true,
			},
			args: args{
				key: []byte("key3"),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "found in old file",
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
				moreValues: true,
			},
			args: args{
				key: []byte("key"),
			},
			want:    []byte("value"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		for _, indexType := range indexTypesForTest {
			name := fmt.Sprintf("%s-indexTYpe_%s", tt.name, indexTypeString(indexType))
			options := tt.fields.options
			options.IndexType = indexType

			t.Run(name, func(t *testing.T) {
				db, err := Open(options)
				if err != nil {
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

				if tt.fields.moreValues {
					for i := 0; i < int(db.options.MaxFileSize/100); i++ {
						key := utils.RandomValue(10)
						value := utils.RandomValue(10)
						err = db.Put(key, value)
						if err != nil {
							t.Errorf("Put() error = %v", err)
						}
					}
				}

				got, err := db.Get(tt.args.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Get() got = %v, want %v", got, tt.want)
				}

				err = db.Close()
				if err != nil {
					t.Errorf("Close() error = %v", err)
				}

				// 重新打开数据库，检查数据是否存在
				db, err = Open(options)
				if err != nil {
					t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				got, err = db.Get(tt.args.key)
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
}

func TestDB_Put(t *testing.T) {
	type fields struct {
		options Options
		values  []data.LogRecord
		// 注入大量数据, 触发存储到旧文件
		moreValues bool
	}
	type args struct {
		key   []byte
		value []byte
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
			},
			args: args{
				key:   []byte("key"),
				value: []byte("value"),
			},
			wantErr: false,
		},
		{
			name: "put same data",
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
				key:   []byte("key"),
				value: []byte("value"),
			},
			wantErr: false,
		},
		{
			name: "put more data",
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
				moreValues: true,
			},
			args: args{
				key:   []byte("key"),
				value: []byte("value"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		for _, indexType := range indexTypesForTest {
			name := fmt.Sprintf("%s-indexTYpe_%s", tt.name, indexTypeString(indexType))
			options := tt.fields.options
			options.IndexType = indexType

			t.Run(name, func(t *testing.T) {
				db, err := Open(options)
				if err != nil {
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
				if tt.fields.moreValues {
					for i := 0; i < int(db.options.MaxFileSize/100); i++ {
						key := utils.RandomValue(10)
						value := utils.RandomValue(10)
						err = db.Put(key, value)
						if err != nil {
							t.Errorf("Put() error = %v", err)
						}
					}
				}
				if err = db.Put(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
					t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
				}

				err = db.Close()
				if err != nil {
					t.Errorf("Close() error = %v", err)
				}
				// 重新打开数据库，检查数据是否存在
				db, err = Open(options)
				if err != nil {
					t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				got, err := db.Get(tt.args.key)
				if err != nil {
					t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.args.value) {
					t.Errorf("Get() got = %v, want %v", got, tt.args.value)
				}
				// 重新 put
				if err = db.Put(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
					t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	}
}

func TestDB_Stat(t *testing.T) {
	type fields struct {
		options Options
		values  []data.LogRecord
	}
	tests := []struct {
		name   string
		fields fields
		want   *Stat
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
			want: &Stat{
				KeyNum:          2,
				DataFileNum:     1,
				ReclaimableSize: 0,
			},
		},
	}
	for _, tt := range tests {
		for _, indexType := range indexTypesForTest {
			name := fmt.Sprintf("%s-indexTYpe_%s", tt.name, indexTypeString(indexType))
			options := tt.fields.options
			options.IndexType = indexType

			t.Run(name, func(t *testing.T) {
				db, err := Open(options)
				if err != nil {
					t.Errorf("Open() error = %v", err)
					return
				}
				defer destroyDB(db)
				for _, value := range tt.fields.values {
					err = db.Put(value.Key, value.Value)
					if err != nil {
						t.Errorf("Put() error = %v", err)
					}
				}
				got := db.Stat()
				if !(got.KeyNum == tt.want.KeyNum && got.DataFileNum == tt.want.DataFileNum) {
					t.Errorf("Stat() = %v, want %v", got, tt.want)
				}
			})
		}
	}
}

func TestOpen(t *testing.T) {
	type fields struct {
		values []data.LogRecord
	}

	type args struct {
		options Options
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "empty data",
			args: args{
				options: defaultOptions(),
			},
			wantErr: false,
		},
		{
			name: "data before open",
			fields: fields{
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
				options: defaultOptions(),
			},
			wantErr: false,
		},
		{
			name: "BPlusTree index",
			args: args{
				options: Options{
					DirPath:     filepath.Join(os.TempDir(), "bitcask-go"),
					MaxFileSize: 1 * 1024 * 1024,
					SyncWrites:  false,
					IndexType:   BPlusTree,
				},
			},
			wantErr: false,
		},
		{
			name: "ART, index",
			args: args{
				options: Options{
					DirPath:     filepath.Join(os.TempDir(), "bitcask-go"),
					MaxFileSize: 1 * 1024 * 1024,
					SyncWrites:  false,
					IndexType:   ART,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.fields.values) > 0 {
				db, err := Open(tt.args.options)
				if err != nil {
					t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				for _, value := range tt.fields.values {
					err = db.Put(value.Key, value.Value)
					if err != nil {
						t.Errorf("Put() error = %v", err)
					}
				}
				err = db.Close()
				if err != nil {
					t.Errorf("Close() error = %v", err)
				}
			}

			db, err := Open(tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer destroyDB(db)

			// 检查是否有锁文件
			if _, err = os.Stat(fileLockPath(tt.args.options.DirPath)); os.IsNotExist(err) {
				t.Errorf("Open() error = %v", err)
			}

		})
	}

}
