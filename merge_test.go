package bitcask_go

import (
	"bytes"
	"github.com/xiecang/bitcask/data"
	"os"
	"path/filepath"
	"testing"
)

func TestDB_Merge(t *testing.T) {
	type fields struct {
		options         Options
		values          []data.LogRecord
		valuesWhenMerge []data.LogRecord
	}
	tests := []struct {
		name                  string
		fields                fields
		wantErr               bool
		wantHintFile          bool             // merge 后，是否有 hint 文件
		wantDataFile          bool             // merge 后，是否有 data 文件
		wantMergeFinishedFile bool             // merge 后，是否有 merge finished 文件
		wantRead              []data.LogRecord // merge 后，读取的数据
	}{
		{
			name: "empty data",
			fields: fields{
				options: defaultOptions(),
			},
			wantErr: false,
		},
		{
			name: "normal data",
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
			wantErr:               false,
			wantDataFile:          true,
			wantHintFile:          true,
			wantMergeFinishedFile: true,
			wantRead: []data.LogRecord{
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
		{
			name: "delete data",
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
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
						Type:  data.LogRecordTypeDelete,
					},
				},
			},
			wantErr:               false,
			wantMergeFinishedFile: true,
			wantDataFile:          true,
			wantHintFile:          true,
			wantRead: []data.LogRecord{
				{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
		},
		{
			name: "delete all data",
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
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
						Type:  data.LogRecordTypeDelete,
					},
					{
						Key:   []byte("key"),
						Value: []byte("value"),
						Type:  data.LogRecordTypeDelete,
					},
				},
			},
			wantErr:               false,
			wantDataFile:          false,
			wantHintFile:          false,
			wantMergeFinishedFile: false,
		},
		{
			name: "dup data",
			fields: fields{
				options: defaultOptions(),
				values: []data.LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
						Type:  data.LogRecordTypeDelete,
					},
				},
			},
			wantErr:               false,
			wantMergeFinishedFile: true,
			wantDataFile:          true,
			wantHintFile:          true,
			wantRead: []data.LogRecord{
				{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
		},
		{
			name: "put and delete data when merge",
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
				valuesWhenMerge: []data.LogRecord{
					{
						Key:   []byte("key"),
						Value: []byte("value"),
						Type:  data.LogRecordTypeDelete,
					},
					{
						Key:   []byte("key3"),
						Value: []byte("value3"),
					},
					{
						Key:   []byte("key4"),
						Value: []byte("value4"),
					},
				},
			},
			wantErr:               false,
			wantMergeFinishedFile: true,
			wantDataFile:          true,
			wantHintFile:          true,
			wantRead: []data.LogRecord{
				{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				},
				{
					Key:   []byte("key3"),
					Value: []byte("value3"),
				},
				{
					Key:   []byte("key4"),
					Value: []byte("value4"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := tt.fields.options
			db, err := Open(options)
			if err != nil {
				t.Errorf("Open() error = %v", err)
				return
			}
			defer destroyDB(db)
			for _, value := range tt.fields.values {
				if value.Type == data.LogRecordTypeDelete {
					err = db.Delete(value.Key)
					if err != nil {
						t.Errorf("Delete() error = %v", err)
					}
				} else {
					err = db.Put(value.Key, value.Value)
					if err != nil {
						t.Errorf("Put() error = %v", err)
					}
				}
			}
			if err = db.Merge(); (err != nil) != tt.wantErr {
				t.Errorf("Merge() error = %v, wantErr %v", err, tt.wantErr)
			}
			for _, record := range tt.fields.valuesWhenMerge {
				if record.Type == data.LogRecordTypeDelete {
					err = db.Delete(record.Key)
					if err != nil {
						t.Errorf("Delete() error = %v", err)
					}
				} else {
					err = db.Put(record.Key, record.Value)
					if err != nil {
						t.Errorf("Put() error = %v", err)
					}
				}

				// 这里应该校验，新写入的数据不在 merge 文件中，下次一定
			}
			var mergePath = db.getMergePath()

			defer func() {
				// 删除 merge 文件
				_ = os.Remove(data.MergeFinishedFileName(options.DirPath))
				_ = os.Remove(data.GetHintFileName(options.DirPath))
				_ = os.RemoveAll(mergePath)
			}()

			if tt.wantMergeFinishedFile {
				if _, err = os.Stat(data.MergeFinishedFileName(mergePath)); os.IsNotExist(err) {
					t.Errorf("Merge() merge finished file not exist. error = %v, wantErr %v", err, tt.wantErr)
				}
			}

			if tt.wantHintFile {
				if _, err = os.Stat(data.GetHintFileName(mergePath)); os.IsNotExist(err) {
					t.Errorf("Merge() hit file not exist. error = %v, wantErr %v", err, tt.wantErr)
				}
			}

			if tt.wantDataFile {
				var dataFileExist bool
				err = filepath.Walk(mergePath, func(path string, info os.FileInfo, err error) error {
					if !info.IsDir() && filepath.Ext(path) == data.FileNameSuffix {
						dataFileExist = true
						return nil
					}
					return nil
				})
				if err != nil {
					t.Errorf("Merge() error = %v, wantErr %v", err, tt.wantErr)
				}
				if !dataFileExist {
					t.Errorf("Merge() data file not exist. error = %v, wantErr %v", err, tt.wantErr)
				}
			}

			//
			err = db.Close()
			if err != nil {
				t.Errorf("Close() error = %v", err)
			}

			// 重新打开数据库
			db, err = Open(options)
			if err != nil {
				t.Errorf("Open() error = %v", err)
				return
			}
			for _, record := range tt.wantRead {
				value, err := db.Get(record.Key)
				if err != nil {
					t.Errorf("Get() error = %v", err)
				}
				if !bytes.Equal(value, record.Value) {
					t.Errorf("Get() value = %v, want %v", value, record.Value)
				}
			}
		})
	}
}
