package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/utils"
	"bytes"
	"reflect"
	"testing"
)

func defaultWriteBatchOption() WriteBatchOption {
	return WriteBatchOption{
		MaxBatchSize: 100,
		SyncWrites:   true,
	}
}

func TestDB_NewWriteBatch(t *testing.T) {
	type fields struct {
		options Options
	}
	type args struct {
		options WriteBatchOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test",
			fields: fields{
				options: defaultOptions(),
			},
			args: args{
				options: defaultWriteBatchOption(),
			},
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

			got := db.NewWriteBatch(tt.args.options)
			t.Logf("got: %+v", got)
		})
	}
}

func TestWriteBatch_Commit(t *testing.T) {
	type testLogRecord struct {
		data.LogRecord
		inDB    bool
		dbValue []byte
	}
	type fields struct {
		options          Options
		writeBatchOption WriteBatchOption
		records          []testLogRecord
	}
	tests := []struct {
		name      string
		fields    fields
		wantErr   bool
		wantSeqId uint64
	}{
		{
			name: "put and commit",
			fields: fields{
				options:          defaultOptions(),
				writeBatchOption: defaultWriteBatchOption(),
				records: []testLogRecord{
					{
						LogRecord: data.LogRecord{
							Key:   []byte("key"),
							Value: []byte("value"),
						},
						inDB:    true,
						dbValue: []byte("value"),
					},
					{
						LogRecord: data.LogRecord{
							Key:   utils.GetTestKey(3),
							Value: []byte("value2"),
						},
						inDB:    true,
						dbValue: []byte("value2"),
					},
				},
			},
			wantErr:   false,
			wantSeqId: 1,
		},
		{
			name: "commit empty",
			fields: fields{
				options:          defaultOptions(),
				writeBatchOption: defaultWriteBatchOption(),
			},
			wantErr:   false,
			wantSeqId: 0,
		},
		{
			name: "delete and commit",
			fields: fields{
				options:          defaultOptions(),
				writeBatchOption: defaultWriteBatchOption(),
				records: []testLogRecord{
					{
						LogRecord: data.LogRecord{
							Key:   []byte("key"),
							Value: []byte("value"),
						},
						inDB: false,
					},
					{
						LogRecord: data.LogRecord{
							Key:   []byte("key"),
							Value: []byte("value"),
							Type:  data.LogRecordTypeDelete,
						},
						inDB: false,
					},
					{
						LogRecord: data.LogRecord{
							Key:   utils.GetTestKey(3),
							Value: []byte("value2"),
						},
						inDB:    true,
						dbValue: []byte("value2"),
					},
				},
			},
			wantErr:   false,
			wantSeqId: 1,
		},
		{
			name: "put key with diff value and commit",
			fields: fields{
				options:          defaultOptions(),
				writeBatchOption: defaultWriteBatchOption(),
				records: []testLogRecord{
					{
						LogRecord: data.LogRecord{
							Key:   []byte("key"),
							Value: []byte("value"),
						},
						inDB:    true,
						dbValue: []byte("value333"),
					},
					{
						LogRecord: data.LogRecord{
							Key:   []byte("key"),
							Value: []byte("value"),
							Type:  data.LogRecordTypeDelete,
						},
						inDB:    false,
						dbValue: []byte("value333"),
					},
					{
						LogRecord: data.LogRecord{
							Key:   []byte("key"),
							Value: []byte("value333"),
						},
						inDB:    true,
						dbValue: []byte("value333"),
					},
					{
						LogRecord: data.LogRecord{
							Key:   utils.GetTestKey(3),
							Value: []byte("value2"),
						},
						inDB:    true,
						dbValue: []byte("value2"),
					},
				},
			},
			wantErr:   false,
			wantSeqId: 1,
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
			w := db.NewWriteBatch(tt.fields.writeBatchOption)

			for _, r := range tt.fields.records {
				if r.Type == data.LogRecordTypeDelete {
					if err = w.Delete(r.Key); (err != nil) != tt.wantErr {
						t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
						return
					}
				} else {
					if err = w.Put(r.Key, r.Value); (err != nil) != tt.wantErr {
						t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
						return
					}
				}
			}

			if err = w.Commit(); (err != nil) != tt.wantErr {
				t.Errorf("Commit() error = %v, wantErr %v", err, tt.wantErr)
			}

			for _, r := range tt.fields.records {
				var gotValue []byte
				gotValue, err = db.Get(r.Key)
				if err != nil {
					if (err == ErrKeyNotFound) && (!r.inDB) {
						// 未找到，且不应该在数据库里，正常情况
					} else {
						t.Errorf("提交后，未查询到值, %v", err)
						return
					}
				}
				if bytes.Compare(gotValue, r.dbValue) != 0 {
					t.Errorf("数据库中的值，与原值不符, valueInDB: %v, valueWant: %v", gotValue, r.dbValue)
				}
			}
			if db.seqId != tt.wantSeqId {
				t.Errorf("seqId 不符, got: %v, want: %v", db.seqId, tt.wantSeqId)
			}

			// 重启 db 后行为应保持一致
			if err = db.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close db error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			db2, err := Open(tt.fields.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for _, r := range tt.fields.records {
				var gotValue []byte
				gotValue, err = db2.Get(r.Key)
				if err != nil {
					if (err == ErrKeyNotFound) && (!r.inDB) {
						// 未找到，且不应该在数据库里，正常情况
					} else {
						t.Errorf("提交后，重启，未查询到值, %v", err)
						return
					}
				}
				if bytes.Compare(gotValue, r.dbValue) != 0 {
					t.Errorf("数据库中的值，与原值不符, valueInDB: %v, valueWant: %v", gotValue, r.dbValue)
				}
			}

			// 校验 seqId 是否递增
			if db2.seqId != tt.wantSeqId {
				t.Errorf("seqId 不符, got: %v, want: %v", db2.seqId, tt.wantSeqId)
			}
		})
	}

	t.Run("write greater than MaxBatchSize", func(t *testing.T) {
		db, err := Open(defaultOptions())
		if err != nil {
			t.Errorf("Open() error = %v", err)
			return
		}
		defer destroyDB(db)
		w := db.NewWriteBatch(defaultWriteBatchOption())

		var maxSize = int(w.options.MaxBatchSize)
		for i := 0; i < maxSize+1; i++ {
			if err = w.Put(utils.GetTestKey(i), utils.RandomValue(i)); err != nil {
				t.Errorf("Put() error = %v", err)
				return
			}
		}

		if err = w.Commit(); err != ErrExceedMaxBatchSize {
			t.Errorf("Commit() error = %v, wantErr %v", err, ErrExceedMaxBatchSize)
		}

		if db.index.Size() != 0 {
			t.Errorf("index size should be 0, got %v", db.index.Size())
		}
	})
}

func TestWriteBatch_Delete(t *testing.T) {
	type fields struct {
		options          Options
		writeBatchOption WriteBatchOption
		shouldPutValue   bool
		putValue         []byte
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
			name: "put nil and delete",
			fields: fields{
				options:          defaultOptions(),
				writeBatchOption: defaultWriteBatchOption(),
				shouldPutValue:   true,
				putValue:         nil,
			},
			args: args{
				key: []byte("test"),
			},
			wantErr: false,
		},
		{
			name: "put value and delete",
			fields: fields{
				options:          defaultOptions(),
				writeBatchOption: defaultWriteBatchOption(),
				shouldPutValue:   true,
				putValue:         utils.RandomValue(10),
			},
			args: args{
				key: []byte("test"),
			},
			wantErr: false,
		},
		{
			name: "delete not exist key",
			fields: fields{
				options:          defaultOptions(),
				writeBatchOption: defaultWriteBatchOption(),
				shouldPutValue:   false,
			},
			args: args{
				key: []byte("test"),
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
			w := db.NewWriteBatch(tt.fields.writeBatchOption)

			if tt.fields.shouldPutValue {
				err = w.Put(tt.args.key, tt.fields.putValue)
				if (err != nil) != tt.wantErr {
					t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}

			if err = w.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}

			if _, existed := w.pendingWrites[string(tt.args.key)]; existed != tt.wantErr {
				t.Errorf("Delete() key existed, but wantErr %v", tt.wantErr)
			}
		})
	}
}

func TestWriteBatch_Put(t *testing.T) {
	type fields struct {
		options          Options
		writeBatchOption WriteBatchOption
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
			name: "put nil value",
			fields: fields{
				options:          defaultOptions(),
				writeBatchOption: defaultWriteBatchOption(),
			},
			args: args{
				key:   []byte("test"),
				value: nil,
			},
			wantErr: false,
		},
		{
			name: "put value",
			fields: fields{
				options:          defaultOptions(),
				writeBatchOption: defaultWriteBatchOption(),
			},
			args: args{
				key:   []byte("test"),
				value: utils.RandomValue(10),
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
			w := db.NewWriteBatch(tt.fields.writeBatchOption)
			if err = w.Put(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}

			if _, existed := w.pendingWrites[string(tt.args.key)]; !existed {
				t.Errorf("Put() key not existed")
			}
			// 判断是否写入成功
			_, err = db.Get(tt.args.key)
			if err != ErrKeyNotFound {
				t.Errorf("Get() error = %v, wantErr %v", err, ErrKeyNotFound)
				return
			}
		})
	}
}

func Test_logRecordKeyWithSeq(t *testing.T) {
	type args struct {
		key   []byte
		seqId uint64
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "number key",
			args: args{
				key:   []byte("123"),
				seqId: 1,
			},
			want: []byte("\u0001123"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := logRecordKeyWithSeq(tt.args.key, tt.args.seqId); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("logRecordKeyWithSeq() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_parsedLogRecordKey(t *testing.T) {
	type args struct {
		key []byte
	}
	tests := []struct {
		name  string
		args  args
		want  []byte
		want1 uint64
	}{
		{
			name: "number key",
			args: args{
				key: []byte("\u0001123"),
			},
			want:  []byte("123"),
			want1: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := parsedLogRecordKey(tt.args.key)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parsedLogRecordKey() got = %v, want %v", string(got), string(tt.want))
			}
			if got1 != tt.want1 {
				t.Errorf("parsedLogRecordKey() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
