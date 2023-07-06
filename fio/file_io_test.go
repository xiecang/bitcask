package fio

import (
	"os"
	"testing"
)

func TestFileIo_Close(t *testing.T) {
	type fields struct {
		filename string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "test close",
			fields: fields{
				filename: "test",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fio, err := NewFileManager(tt.fields.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFileManager error = %v, wantErr %v", err, tt.wantErr)
			}
			_, err = os.Stat(tt.fields.filename)
			if os.IsNotExist(err) {
				t.Errorf("NewFileManager error = %v, wantErr %v", err, tt.wantErr)
			}
			if err = fio.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
			// delete test file
			_ = os.Remove(tt.fields.filename)
		})
	}
}

func TestFileIo_Read(t *testing.T) {
	type fields struct {
		filename string
	}
	type args struct {
		b      []byte
		offset int64
	}
	type pre struct {
		data []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		pre     pre
		want    int
		wantErr bool
	}{
		{
			name: "test read",
			fields: fields{
				filename: "test",
			},
			args: args{
				b:      []byte("test"),
				offset: 0,
			},
			pre: pre{
				data: []byte("testtest"),
			},
			want:    4,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fio, err := NewFileManager(tt.fields.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFileManager error = %v, wantErr %v", err, tt.wantErr)
			}

			// write test data
			_, err = fio.Write(tt.pre.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			var got int
			got, err = fio.Read(tt.args.b, tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
			// delete test file
			_ = os.Remove(tt.fields.filename)
		})
	}
}

func TestFileIo_Sync(t *testing.T) {
	type fields struct {
		filename string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "test sync",
			fields: fields{
				filename: "test",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fio, err := NewFileManager(tt.fields.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFileManager error = %v, wantErr %v", err, tt.wantErr)
			}

			if err = fio.Sync(); (err != nil) != tt.wantErr {
				t.Errorf("Sync() error = %v, wantErr %v", err, tt.wantErr)
			}

			// delete test file
			_ = os.Remove(tt.fields.filename)
		})
	}
}

func TestFileIo_Write(t *testing.T) {
	type fields struct {
		filename string
	}
	type args struct {
		b []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "test write",
			fields: fields{
				filename: "test",
			},
			args: args{
				b: []byte("test"),
			},
			want:    4,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fio, err := NewFileManager(tt.fields.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFileManager error = %v, wantErr %v", err, tt.wantErr)
			}
			var got int
			got, err = fio.Write(tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Write() got = %v, want %v", got, tt.want)
			}
			// delete test file
			_ = os.Remove(tt.fields.filename)
		})
	}
}
