package fio

import (
	"os"
	"testing"
)

func destroyFile(filename string) {
	err := os.Remove(filename)
	if err != nil {
		panic(err)
	}
}

func writeSomeData(filename string, data []byte) error {
	f, err := NewFileManager(filename)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return err
}
func TestMMap_Close(t *testing.T) {
	type fields struct {
		filename string
		data     []byte
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				filename: "test",
				data:     []byte("test"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := writeSomeData(tt.fields.filename, tt.fields.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeSomeData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			m, err := NewMMapIOManager(tt.fields.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMMapIOManager() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer destroyFile(tt.fields.filename)
			if err = m.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

}

func TestMMap_Read(t *testing.T) {
	type fields struct {
		filename  string
		data      []byte
		writeData bool
	}
	type args struct {
		bytes []byte
		i     int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				filename:  "test",
				data:      []byte("test"),
				writeData: true,
			},
			args: args{
				bytes: make([]byte, 4),
				i:     0,
			},
			want:    4,
			wantErr: false,
		},
		{
			name: "file not exist/empty",
			fields: fields{
				filename: "test",
			},
			args: args{
				bytes: make([]byte, 4),
				i:     0,
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.fields.writeData {
				err := writeSomeData(tt.fields.filename, tt.fields.data)
				if (err != nil) != tt.wantErr {
					t.Errorf("writeSomeData() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}
			m, err := NewMMapIOManager(tt.fields.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMMapIOManager() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer destroyFile(tt.fields.filename)
			got, err := m.Read(tt.args.bytes, tt.args.i)
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMMap_Size(t *testing.T) {
	type fields struct {
		filename  string
		data      []byte
		writeData bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    int64
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				filename:  "test",
				data:      []byte("test"),
				writeData: true,
			},
			want:    4,
			wantErr: false,
		},
		{
			name: "file not exist/empty",
			fields: fields{
				filename: "test",
			},
			want:    0,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.fields.writeData {
				err := writeSomeData(tt.fields.filename, tt.fields.data)
				if (err != nil) != tt.wantErr {
					t.Errorf("writeSomeData() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}
			m, err := NewMMapIOManager(tt.fields.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMMapIOManager() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer destroyFile(tt.fields.filename)
			got, err := m.Size()
			if (err != nil) != tt.wantErr {
				t.Errorf("Size() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Size() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewMMapIOManager(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal",
			args: args{
				filename: "test",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewMMapIOManager(tt.args.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMMapIOManager() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer destroyFile(tt.args.filename)
		})
	}
}
