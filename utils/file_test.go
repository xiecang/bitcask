package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var testDir = filepath.Join(os.TempDir(), "test")

func createTestDir() {
	err := os.Mkdir(testDir, os.ModePerm)
	if err != nil {
		panic(err)
	}
}

func writeData(filename string, data []byte) {
	fio, err := os.OpenFile(filepath.Join(testDir, filename), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(fmt.Errorf("OpenFile() error = %v", err))
	}
	_, err = fio.Write(data)
	if err != nil {
		panic(fmt.Errorf("write() error = %v", err))
	}
}

func destroyTestDir() {
	err := os.RemoveAll(testDir)
	if err != nil {
		panic(err)
	}
}
func TestDirSize(t *testing.T) {
	type file struct {
		name string
		data []byte
	}
	type filed struct {
		files []file
	}
	type args struct {
		dir string
	}
	tests := []struct {
		name     string
		filed    filed
		args     args
		wantSize int64
		wantErr  bool
	}{
		{
			name: "test1",
			filed: filed{
				files: []file{
					{
						name: "test1",
						data: []byte("test1"),
					},
				},
			},
			args:     args{dir: testDir},
			wantSize: 5,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			createTestDir()
			defer destroyTestDir()

			for _, f := range tt.filed.files {
				writeData(f.name, f.data)
			}

			gotSize, err := DirSize(tt.args.dir)
			if (err != nil) != tt.wantErr {
				t.Errorf("DirSize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotSize != tt.wantSize {
				t.Errorf("DirSize() gotSize = %v, want %v", gotSize, tt.wantSize)
			}
		})
	}
}

func TestAvailableDiskSpace(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "test1",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSize, err := AvailableDiskSpace()
			if (err != nil) != tt.wantErr {
				t.Errorf("AvailableDiskSpace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Logf("AvailableDiskSpace() gotSize = %v", gotSize/1024/1024/1024)
		})
	}
}
