package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("key is empty")
	ErrIndexUpdateFailed = errors.New("failed to update index")
	ErrKeyNotFound       = errors.New("key not found")
	ErrFileNotFound      = errors.New("file not found")
)