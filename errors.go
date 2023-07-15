package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("key not found")
	ErrFileNotFound           = errors.New("file not found")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrExceedMaxBatchSize     = errors.New("exceed max batch size")
	ErrMergeInProgress        = errors.New("merge in progress")
)
