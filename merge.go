package bitcask_go

import (
	"bitcask-go/data"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

func (db *DB) getMergeFiles() (mergeFiles []*data.File, nonMergeFileId uint32, err error) {
	if db.activeFile == nil {
		// 数据库为空，直接返回
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isMerging {
		// 正在合并中，直接返回
		err = ErrMergeInProgress
		return
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err = db.activeFile.Sync(); err != nil {
		return
	}

	// 将当前活跃文件加入旧文件列表
	db.olderFiles[db.activeFile.Id] = db.activeFile
	// 打开新的活跃文件
	if err = db.setActivateDataFile(); err != nil {
		return
	}

	nonMergeFileId = db.activeFile.Id

	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}

	return
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return path.Join(dir, base+mergeDirName)
}

// Merge 清理无效数据，生成 Hint 文件
func (db *DB) Merge() error {
	var mergeFiles, nonMergeFileId, err = db.getMergeFiles()
	if err != nil {
		return err
	}

	// 将待合并的文件列表按照文件 ID 从小到大排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].Id < mergeFiles[j].Id
	})

	mergePath := db.getMergePath()
	// 如果目录存在，说明上次合并过程中出现了异常，删除目录
	if _, err = os.Stat(mergePath); err == nil {
		if err = os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 创建目录
	if err = os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 打开新的临时 bitcask 实例
	mergeOption := db.options
	mergeOption.DirPath = mergePath
	mergeOption.SyncWrites = false
	mergeDB, err := Open(mergeOption)
	if err != nil {
		return err
	}

	// 打开 hint 文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 将旧文件中的数据写入新的临时 bitcask 实例
	for _, file := range mergeFiles {
		var offset int64 = 0
		for {
			record, size, err := file.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			realKey, _ := parsedLogRecordKey(record.Key)
			pos := db.index.Get(realKey)
			// 和内存索引比较，如果内存索引中存在这个 key，说明这个 key 是有效的
			if pos != nil && pos.Fid == file.Id && pos.Offset == offset {
				// 清除事务标记
				record.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqId)
				p, err := mergeDB.appendLogRecord(record)
				if err != nil {
					return err
				}
				// 将当前位置索引写入 Hint 文件
				if err = hintFile.WriteHintRecord(realKey, p); err != nil {
					return err
				}
			}
			offset += size
		}
	}

	// sync hint file
	if err = hintFile.Sync(); err != nil {
		return err
	}
	if err = mergeDB.Sync(); err != nil {
		return err
	}

	// 写入标识 merge 完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	var finishedRecord = data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	encodedRecord, _ := data.EncodeLogRecord(&finishedRecord)
	if err = mergeFinishedFile.Write(encodedRecord); err != nil {
		return err
	}
	if err = mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	file, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := file.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	id, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}
func (db *DB) loadMergeFiles() error {
	var mergePath = db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return err
	}
	//
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	//
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找 merge 完成的文件
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		// 这里包含了 hint file 和 merge finished file
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 如果没有 merge 完成的文件，说明上次合并过程中出现了异常
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// 删除旧的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		filePath := data.GetFilePath(mergePath, fileId)
		if _, err = os.Stat(filePath); err == nil {
			if err = os.Remove(filePath); err != nil {
				return err
			}
		}
	}
	// 将新的数据文件移动到数据目录
	for _, fileName := range mergeFileNames {
		// 每次合并都会生成一个新的数据文件，文件名为 0000.data 这种格式，id 均是从 0 递增
		// /tmp/bitcask-merge/0000.data  ==>  /tmp/bitcask/0000.data

		srcPath := path.Join(mergePath, fileName)
		dstPath := path.Join(db.options.DirPath, fileName)
		if err = os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) loadIndexFromHintFile() error {
	// 查看是否存在 hint 文件
	var hintFileName = data.GetHintFileName(db.options.DirPath)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// 读取 hint 文件中的索引
	var offset int64 = 0
	for {
		record, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		pos := data.DecodeLogRecordPos(record.Value)
		db.index.Put(record.Key, pos)
		offset += size
	}
	return nil
}
