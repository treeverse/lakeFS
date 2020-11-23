package sstable

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"

	"github.com/cockroachdb/pebble/sstable"

	"github.com/google/uuid"
)

type diskSSTableManager struct {
	localDiskSSTablesDir string
	cache                *pebble.Cache

	// maximum size in bytes for the localDiskSSTablesDir
	dirMaxSize int64
}

func (disk *diskSSTableManager) newSSTableWriter() (Writer, error) {
	return newDiskWriter(disk.localDiskSSTablesDir)
}

func (disk *diskSSTableManager) loadReader(tid SSTableID) (*sstable.Reader, error) {
	localPath := sstableLocalPath(disk.localDiskSSTablesDir, tid)
	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		// TODO: read from storage adaptor to the local path
	}

	fh, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}
	reader, err := sstable.NewReader(fh, sstable.ReaderOptions{Cache: disk.cache})
	if err != nil {
		fh.Close()
		return nil, err
	}

	return reader, nil
}

func sstableLocalPath(dir string, tid SSTableID) string {
	return fmt.Sprintf("%s/%s.sst", dir, tid)
}

func sstableLocalTempPath(dir string) string {
	uuid := uuid.New().String()
	return fmt.Sprintf("%s/temp_%s.sst", dir, uuid)
}
