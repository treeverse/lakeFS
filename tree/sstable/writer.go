package sstable

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"os"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/rocks3"
)

type DiskWriter struct {
	w *sstable.Writer

	first  rocks3.Path
	last   rocks3.Path
	count  int
	sha256 hash.Hash

	tablesDirPath string
	tempPath      string
}

func newDiskWriter(tablesDirPath string) (*DiskWriter, error) {
	tempPath := sstableLocalTempPath(tablesDirPath)
	fh, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}

	writer := sstable.NewWriter(fh, sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
	})

	return &DiskWriter{
		w:             writer,
		sha256:        sha256.New(),
		tablesDirPath: tablesDirPath,
		tempPath:      tempPath,
	}, nil
}

func (dw *DiskWriter) WriteEntry(path rocks3.Path, entry rocks3.Entry) error {
	pathBytes := []byte(path)
	entryBytes, err := serializeEntry(entry)
	if err != nil {
		return fmt.Errorf("serializing entry: %w", err)
	}
	if err := dw.w.Set(pathBytes, entryBytes); err != nil {
		return fmt.Errorf("setting key and value: %w", err)
	}

	// updating stats
	if dw.count == 0 {
		dw.first = path
	}
	dw.last = path
	dw.count++

	dw.sha256.Sum(pathBytes)
	dw.sha256.Sum(entryBytes)

	return nil
}

func (dw *DiskWriter) Close() (*WriteResult, error) {
	if err := dw.w.Close(); err != nil {
		return nil, fmt.Errorf("sstable file write: %w", err)
	}

	sstableID := SSTableID(fmt.Sprintf("%x", dw.sha256.Sum(nil)))
	if err := os.Rename(dw.tempPath, sstableLocalPath(dw.tablesDirPath, sstableID)); err != nil {
		return nil, fmt.Errorf("temp file rename: %w", err)
	}

	// TODO: check if this sstable already exists
	// TODO: write to s3
	return &WriteResult{
		SSTableID: sstableID,
		First:     dw.first,
		Last:      dw.last,
		Count:     dw.count,
	}, nil
}
