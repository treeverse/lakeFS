package sstable

import (
	"fmt"
	"hash"
	"os"

	"github.com/treeverse/lakefs/pyramid"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/catalog/rocks"
)

type DiskWriter struct {
	w      *sstable.Writer
	tierFS pyramid.FS

	first rocks.Path
	last  rocks.Path
	count int
	hash  hash.Hash

	tempPath string
}

func newDiskWriter(tierFS pyramid.FS, hash hash.Hash) (*DiskWriter, error) {
	tempPath := getTempPath()
	fh, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}

	writer := sstable.NewWriter(fh, sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
	})

	return &DiskWriter{
		w:        writer,
		tierFS:   tierFS,
		hash:     hash,
		tempPath: tempPath,
	}, nil
}

func getTempPath() string {
	return ""
}

func (dw *DiskWriter) WriteEntry(path rocks.Path, entry rocks.Entry) error {
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

	dw.hash.Sum(pathBytes)
	dw.hash.Sum(entryBytes)

	return nil
}

func (dw *DiskWriter) Close() (*WriteResult, error) {
	if err := dw.w.Close(); err != nil {
		return nil, fmt.Errorf("sstable file write: %w", err)
	}

	sstableID := fmt.Sprintf("%x", dw.hash.Sum(nil))
	if err := dw.tierFS.Store(sstableTierFSNamespace, dw.tempPath, sstableID); err != nil {
		return nil, fmt.Errorf("error storing sstable: %w", err)
	}

	return &WriteResult{
		SSTableID: SSTableID(sstableID),
		First:     dw.first,
		Last:      dw.last,
		Count:     dw.count,
	}, nil
}
