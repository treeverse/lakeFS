package sstable

import (
	"fmt"
	"hash"

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
	fh       *pyramid.File
}

func newDiskWriter(tierFS pyramid.FS, hash hash.Hash) (*DiskWriter, error) {
	fh, err := tierFS.Create(sstableTierFSNamespace)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}

	writer := sstable.NewWriter(fh, sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
	})

	return &DiskWriter{
		w:      writer,
		fh:     fh,
		tierFS: tierFS,
		hash:   hash,
	}, nil
}

func (dw *DiskWriter) WriteEntry(entry rocks.EntryRecord) error {
	pathBytes := []byte(entry.Path)
	entryBytes, err := serializeEntry(*entry.Entry)
	if err != nil {
		return fmt.Errorf("serializing entry: %w", err)
	}
	if err := dw.w.Set(pathBytes, entryBytes); err != nil {
		return fmt.Errorf("setting key and value: %w", err)
	}

	// updating stats
	if dw.count == 0 {
		dw.first = entry.Path
	}
	dw.last = entry.Path
	dw.count++

	if _, err := dw.hash.Write(pathBytes); err != nil {
		return err
	}
	if _, err := dw.hash.Write(entryBytes); err != nil {
		return err
	}

	return nil
}

func (dw *DiskWriter) Close() (*WriteResult, error) {
	if err := dw.w.Close(); err != nil {
		return nil, fmt.Errorf("sstable file write: %w", err)
	}

	sstableID := fmt.Sprintf("%x", dw.hash.Sum(nil))
	if err := dw.fh.Store(sstableID); err != nil {
		return nil, fmt.Errorf("error storing sstable: %w", err)
	}

	return &WriteResult{
		SSTableID: ID(sstableID),
		First:     dw.first,
		Last:      dw.last,
		Count:     dw.count,
	}, nil
}
