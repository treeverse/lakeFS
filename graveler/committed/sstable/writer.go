package sstable

import (
	"fmt"
	"hash"
	"strconv"

	"github.com/treeverse/lakefs/graveler/committed"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/pyramid"
)

type DiskWriter struct {
	w      *sstable.Writer
	tierFS pyramid.FS

	first committed.Key
	last  committed.Key
	count int
	hash  hash.Hash

	fh pyramid.StoredFile
}

func newDiskWriter(tierFS pyramid.FS, ns committed.Namespace, hash hash.Hash) (*DiskWriter, error) {
	fh, err := tierFS.Create(string(ns))
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

func (dw *DiskWriter) WriteRecord(record committed.Record) error {
	if err := dw.w.Set(record.Key, record.Value); err != nil {
		return fmt.Errorf("setting key and value: %w", err)
	}

	// updating stats
	if dw.count == 0 {
		dw.first = record.Key
	}
	dw.last = record.Key
	dw.count++

	if err := dw.writeHashWithLen(record.Key); err != nil {
		return err
	}
	return dw.writeHashWithLen(record.Value)
}

func (dw *DiskWriter) writeHashWithLen(buf []byte) error {
	if _, err := dw.hash.Write([]byte(strconv.Itoa(len(buf)))); err != nil {
		return err
	}
	if _, err := dw.hash.Write(buf); err != nil {
		return err
	}
	if _, err := dw.hash.Write([]byte("|")); err != nil {
		return err
	}

	return nil
}

func (dw *DiskWriter) Close() (*committed.WriteResult, error) {
	if err := dw.w.Close(); err != nil {
		return nil, fmt.Errorf("sstable file write: %w", err)
	}

	sstableID := fmt.Sprintf("%x", dw.hash.Sum(nil))
	if err := dw.fh.Store(sstableID); err != nil {
		return nil, fmt.Errorf("error storing sstable: %w", err)
	}

	return &committed.WriteResult{
		PartID: committed.ID(sstableID),
		First:  dw.first,
		Last:   dw.last,
		Count:  dw.count,
	}, nil
}
