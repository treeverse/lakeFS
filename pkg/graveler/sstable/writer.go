package sstable

import (
	"context"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/fnv"
	"strconv"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/pyramid"
)

const (
	MetadataFirstKey         = "min_key"
	MetadataLastKey          = "max_key"
	MetadataNumRecordsKey    = "count"
	MetadataEstimatedSizeKey = "estimated_size_bytes"
)

type DiskWriter struct {
	ctx    context.Context
	w      *sstable.Writer
	props  map[string]string
	tierFS pyramid.FS
	first  committed.Key
	last   committed.Key
	count  int
	hash   hash.Hash
	fh     pyramid.StoredFile
	closed bool
}

func NewDiskWriter(ctx context.Context, tierFS pyramid.FS, ns committed.Namespace, hash hash.Hash, metadata graveler.Metadata) (*DiskWriter, error) {
	fh, err := tierFS.Create(ctx, string(ns))
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}

	props := make(map[string]string)
	for k, v := range metadata {
		props[k] = v
	}

	writer := sstable.NewWriter(fh, sstable.WriterOptions{
		Compression:             sstable.SnappyCompression,
		TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(props)},
	})

	return &DiskWriter{
		ctx:    ctx,
		w:      writer,
		props:  props,
		fh:     fh,
		tierFS: tierFS,
		hash:   hash,
	}, nil
}

// SetMetadata associates metadata value (which will be stringified) with key.
// Keys and values are also calculated as part of the resulting range ID
func (dw *DiskWriter) SetMetadata(key, value string) {
	dw.props[key] = value
}

func (dw *DiskWriter) GetFS() pyramid.FS {
	return dw.tierFS
}

func (dw *DiskWriter) GetStoredFile() pyramid.StoredFile {
	return dw.fh
}

func (dw *DiskWriter) WriteRecord(record committed.Record) error {
	if err := dw.w.Set(record.Key, record.Value); err != nil {
		return fmt.Errorf("setting key and value: %w", err)
	}

	// updating stats
	if dw.count == 0 {
		dw.first = make(committed.Key, len(record.Key))
		copy(dw.first, record.Key)
	}
	dw.last = make(committed.Key, len(record.Key))
	copy(dw.last, record.Key)
	dw.count++

	if err := dw.writeHashWithLen(record.Key); err != nil {
		return err
	}
	return dw.writeHashWithLen(record.Value)
}

func (dw *DiskWriter) GetApproximateSize() uint64 {
	return dw.w.EstimatedSize()
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

func (dw *DiskWriter) Abort() error {
	if dw.closed {
		return nil
	}

	if err := dw.w.Close(); err != nil {
		return fmt.Errorf("sstable file close: %w", err)
	}

	if err := dw.fh.Abort(dw.ctx); err != nil {
		return fmt.Errorf("sstable file abort: %w", err)
	}
	return nil
}

func (dw *DiskWriter) Close() (*committed.WriteResult, error) {
	// Before closing, we write all user supplied metadata keys and values to the hash
	// This is done to avoid collisions, especially on empty sstables that might hash to the same value otherwise.
	ident.MarshalStringMap(dw.hash, dw.props)

	tableHash := dw.hash.Sum(nil)
	sstableID := hex.EncodeToString(tableHash)

	// Prepare metadata properties for Close to write.  The map was already set in the
	// sstable.Writer constructor and cannot be changed, but we can replace its values
	// before writing it out.
	dw.SetMetadata(MetadataFirstKey, string(dw.first))
	dw.SetMetadata(MetadataLastKey, string(dw.last))
	dw.SetMetadata(MetadataNumRecordsKey, fmt.Sprint(dw.count))
	dw.SetMetadata(MetadataEstimatedSizeKey, fmt.Sprint(dw.w.EstimatedSize()))

	if err := dw.w.Close(); err != nil {
		return nil, fmt.Errorf("sstable close (%s): %w", sstableID, err)
	}

	if err := dw.fh.Store(dw.ctx, sstableID); err != nil {
		return nil, fmt.Errorf("sstable store (%s): %w", sstableID, err)
	}

	dw.closed = true

	return &committed.WriteResult{
		RangeID:                 committed.ID(sstableID),
		First:                   dw.first,
		Last:                    dw.last,
		Count:                   dw.count,
		EstimatedRangeSizeBytes: dw.w.EstimatedSize(),
	}, nil
}

// ShouldBreakAtKey returns true if should break range after the given key
func (dw *DiskWriter) ShouldBreakAtKey(key graveler.Key, params *committed.Params) bool {
	approximateSize := dw.GetApproximateSize()
	if approximateSize < params.MinRangeSizeBytes {
		return false
	}
	if approximateSize >= params.MaxRangeSizeBytes {
		return true
	}

	h := fnv.New64a()
	// FNV always reads all bytes and never fails; ignore its return values
	_, _ = h.Write(key)
	r := h.Sum64() % uint64(params.RangeSizeEntriesRaggedness)
	return r == 0
}
