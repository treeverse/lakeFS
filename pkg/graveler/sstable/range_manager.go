package sstable

import (
	"bytes"
	"context"
	"crypto"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/pyramid"
)

type NewSSTableReaderFn func(ctx context.Context, ns committed.Namespace, id committed.ID) (*sstable.Reader, error)

type Unrefer interface {
	Unref()
}

type RangeManager struct {
	newReader NewSSTableReaderFn
	fs        pyramid.FS
	hash      crypto.Hash
	cache     Unrefer
}

func NewPebbleSSTableRangeManager(cache *pebble.Cache, fs pyramid.FS, hash crypto.Hash) *RangeManager {
	if cache != nil { // nil cache allowed (size=0), see sstable.ReaderOptions
		cache.Ref()
	}
	opts := sstable.ReaderOptions{Cache: cache}
	newReader := func(ctx context.Context, ns committed.Namespace, id committed.ID) (*sstable.Reader, error) {
		return newReader(ctx, fs, ns, id, opts)
	}
	return NewPebbleSSTableRangeManagerWithNewReader(newReader, opts.Cache, fs, hash)
}

func newReader(ctx context.Context, fs pyramid.FS, ns committed.Namespace, id committed.ID, opts sstable.ReaderOptions) (*sstable.Reader, error) {
	file, err := fs.Open(ctx, string(ns), string(id))
	if err != nil {
		return nil, fmt.Errorf("open sstable file %s %s: %w", ns, id, err)
	}
	r, err := sstable.NewReader(file, opts)
	if err != nil {
		return nil, fmt.Errorf("open sstable reader %s %s: %w", ns, id, err)
	}
	return r, nil
}

func NewPebbleSSTableRangeManagerWithNewReader(newReader NewSSTableReaderFn, cache Unrefer, fs pyramid.FS, hash crypto.Hash) *RangeManager {
	return &RangeManager{
		fs:        fs,
		hash:      hash,
		newReader: newReader,
		cache:     cache,
	}
}

var (
	// ErrKeyNotFound is the error returned when a path is not found
	ErrKeyNotFound = fmt.Errorf("key: %w", committed.ErrNotFound)

	_ committed.RangeManager = &RangeManager{}
)

func (m *RangeManager) Exists(ctx context.Context, ns committed.Namespace, id committed.ID) (bool, error) {
	return m.fs.Exists(ctx, string(ns), string(id))
}

func (m *RangeManager) GetValueGE(ctx context.Context, ns committed.Namespace, id committed.ID, lookup committed.Key) (*committed.Record, error) {
	reader, err := m.newReader(ctx, ns, id)
	if err != nil {
		return nil, err
	}
	defer m.execAndLog(ctx, reader.Close, "close reader")

	// TODO(ariels): reader.NewIter(lookup, lookup)?
	it, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer m.execAndLog(ctx, it.Close, "close iterator")

	// Ranges are keyed by MaxKey, seek to the range that might contain key.
	key, value := it.SeekGE(lookup, sstable.SeekGEFlags(0))
	if key == nil {
		if it.Error() != nil {
			return nil, fmt.Errorf("read metarange from sstable id %s: %w", id, it.Error())
		}
		return nil, ErrKeyNotFound
	}
	vBytes, err := retrieveValue(value)

	if err != nil {
		return nil, fmt.Errorf("extract value from sstable id %s (key %s): %w", id, key, err)
	}
	return &committed.Record{
		Key:   key.UserKey,
		Value: vBytes,
	}, nil
}

// GetValue returns the Record matching the key in the SSTable referenced by the id.
// If key is not found, (nil, ErrKeyNotFound) is returned.
func (m *RangeManager) GetValue(ctx context.Context, ns committed.Namespace, id committed.ID, lookup committed.Key) (*committed.Record, error) {
	reader, err := m.newReader(ctx, ns, id)
	if err != nil {
		return nil, err
	}
	defer m.execAndLog(ctx, reader.Close, "close reader")

	it, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer m.execAndLog(ctx, it.Close, "close iterator")

	// actual reading
	key, value := it.SeekGE(lookup, sstable.SeekGEFlags(0))
	if key == nil {
		if it.Error() != nil {
			return nil, fmt.Errorf("read key from sstable id %s: %w", id, it.Error())
		}

		// lookup path is after the last path in the SSTable
		return nil, ErrKeyNotFound
	}

	if !bytes.Equal(lookup, key.UserKey) {
		// lookup path in range but key not found
		return nil, ErrKeyNotFound
	}
	vBytes, err := retrieveValue(value)

	if err != nil {
		return nil, fmt.Errorf("extract value from sstable id %s (key %s): %w", id, key, err)
	}

	return &committed.Record{
		Key:   key.UserKey,
		Value: vBytes,
	}, nil
}

// NewRangeIterator takes a given SSTable and returns an EntryIterator seeked to >= "from" path
func (m *RangeManager) NewRangeIterator(ctx context.Context, ns committed.Namespace, tid committed.ID) (committed.ValueIterator, error) {
	reader, err := m.newReader(ctx, ns, tid)
	if err != nil {
		return nil, err
	}

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		if e := reader.Close(); e != nil {
			logging.FromContext(ctx).WithError(e).Errorf("Failed de-referencing sstable %s", tid)
		}
		return nil, fmt.Errorf("creating sstable iterator: %w", err)
	}

	return NewIterator(iter, reader.Close), nil
}

// GetWriter returns a new SSTable writer instance
func (m *RangeManager) GetWriter(ctx context.Context, ns committed.Namespace, metadata graveler.Metadata) (committed.RangeWriter, error) {
	return NewDiskWriter(ctx, m.fs, ns, m.hash.New(), metadata)
}

func (m *RangeManager) GetURI(ctx context.Context, ns committed.Namespace, id committed.ID) (string, error) {
	return m.fs.GetRemoteURI(ctx, string(ns), string(id))
}

func (m *RangeManager) execAndLog(ctx context.Context, f func() error, msg string) {
	if err := f(); err != nil {
		logging.FromContext(ctx).WithError(err).Error(msg)
	}
}

func (m *RangeManager) Close() error {
	m.cache.Unref()
	return nil
}
