package sstable

import (
	"bytes"
	"crypto"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/pyramid"
)

type Manager struct {
	cache Cache
	fs    pyramid.FS
	// TODO(ariels): Replace with loggers constructed from context.
	logger logging.Logger
	hash   crypto.Hash
}

func NewPebbleSSTableRangeManager(cache Cache, fs pyramid.FS, hash crypto.Hash) *Manager {
	return &Manager{cache: cache, logger: logging.Default(), fs: fs, hash: hash}
}

var (
	// ErrKeyNotFound is the error returned when a path is not found
	ErrKeyNotFound = errors.New("path not found")

	_ committed.RangeManager = &Manager{}
)

func (m *Manager) Exists(ns committed.Namespace, id committed.ID) (bool, error) {
	return m.cache.Exists(string(ns), id)
}

func (m *Manager) GetValueGE(ns committed.Namespace, id committed.ID, lookup committed.Key) (*committed.Record, error) {
	reader, derefer, err := m.cache.GetOrOpen(string(ns), id)
	if err != nil {
		return nil, err
	}
	defer m.execAndLog(derefer, "Failed to dereference reader")

	it, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer m.execAndLog(it.Close, "Failed to close iterator")

	// Ranges are keyed by MaxKey, seek to the range that might contain key.
	key, value := it.SeekGE(lookup)
	if key == nil {
		if it.Error() != nil {
			return nil, fmt.Errorf("read metarange from sstable id %s: %w", id, it.Error())
		}
		return nil, ErrKeyNotFound
	}

	return &committed.Record{
		Key:   key.UserKey,
		Value: value,
	}, nil
}

// GetEntry returns the entry matching the path in the SSTable referenced by the id.
// If path not found, (nil, ErrPathNotFound) is returned.
func (m *Manager) GetValue(ns committed.Namespace, id committed.ID, lookup committed.Key) (*committed.Record, error) {
	reader, derefer, err := m.cache.GetOrOpen(string(ns), id)
	if err != nil {
		return nil, err
	}
	defer m.execAndLog(derefer, "Failed to dereference reader")

	it, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer m.execAndLog(it.Close, "Failed to close iterator")

	// actual reading
	key, value := it.SeekGE(lookup)
	if key == nil {
		// checking if an error occurred or key simply not found
		if it.Error() != nil {
			return nil, fmt.Errorf("reading key from sstable id %s: %w", id, it.Error())
		}

		// lookup path is bigger than the last path in the SSTable
		return nil, ErrKeyNotFound
	}

	if !bytes.Equal(lookup, key.UserKey) {
		// lookup path in range but key not found
		return nil, ErrKeyNotFound
	}

	return &committed.Record{
		Key:   key.UserKey,
		Value: value,
	}, nil
}

// NewRangeIterator takes a given SSTable and returns an EntryIterator seeked to >= "from" path
func (m *Manager) NewRangeIterator(ns committed.Namespace, tid committed.ID) (committed.ValueIterator, error) {
	reader, derefer, err := m.cache.GetOrOpen(string(ns), tid)
	if err != nil {
		return nil, err
	}

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		if e := derefer(); e != nil {
			m.logger.WithError(e).Errorf("Failed de-referencing sstable %s", tid)
		}
		return nil, fmt.Errorf("creating sstable iterator: %w", err)
	}

	return NewIterator(iter, derefer), nil
}

// GetWriter returns a new SSTable writer instance
func (m *Manager) GetWriter(ns committed.Namespace) (committed.RangeWriter, error) {
	return NewDiskWriter(m.fs, ns, m.hash.New())
}

func (m *Manager) execAndLog(f func() error, msg string) {
	if err := f(); err != nil {
		m.logger.WithError(err).Error(msg)
	}
}
