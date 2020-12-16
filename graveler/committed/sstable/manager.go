package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"hash"

	"github.com/treeverse/lakefs/graveler/committed"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/pyramid"

	"github.com/treeverse/lakefs/graveler"
)

type Manager struct {
	cache      cache
	fs         pyramid.FS
	logger     logging.Logger
	hash       hash.Hash
	serializer serializer
}

func NewPebbleSSTableManager(cache cache, fs pyramid.FS, serializer serializer, hash hash.Hash) committed.PartManager {
	return &Manager{cache: cache, fs: fs, serializer: serializer, hash: hash}
}

var (
	// ErrPathNotFound is the error returned when the path is not found
	ErrPathNotFound = errors.New("path not found")
)

// GetEntry returns the entry matching the path in the SSTable referenced by the id.
// If path not found, (nil, ErrPathNotFound) is returned.
func (m *Manager) GetValue(ns committed.Namespace, lookup graveler.Key, tid committed.ID) (*graveler.Value, error) {
	reader, derefer, err := m.cache.GetOrOpen(string(ns), tid)
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
	key, val := it.SeekGE(lookup)
	if key == nil {
		// checking if an error occurred or key simply not found
		if it.Error() != nil {
			return nil, fmt.Errorf("reading key from sstable id %s: %w", tid, it.Error())
		}

		// lookup path is bigger than the last path in the SSTable
		return nil, ErrPathNotFound
	}

	if !bytes.Equal(lookup, key.UserKey) {
		// lookup path in range but key not found
		return nil, ErrPathNotFound
	}

	return m.serializer.DeserializeValue(val)
}

// SSTableIterator takes a given SSTable and returns an EntryIterator seeked to >= "from" path
func (m *Manager) NewPartIterator(ns committed.Namespace, tid committed.ID, from graveler.Key) (graveler.ValueIterator, error) {
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

	return NewIterator(iter, m.serializer, derefer, from), nil
}

// GetWriter returns a new SSTable writer instance
func (m *Manager) GetWriter(ns committed.Namespace) (committed.Writer, error) {
	return newDiskWriter(m.fs, ns, m.hash, m.serializer)
}

func (m *Manager) execAndLog(f func() error, msg string) {
	if err := f(); err != nil {
		m.logger.WithError(err).Error(msg)
	}
}
