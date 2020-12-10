package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"hash"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/pyramid"

	"github.com/treeverse/lakefs/graveler"
)

type PebbleSSTableManager struct {
	cache  cache
	fs     pyramid.FS
	logger logging.Logger
	hash   hash.Hash
}

const sstableTierFSNamespace = "sstables"

func NewPebbleSSTableManager(cache cache, fs pyramid.FS) Manager {
	return &PebbleSSTableManager{cache: cache, fs: fs}
}

var (
	// ErrPathNotFound is the error returned when the path is not found
	ErrPathNotFound = errors.New("path not found")
)

// GetEntry returns the entry matching the path in the SSTable referenced by the id.
// If path not found, (nil, ErrPathNotFound) is returned.
func (m *PebbleSSTableManager) GetValue(ns Namespace, lookup graveler.Key, tid ID) (*graveler.Value, error) {
	reader, derefer, err := m.cache.GetOrOpen(string(ns), tid)
	if err != nil {
		return nil, err
	}
	defer m.execAndLog(derefer, "Failed to dereference reader")

	it, err := reader.NewIter(lookup, nil)
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer m.execAndLog(it.Close, "Failed to close iterator")

	// actual reading
	key, val := it.Next()
	if key == nil {
		// checking if an error occurred or key simply not found
		if it.Error() != nil {
			return nil, fmt.Errorf("reading key: %w", it.Error())
		}

		// lookup path is bigger than the last path in the SSTable
		return nil, ErrPathNotFound
	}

	if !bytes.Equal(lookup, key.UserKey) {
		// lookup path in range but key not found
		return nil, ErrPathNotFound
	}

	return deserializeValue(val)
}

// SSTableIterator takes a given SSTable and returns an EntryIterator seeked to >= "from" path
func (m *PebbleSSTableManager) NewSSTableIterator(ns Namespace, tid ID, from graveler.Key) (graveler.ValueIterator, error) {
	reader, derefer, err := m.cache.GetOrOpen(string(ns), tid)
	if err != nil {
		return nil, err
	}

	iter, err := reader.NewIter(from, nil)
	if err != nil {
		if e := derefer(); e != nil {
			m.logger.WithError(e).Errorf("Failed de-referencing sstable %s", tid)
		}
		return nil, fmt.Errorf("creating sstable iterator: %w", err)
	}

	return &Iterator{it: iter, derefer: derefer}, nil
}

// GetWriter returns a new SSTable writer instance
func (m *PebbleSSTableManager) GetWriter() (Writer, error) {
	return newDiskWriter(m.fs, m.hash)
}

func (m *PebbleSSTableManager) execAndLog(f func() error, msg string) {
	if err := f(); err != nil {
		m.logger.WithError(err).Error(msg)
	}
}
