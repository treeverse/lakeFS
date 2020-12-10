package sstable

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/graveler"

	"github.com/cockroachdb/pebble"

	"github.com/treeverse/lakefs/pyramid"

	"github.com/cockroachdb/pebble/sstable"
)

type PebbleSSTableManager struct {
	fs    pyramid.FS
	cache *pebble.Cache
}

const sstableTierFSNamespace = "sstables"

func NewPebbleSSTableManager() Manager {
	return &PebbleSSTableManager{}
}

var (
	// ErrPathNotFound is the error returned when the path is not found
	ErrPathNotFound = errors.New("path not found")
)

// GetEntry returns the entry matching the path in the SSTable referenced by the id.
// If path not found, (nil, ErrPathNotFound) is returned.
func (m *PebbleSSTableManager) GetValue(lookup graveler.Key, tid ID) (*graveler.Value, error) {
	reader, err := m.getReader(tid)
	if err != nil {
		return nil, err
	}

	it, err := reader.NewIter(lookup, nil)
	defer it.Close()
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}

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

func (m *PebbleSSTableManager) getReader(tid ID) (*sstable.Reader, error) {
	f, err := m.fs.Open(sstableTierFSNamespace, string(tid))
	if err != nil {
		return nil, fmt.Errorf("open sstable %s: %w", tid, err)
	}

	return sstable.NewReader(f, sstable.ReaderOptions{Cache: m.cache})
}

// SSTableIterator takes a given SSTable and returns an EntryIterator seeked to >= "from" path
func (m *PebbleSSTableManager) NewSSTableIterator(tid ID, from graveler.Key) (graveler.ValueIterator, error) {
	reader, err := m.getReader(tid)
	if err != nil {
		return nil, err
	}

	iter, err := reader.NewIter(from, nil)
	if err != nil {
		return nil, fmt.Errorf("creating sstable iterator: %w", err)
	}

	return &Iterator{it: iter}, nil
}

// GetWriter returns a new SSTable writer instance
func (m *PebbleSSTableManager) GetWriter() (Writer, error) {
	return newDiskWriter(m.fs, sha256.New())
}
