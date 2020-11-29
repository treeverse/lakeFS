package sstable

import (
	"github.com/treeverse/lakefs/catalog/rocks"
)

// SSTableID is an identifier for an SSTable
type SSTableID string

type Manager interface {
	// GetEntry returns the entry matching the path in the SSTable referenced by the id.
	// If path not found, (nil, ErrPathNotFound) is returned.
	GetEntry(path rocks.Path, tid SSTableID) (*rocks.Entry, error)

	// SSTableIterator takes a given SSTable and returns an EntryIterator seeked to >= "from" path
	SSTableIterator(tid SSTableID, from rocks.Path) (rocks.EntryIterator, error)

	// GetWriter returns a new SSTable writer instance
	GetWriter() (Writer, error)
}

// WriteResult is the result of a completed write of an SSTable
type WriteResult struct {
	// SSTableID is the identifier for the written SSTable.
	// Calculated by an hash function to all paths and entries.
	SSTableID SSTableID

	// First is the Path of the first entry in the SSTable.
	First rocks.Path

	// Last is the Path of the last entry in the SSTable.
	Last rocks.Path

	// Count is the number of entries in the SSTable.
	Count int
}

// Writer is an abstraction for writing SSTables.
// Written entries must be sorted by path.
type Writer interface {
	// WriteEntry appends the given entry to the SSTable
	WriteEntry(path rocks.Path, entry rocks.Entry) error

	// Close flushes all entries to the disk and returns the WriteResult.
	Close() error

	// Result returns the sstable statistics for the table.
	// If table isn't closed yet and there are more pending writes, result is bound to change.
	Result() WriteResult
}
