package sstable

import (
	"errors"
	"io"

	"github.com/treeverse/lakefs/rocks3"
)

// SSTableID is an identifier for an SSTable
type SSTableID string

var (
	// ErrPathNotFound is the error returned when the path is not found
	ErrPathNotFound = errors.New("path not found")
)

type Manager interface {
	// GetEntry returns the entry matching the path in the SSTable referenced by the id.
	// If path not found, (nil, ErrPathNotFound) is returned.
	GetEntry(path rocks3.Path, tid SSTableID) (*rocks3.Entry, error)

	// ListEntries takes a given SSTable and returns an EntryIterator seeked to >= "from" path
	ListEntries(tid SSTableID, from rocks3.Path) (EntryIterator, error)

	// GetWriter returns a new SSTable writer instance
	GetWriter() (Writer, error)
}

// EntryIterator returns ordered iteration of the SSTable entries
type EntryIterator interface {
	// SeekGE advances the iterator to point to the given path.
	// Returns true iff if the iterator is pointing at a valid entry.
	SeekGE(rocks3.Path) bool

	// Next advances the iterator.
	// Returns true iff if the iterator is pointing at a valid entry.
	Next() bool

	// Value returns the last read path and entry.
	// Must return non-nil results after Next() or SeekGE(path) returned true.
	Value() (*rocks3.Path, *rocks3.Entry)

	// Error returns any accumulated error.
	Error() error

	io.Closer
}

// WriteResult is the result of a completed write of an SSTable
type WriteResult struct {
	// SSTableID is the identifier for the written SSTable.
	// Calculated by an hash function to all paths and entries.
	SSTableID SSTableID

	// First is the Path of the first entry in the SSTable.
	First rocks3.Path

	// Last is the Path of the last entry in the SSTable.
	Last rocks3.Path

	// Count is the number of entries in the SSTable.
	Count int
}

// Writer is an abstraction for writing SSTables.
// Written entries must be sorted by path.
type Writer interface {
	// WriteEntry appends the given entry to the SSTable
	WriteEntry(path rocks3.Path, entry rocks3.Entry) error

	// Close flushes all entries to the disk and returns the WriteResult.
	Close() (*WriteResult, error)
}
