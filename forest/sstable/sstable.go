package sstable

import (
	"github.com/treeverse/lakefs/catalog/rocks"
)

// SSTableID is an identifier for an SSTable
type SSTableID string

type Manager interface {
	// GetEntry returns the entry matching the path in the SSTable referenced by the id.
	// If path not found, (nil, ErrPathNotFound) is returned.
	GetEntry(path rocks.Key, tid SSTableID) (*rocks.Value, error)

	// NewSSTableIterator takes a given SSTable and returns an Iterator seeked to >= "from" path
	NewSSTableIterator(tid SSTableID, from rocks.Key) (rocks.Iterator, error)

	// GetWriter returns a new SSTable writer instance
	GetWriter() (Writer, error)
}

// WriteResult is the result of a completed write of an SSTable
type WriteResult struct {
	// SSTableID is the identifier for the written SSTable.
	// Calculated by an hash function to all paths and entries.
	SSTableID SSTableID

	// First is the Key of the first entry in the SSTable.
	First rocks.Key

	// Last is the Key of the last entry in the SSTable.
	Last rocks.Key

	// Count is the number of entries in the SSTable.
	Count int
}

// Writer is an abstraction for writing SSTables.
// Written entries must be sorted by path.
type Writer interface {
	// WriteEntry appends the given entry to the SSTable
	WriteEntry(entry rocks.ValueRecord) error

	// Close flushes all entries to the disk and returns the WriteResult.
	Close() (*WriteResult, error)
}

// BatchWriterCloser collects sstable writers and handles the asynchronous
// flushing and closing of the writers.
// Example usage:
// func batch(manager Manager, bwc BatchWriterCloser) {
//	w1, _ := manager.GetWriter()
//	_ = w1.WriteEntry(rocks.ValueRecord{Key: "foo1", Value: &rocks.Value{Address: "bar1"}})
//	_ = w1.WriteEntry(rocks.ValueRecord{Key: "foo2", Value: &rocks.Value{Address: "bar2"}})
//	_ = bwc.CloseWriterAsync(w1)

//	w2, _ := manager.GetWriter()
//	_ = w2.WriteEntry(rocks.ValueRecord{Key: "goo1", Value: &rocks.Value{Address: "baz1"}})
//	_ = bwc.CloseWriterAsync(w2)

//	// blocks until all writers finished or any writer failed
//	res, err := bwc.Wait()
//	// handle err, results, etc..
// }
type BatchWriterCloser interface {
	// CloseWriterAsync adds Writer instance for the BatchWriterCloser to handle.
	// Any writes executed to the writer after this call are not guaranteed to succeed.
	// If Wait() has already been called, returns an error.
	CloseWriterAsync(Writer) error

	// Wait returns when all Writers finished.
	// Any failure to close a single Writer will return with a nil results slice and an error.
	Wait() ([]WriteResult, error)
}
