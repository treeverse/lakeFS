package sstable

import (
	"github.com/treeverse/lakefs/graveler"
)

// ID is an identifier for an SSTable
type ID string

type Manager interface {
	// GetValue returns the value matching the key in the SSTable referenced by the id.
	// If path not found, (nil, ErrPathNotFound) is returned.
	GetValue(key graveler.Key, tid ID) (*graveler.Value, error)

	// NewSSTableIterator takes a given SSTable and returns an ValueIterator seeked to >= "from" value
	NewSSTableIterator(tid ID, from graveler.Key) (graveler.ValueIterator, error)

	// GetWriter returns a new SSTable writer instance
	GetWriter() (Writer, error)
}

// WriteResult is the result of a completed write of an SSTable
type WriteResult struct {
	// ID is the identifier for the written SSTable.
	// Calculated by an hash function to all keys and values' identity.
	SSTableID ID

	// First is the first key in the SSTable.
	First graveler.Key

	// Last is the last key in the SSTable.
	Last graveler.Key

	// Count is the number of records in the SSTable.
	Count int
}

// Writer is an abstraction for writing SSTables.
// Written records must be sorted by key.
type Writer interface {
	// WriteRecord appends the given record to the SSTable
	WriteRecord(record graveler.ValueRecord) error

	// Close flushes all records to the disk and returns the WriteResult.
	Close() (*WriteResult, error)
}

// BatchWriterCloser collects sstable writers and handles the asynchronous
// flushing and closing of the writers.
// Example usage:
// func batch(manager Manager, bwc BatchWriterCloser) {
//	w1, _ := manager.GetWriter()
//	_ = w1.WriteRecord(graveler.ValueRecord{Key: "foo1", Value: &graveler.Value{Address: "bar1"}})
//	_ = w1.WriteRecord(graveler.ValueRecord{Key: "foo2", Value: &graveler.Value{Address: "bar2"}})
//	_ = bwc.CloseWriterAsync(w1)

//	w2, _ := manager.GetWriter()
//	_ = w2.WriteRecord(graveler.ValueRecord{Key: "goo1", Value: &graveler.Value{Address: "baz1"}})
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
