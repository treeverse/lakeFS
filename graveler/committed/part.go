package committed

// ID is an identifier for a part
type ID string

// Namespace is namespace for ID parts
type Namespace string

// Key and Value types for to be stored in any part of the tree
type Key []byte
type Value []byte
type Record struct {
	Key
	Value
}

type ValueIterator interface {
	Next() bool
	SeekGE(id Key)
	Value() *Record
	Err() error
	Close()
}

type PartManager interface {
	// GetValue returns the value matching the key in the part referenced by the id.
	// If path not found, (nil, ErrPathNotFound) is returned.
	GetValue(ns Namespace, key Key, pid ID) (*Record, error)

	// NewPartIterator takes a part ID and returns an ValueIterator seeked to >= "from" value
	NewPartIterator(ns Namespace, pid ID, from Key) (ValueIterator, error)

	// GetWriter returns a new part writer instance
	GetWriter(ns Namespace) (Writer, error)
}

// WriteResult is the result of a completed write of an part
type WriteResult struct {
	// ID is the identifier for the written part.
	// Calculated by an hash function to all keys and values' identity.
	PartID ID

	// First is the first key in the part.
	First Key

	// Last is the last key in the part.
	Last Key

	// Count is the number of records in the part.
	Count int
}

// Writer is an abstraction for writing Parts.
// Written records must be sorted by key.
type Writer interface {
	// WriteRecord appends the given record to the part
	WriteRecord(record Record) error

	// Close flushes all records to the disk and returns the WriteResult.
	Close() (*WriteResult, error)
}

// BatchWriterCloser collects part writers and handles the asynchronous
// flushing and closing of the writers.
// Example usage:
// func batch(manager PartManager, bwc BatchWriterCloser) {
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
