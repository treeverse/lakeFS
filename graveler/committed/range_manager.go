package committed

import (
	"context"
	"errors"
)

//go:generate mockgen -source=range_manager.go -destination=mock/range_manager.go -package=mock

// ID is an identifier for a Range
type ID string

// Namespace is namespace for ID ranges
type Namespace string

// Key and Value types for to be stored in any Range of the MetaRange
type Key []byte

func (k Key) Copy() Key {
	c := make([]byte, len(k))
	copy(c, k)
	return c
}

type Value []byte
type Record struct {
	Key   Key
	Value Value
}

type ValueIterator interface {
	Next() bool
	SeekGE(id Key)
	Value() *Record
	Err() error
	Close()
}

var (
	ErrNotFound = errors.New("not found")
)

type RangeManager interface {
	// Exists returns true if id references a Range.
	Exists(ctx context.Context, ns Namespace, id ID) (bool, error)

	// GetValue returns the value matching key in the Range referenced by id.  If id not
	// found, it return (nil, ErrNotFound).
	GetValue(ctx context.Context, ns Namespace, id ID, key Key) (*Record, error)

	// GetValueGE returns the first value keyed at or after key in the Range referenced by
	// id.  If all values are keyed before key, it returns (nil, ErrNotFound).
	GetValueGE(ctx context.Context, ns Namespace, id ID, key Key) (*Record, error)

	// NewRangeIterator returns an iterator over values in the Range with ID.
	NewRangeIterator(ctx context.Context, ns Namespace, pid ID) (ValueIterator, error)

	// GetWriter returns a new Range writer instance
	GetWriter(ctx context.Context, ns Namespace) (RangeWriter, error)
}

// WriteResult is the result of a completed write of a Range
type WriteResult struct {
	// ID is the identifier for the written Range.
	// Calculated by an hash function to all keys and values' identity.
	RangeID ID

	// First is the first key in the Range.
	First Key

	// Last is the last key in the Range.
	Last Key

	// Count is the number of records in the Range.
	Count int

	// EstimatedRangeSizeBytes is Approximate size of each Range
	EstimatedRangeSizeBytes uint64
}

// RangeWriter is an abstraction for writing Ranges.
// Written records must be sorted by key.
type RangeWriter interface {
	// WriteRecord appends the given record to the Range.
	WriteRecord(record Record) error

	// AddMetadata associates metadata value (which will be stringified when the writer is
	// Closed) with key.
	AddMetadata(key, value string)

	// GetApproximateSize returns an estimate of the current written size of the Range.
	GetApproximateSize() uint64

	// Close flushes all records to the disk and returns the WriteResult.
	Close() (*WriteResult, error)

	// Abort terminates the non-closed file and removes all traces.
	Abort() error
}
