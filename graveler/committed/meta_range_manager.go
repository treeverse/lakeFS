package committed

//go:generate mockgen -source=meta_range_manager.go -destination=mock/meta_range_manager.go -package=mock

import (
	"github.com/treeverse/lakefs/graveler"
)

// MetaRange is a sorted slice of ranges with no overlapping between the ranges
type MetaRange struct {
	ID     graveler.RangeID
	Ranges []Range
}

// Iterator iterates over all Range headers and values of a MetaRange, allowing seeking by entire
// ranges.
type Iterator interface {
	// Next moves to look at the next value in the current Range, or a header for the next
	// Range if the current Range is over.
	Next() bool
	// NextRange() skips over the entire remainder of the current Range and continues at the
	// header for the next Range.
	NextRange() bool
	// Value returns a nil ValueRecord and a Range before starting a Range, or a Value and
	// that Range when inside a Range.
	Value() (*graveler.ValueRecord, *Range)
	SeekGE(id graveler.Key)
	Err() error
	Close()
}

// MetaRangeManager is an abstraction for a repository of MetaRanges that exposes operations on them
type MetaRangeManager interface {
	GetMetaRange(ns graveler.StorageNamespace, rangeID graveler.RangeID) (*MetaRange, error)

	// GetValue finds the matching graveler.ValueRecord in the MetaRange with the rangeID
	GetValue(ns graveler.StorageNamespace, rangeID graveler.RangeID, key graveler.Key) (*graveler.ValueRecord, error)

	// NewRangeWriter returns a writer that is used for creating new MetaRanges
	NewWriter(ns graveler.StorageNamespace) MetaRangeWriter

	// NewMetaRangeIterator accepts a MetaRange ID, and returns an Iterator
	// over the MetaRange from the first value >= from
	NewMetaRangeIterator(ns graveler.StorageNamespace, rangeID graveler.RangeID, from graveler.Key) (Iterator, error)

	// NewRangeIterator accepts a Range ID, and returns a ValueIterator
	// over this Range from the first value >= from
	NewRangeIterator(ns graveler.StorageNamespace, rangeID graveler.RangeID, from graveler.Key) (graveler.ValueIterator, error)
}

// MetaRangeWriter is an abstraction for creating new MetaRanges
type MetaRangeWriter interface {
	// WriteRecord adds a record to the MetaRange. The key must be greater than any other key that was written
	// (in other words - values must be entered sorted by key order).
	// If the most recent insertion was using WriteRange, the key must be greater than any key in the added ranges.
	WriteRecord(graveler.ValueRecord) error

	// AddRange adds a complete range to the MetaRange at the current insertion point.
	// Added Range must not contain keys smaller than last previously written value.
	WriteRange(Range) error

	// Close finalizes the MetaRange creation. It's invalid to add records after calling this method.
	// During MetaRange writing, ranges are closed asynchronously and copied by tierFS
	// while writing continues. Close waits until closing and copying all ranges.
	Close() (*graveler.RangeID, error)

	Abort() error
}
