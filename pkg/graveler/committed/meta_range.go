package committed

//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -source=meta_range.go -destination=mock/meta_range.go -package=mock

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

// Iterator iterates over all Range headers and values of a MetaRange, allowing seeking by entire
// ranges.
type Iterator interface {
	// Next moves to look at the next value in the current Range, or a header for the next
	// Range if the current Range is over.
	Next() bool
	// NextRange skips the current Range and continues at the header for the next Range.
	NextRange() bool
	// Value returns a nil ValueRecord and a Range before starting a Range, or a Value and
	// that Range when inside a Range.
	Value() (*graveler.ValueRecord, *Range)
	SeekGE(id graveler.Key)
	Err() error
	Close()
}

// DiffIterator iterates over all Range headers and values of a Diff, allowing seeking by entire
// ranges.
// DiffIterator might contain ranges without headers
// for example:
//
// left        [min].R1.[max]                     [min].R3.[max]        [min]...............R5..............[max]
//
//	------------------------------------------------------------------------------------------------
//
// right                        [min].R2.[max]     [min.....R4....max]  [min].R6.[max] [min].R7.[max]
//
// R1 -  will return as diff with header
// R2 - will return as diff with header
// R3 and R4 - could not return a header because we must enter the ranges in order to get some header values (such as count)
// R5 and R6 - same as R3 and R4
// R7 - in case R5 has no values in the R7 range, R7 would return as a diff with header
type DiffIterator interface {
	// Next moves to look at the next value in the current Range, or a header for the next Range if the current Range is over and a next range exists.
	Next() bool
	// NextRange skips the current range
	// If the next Range is a "headerless" range it will return the first value, otherwise will return the header
	// calling NextRange from a "headerless" should result with ErrNoRange
	NextRange() bool
	// Value returns a nil ValueRecord and a Range before starting a Range, a Value and that Range when inside a Range, or a value with no range when inside a headerless Range
	Value() (*graveler.Diff, *RangeDiff)
	SeekGE(id graveler.Key)
	Err() error
	Close()
}

// RangeDiff represents a change in Range
type RangeDiff struct {
	Type         graveler.DiffType
	Range        *Range
	LeftIdentity ID // the Identity of the value on the left side of the diff (populated on DiffTypeChanged )
}

func (r RangeDiff) Copy() *RangeDiff {
	res := RangeDiff{
		Type: r.Type,
	}
	if r.Range != nil {
		res.Range = r.Range.Copy()
	}
	return &res
}

// MetaRangeManager is an abstraction for a repository of MetaRanges that exposes operations on them
type MetaRangeManager interface {
	Exists(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID) (bool, error)

	// GetValue returns the matching in-range graveler.ValueRecord for key in the
	// MetaRange with id.
	GetValue(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID, key graveler.Key) (*graveler.ValueRecord, error)

	// NewWriter returns a writer that is used for creating new MetaRanges
	NewWriter(ctx context.Context, ns graveler.StorageNamespace, metadata graveler.Metadata) MetaRangeWriter

	// NewMetaRangeIterator returns an Iterator over the MetaRange with id.
	NewMetaRangeIterator(ctx context.Context, ns graveler.StorageNamespace, metaRangeID graveler.MetaRangeID) (Iterator, error)

	// GetMetaRangeURI returns a URI with an object representing metarange ID.  It may
	// return a URI that does not resolve (rather than an error) if ID does not exist.
	GetMetaRangeURI(ctx context.Context, ns graveler.StorageNamespace, metaRangeID graveler.MetaRangeID) (string, error)

	// GetRangeURI returns a URI with an object representing range ID.  It may
	// return a URI that does not resolve (rather than an error) if ID does not exist.
	GetRangeURI(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.RangeID) (string, error)

	// GetRangeByKey returns the Range that contains key in the MetaRange with id.
	GetRangeByKey(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID, key graveler.Key) (*Range, error)
}

// MetaRangeWriter is an abstraction for creating new MetaRanges
type MetaRangeWriter interface {
	// WriteRecord adds a record to the MetaRange. The key must be greater than any other key that was written
	// (in other words - values must be entered sorted by key order).
	// If the most recent insertion was using WriteRange, the key must be greater than any key in the added ranges.
	WriteRecord(graveler.ValueRecord) error

	// WriteRange adds a complete range to the MetaRange at the current insertion point.
	// Added Range must not contain keys smaller than last previously written value.
	WriteRange(Range) error

	// Close finalizes the MetaRange creation. It's invalid to add records after calling this method.
	// During MetaRange writing, ranges are closed asynchronously and copied by tierFS
	// while writing continues. Close waits until closing and copying all ranges.
	Close() (*graveler.MetaRangeID, error)

	Abort() error
}
