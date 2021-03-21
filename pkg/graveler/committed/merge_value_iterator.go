package committed

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type compareValueIterator struct {
	iter *compareIterator
}

// NewCompareValueIterator Wraps CompareIterator in order to return only values
func NewCompareValueIterator(ctx context.Context, diffDestToSource DiffIterator, base Iterator) *compareValueIterator {
	return &compareValueIterator{
		iter: NewCompareIterator(ctx, diffDestToSource, base),
	}
}

func (d *compareValueIterator) Next() bool {
	for d.iter.Next() {
		val, _ := d.iter.Value()
		if val != nil {
			return true
		}
	}
	return false
}

func (d *compareValueIterator) SeekGE(id graveler.Key) {
	d.iter.SeekGE(id)
}

func (d *compareValueIterator) Value() *graveler.Diff {
	val, _ := d.iter.Value()
	return val
}

func (d *compareValueIterator) Err() error {
	return d.iter.Err()
}

func (d *compareValueIterator) Close() {
	d.iter.Close()
}
