package committed

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type compareValueIterator struct {
	*compareIterator
}

// NewCompareValueIterator Wraps CompareIterator in order to return only values
func NewCompareValueIterator(ctx context.Context, diffDestToSource DiffIterator, base Iterator) *compareValueIterator {
	return &compareValueIterator{
		NewCompareIterator(ctx, diffDestToSource, base),
	}
}

func (d *compareValueIterator) Next() bool {
	for d.compareIterator.Next() {
		val, _ := d.compareIterator.Value()
		if val != nil {
			return true
		}
	}
	return false
}

func (d *compareValueIterator) Value() *graveler.Diff {
	val, _ := d.compareIterator.Value()
	return val
}
