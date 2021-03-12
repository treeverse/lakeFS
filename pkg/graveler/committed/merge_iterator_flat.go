package committed

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type flatCompareIterator struct {
	iter *compareIterator
}

// NewCompareIteratorFlat Wraps CompareIterator in order to return only values
func NewCompareIteratorFlat(ctx context.Context, diffDestToSource DiffIterator, base Iterator) *flatCompareIterator {
	return &flatCompareIterator{
		iter: NewCompareIterator(ctx, diffDestToSource, base),
	}
}

func (d *flatCompareIterator) Next() bool {
	for d.iter.Next() {
		val, _ := d.iter.Value()
		if val != nil {
			return true
		}
	}
	return false
}

func (d *flatCompareIterator) SeekGE(id graveler.Key) {
	d.iter.SeekGE(id)
}

func (d *flatCompareIterator) Value() *graveler.Diff {
	val, _ := d.iter.Value()
	return val
}

func (d *flatCompareIterator) Err() error {
	return d.iter.Err()
}

func (d *flatCompareIterator) Close() {
	d.iter.Close()
}
