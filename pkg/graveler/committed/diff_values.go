package committed

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

// diffValuesIterator wraps a diffIterator in order to return only values
type diffValuesIterator struct {
	rangeDiffIter DiffIterator
}

func NewDiffValueIterator(ctx context.Context, left Iterator, right Iterator) graveler.DiffIterator {
	return &diffValuesIterator{
		rangeDiffIter: NewDiffIterator(ctx, left, right),
	}
}

func (d diffValuesIterator) Next() bool {
	for d.rangeDiffIter.Next() {
		if d.rangeDiffIter.Err() != nil {
			return false
		}
		val, _ := d.rangeDiffIter.Value()
		if val != nil {
			return true
		}
	}
	return false
}

func (d diffValuesIterator) SeekGE(id graveler.Key) {
	d.rangeDiffIter.SeekGE(id)
}

func (d diffValuesIterator) Value() *graveler.Diff {
	val, _ := d.rangeDiffIter.Value()
	return val
}

func (d diffValuesIterator) Err() error {
	return d.rangeDiffIter.Err()
}

func (d diffValuesIterator) Close() {
	d.rangeDiffIter.Close()
}
