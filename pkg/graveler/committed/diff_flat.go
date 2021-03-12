package committed

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

// flatDiffIterator wraps a diffIterator in order to return only values
type flatDiffIterator struct {
	rangeDiffIter DiffIterator
}

func NewFlatDiffIterator(ctx context.Context, left Iterator, right Iterator) graveler.DiffIterator {
	return &flatDiffIterator{
		rangeDiffIter: NewDiffIterator(ctx, left, right),
	}
}

func (d flatDiffIterator) Next() bool {
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

func (d flatDiffIterator) SeekGE(id graveler.Key) {
	d.rangeDiffIter.SeekGE(id)
}

func (d flatDiffIterator) Value() *graveler.Diff {
	val, _ := d.rangeDiffIter.Value()
	return val
}

func (d flatDiffIterator) Err() error {
	return d.rangeDiffIter.Err()
}

func (d flatDiffIterator) Close() {
	d.rangeDiffIter.Close()
}
