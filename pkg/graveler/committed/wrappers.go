package committed

import "github.com/treeverse/lakefs/pkg/graveler"

func NewDiffIteratorWrapper(iter graveler.DiffIterator) DiffIterator {
	return &DiffIteratorWrapper{DiffIterator: iter}
}

type DiffIteratorWrapper struct {
	graveler.DiffIterator
	err error
}

func (d *DiffIteratorWrapper) NextRange() bool {
	d.err = ErrNoRange
	return false
}

func (d *DiffIteratorWrapper) Value() (*graveler.Diff, *RangeDiff) {
	return d.DiffIterator.Value(), nil
}

func (d *DiffIteratorWrapper) Err() error {
	if d.err != nil {
		return d.err
	}
	return d.DiffIterator.Err()
}

func (d *DiffIteratorWrapper) Close() {
	d.DiffIterator.Close()
}
