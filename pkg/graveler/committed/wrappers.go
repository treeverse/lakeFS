package committed

import "github.com/treeverse/lakefs/pkg/graveler"

func NewIteratorWrapper(iter graveler.ValueIterator) Iterator {
	return &IteratorWrapper{ValueIterator: iter}
}

type IteratorWrapper struct {
	graveler.ValueIterator
	err error
}

func (i *IteratorWrapper) NextRange() bool {
	i.err = ErrNoRange
	return false
}

func (i *IteratorWrapper) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.ValueIterator.Err()
}

func (i *IteratorWrapper) Value() (*graveler.ValueRecord, *Range) {
	return i.ValueIterator.Value(), nil
}

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
