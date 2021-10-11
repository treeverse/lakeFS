package committed

import (
	"errors"

	"github.com/treeverse/lakefs/pkg/graveler"
)

// ErrNoRange occurs when calling nextRange while not in a range, could happen when the diff is currently comparing keys in two different ranges
var ErrNoRange = errors.New("diff is not currently in a range")

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
