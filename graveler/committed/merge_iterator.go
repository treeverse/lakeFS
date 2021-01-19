package committed

import (
	"github.com/treeverse/lakefs/graveler"
)

type mergeIterator struct {
	*compareIterator
	val *graveler.ValueRecord
	err error
}

// NewMergeIterator accepts an iterator describing a diff from theirs to ours.
// It returns a ValueIterator with the changes to perform on theirs, in order to merge ours into it,
// relative to base as the merge base.
// The iterator will return ErrConflictFound when it reaches a conflict.
func NewMergeIterator(diffTheirsToOurs graveler.DiffIterator, base Iterator) (*mergeIterator, error) {
	return &mergeIterator{compareIterator: NewCompareIterator(diffTheirsToOurs, base)}, nil
}

func (d *mergeIterator) Next() bool {
	if d.err != nil {
		return false
	}
	if !d.compareIterator.Next() {
		return false
	}
	compareVal := d.compareIterator.Value()
	switch compareVal.Type() {
	case graveler.DiffTypeConflict:
		d.err = graveler.ErrConflictFound
		d.val = nil
		return false
	case graveler.DiffTypeRemoved:
		d.val = &graveler.ValueRecord{Key: d.compareIterator.Value().Key().Copy()}
		return true
	default:
		d.val = &graveler.ValueRecord{
			Key:   d.compareIterator.Value().Key().Copy(),
			Value: d.compareIterator.Value().Value(),
		}
		return true
	}
}

func (d *mergeIterator) Value() *graveler.ValueRecord {
	return d.val
}

func (d *mergeIterator) Err() error {
	return d.err
}

func (d *mergeIterator) SeekGE(id graveler.Key) {
	d.compareIterator.SeekGE(id)
	d.err = nil
	d.val = nil
}

func (d *mergeIterator) Close() {
	d.compareIterator.Close()
	d.err = nil
	d.val = nil
}
