package tree

import (
	"bytes"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
)

type IteratorValue struct {
	record *graveler.ValueRecord
	part   *Part
}

type diffIterator struct {
	left       Iterator
	right      Iterator
	leftVal    *IteratorValue
	rightVal   *IteratorValue
	currentVal *graveler.Diff
	err        error
}

func NewDiffIterator(left Iterator, right Iterator) graveler.DiffIterator {
	return &diffIterator{left: left, right: right, leftVal: &IteratorValue{}, rightVal: &IteratorValue{}}
}

func (d *diffIterator) next(it Iterator, val **IteratorValue) {
	if !it.Next() {
		*val = nil
		return
	}
	(*val).record, (*val).part = it.Value()
}

func (d *diffIterator) nextPart(it Iterator, val **IteratorValue) {
	if !it.NextPart() {
		*val = nil
		return
	}
	(*val).record, (*val).part = it.Value()
}

func key(it Iterator) graveler.Key {
	val, part := it.Value()
	if val == nil {
		return part.MinKey
	}
	return val.Key
}

func part(it Iterator) *Part {
	_, part := it.Value()
	return part
}

func (d *diffIterator) compareKeys() int {
	if d.leftVal == nil {
		return 1
	}
	if d.rightVal == nil {
		return -1
	}
	return bytes.Compare(key(d.left), key(d.right))
}

func (d *diffIterator) Next() bool {
	if d.err != nil {
		return false
	}
	advanceLeft, advanceRight := true, true
	for {
		if d.left.Err() != nil {
			d.err = fmt.Errorf("failed in left tree: %w", d.left.Err())
			return false
		}
		if d.right.Err() != nil {
			d.err = fmt.Errorf("failed in right tree: %w", d.right.Err())
			return false
		}
		if advanceLeft {
			advanceLeft = false
			d.nextPart(d.left, &d.leftVal)
		}
		if advanceRight {
			advanceRight = false
			d.nextPart(d.right, &d.rightVal)
		}
		comp := d.compareKeys()
		switch {
		case d.leftVal == nil && d.rightVal == nil:
			return false
		case d.leftVal != nil && d.rightVal != nil && d.leftVal.part.Name == d.rightVal.part.Name:
			// skip identical parts
			advanceLeft = true
			advanceRight = true
			continue
		case d.leftVal != nil && d.rightVal != nil && d.leftVal.record != nil && d.rightVal.record != nil && bytes.Equal(key(d.left), key(d.right)):
			// same keys on different parts
			advanceLeft = true
			advanceRight = true
			if bytes.Equal(d.leftVal.record.Identity, d.rightVal.record.Identity) {
				continue
			}
			d.currentVal = &graveler.Diff{
				Type:  graveler.DiffTypeChanged,
				Key:   d.rightVal.record.Key,
				Value: d.rightVal.record.Value,
			}
			return true
		case comp < 0:
			// nothing on right, or left before right
			advanceLeft = true
			if d.leftVal.record == nil {
				continue
			}
			d.currentVal = &graveler.Diff{
				Type:  graveler.DiffTypeRemoved,
				Key:   d.leftVal.record.Key,
				Value: d.leftVal.record.Value,
			}
			return true
		case comp > 0:
			// nothing on left, or right before left
			advanceRight = true
			if d.rightVal.record == nil {
				continue
			}
			d.currentVal = &graveler.Diff{
				Type:  graveler.DiffTypeAdded,
				Key:   d.rightVal.record.Key,
				Value: d.rightVal.record.Value,
			}
			return true
		case d.leftVal.record == nil:
			advanceLeft = true
		case d.rightVal.record == nil:
			advanceRight = true
		}
	}
	d.currentVal = nil
	return false
}

func (d *diffIterator) SeekGE(id graveler.Key) {
	d.left.SeekGE(id)
	d.right.SeekGE(id)
	d.currentVal = nil
	d.next(d.left, &d.leftVal)
	d.next(d.right, &d.rightVal)
	d.err = nil
}

func (d *diffIterator) Value() *graveler.Diff {
	return d.currentVal
}

func (d *diffIterator) Err() error {
	return d.err
}

func (d *diffIterator) Close() {
	d.left.Close()
	d.right.Close()
}
