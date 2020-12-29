package tree

import (
	"bytes"

	"github.com/treeverse/lakefs/graveler"
)

type IteratorValue struct {
	record *graveler.ValueRecord
	part   *Part
}

type diffIterator struct {
	left          Iterator
	right         Iterator
	leftVal       *IteratorValue
	rightVal      *IteratorValue
	needLeftNext  bool
	nextRightNext bool
	currentVal    *graveler.Diff
	err           error
}

const (
	done = iota
	sameParts
	sameIdentities
	sameKeys
	needStartPartLeft
	needStartPartRight
	leftBeforeRight
	rightBeforeLeft
)

func NewDiffIterator(left Iterator, right Iterator) graveler.DiffIterator {
	return &diffIterator{
		left:          left,
		right:         right,
		leftVal:       &IteratorValue{},
		rightVal:      &IteratorValue{},
		needLeftNext:  true,
		nextRightNext: true,
	}
}

func (d *diffIterator) next(it Iterator, val **IteratorValue) {
	if it.Next() {
		(*val).record, (*val).part = it.Value()
		return
	}
	*val = nil
	if it.Err() != nil {
		d.err = it.Err()
	}

}

func (d *diffIterator) nextPart(it Iterator, val **IteratorValue) {
	if it.NextPart() {
		(*val).record, (*val).part = it.Value()
		return
	}
	*val = nil
	if it.Err() != nil {
		d.err = it.Err()
	}
}

func key(it Iterator) []byte {
	val, part := it.Value()
	if val == nil {
		return part.MinKey
	}
	return val.Key
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

func (d *diffIterator) getState() int {
	comp := d.compareKeys()
	switch {
	case d.leftVal == nil && d.rightVal == nil:
		return done
	case d.leftVal != nil && d.rightVal != nil && d.leftVal.part.ID == d.rightVal.part.ID:
		return sameParts
	case d.leftVal != nil && d.leftVal.record == nil && comp <= 0:
		return needStartPartLeft
	case d.rightVal != nil && d.rightVal.record == nil && comp >= 0:
		return needStartPartRight
	case comp == 0 && bytes.Equal(d.leftVal.record.Identity, d.rightVal.record.Identity):
		return sameIdentities
	case comp == 0:
		return sameKeys
	case comp < 0:
		return leftBeforeRight
	default:
		return rightBeforeLeft
	}
}

func (d *diffIterator) Next() bool {
	for {
		if d.err != nil {
			return false
		}
		if d.needLeftNext {
			d.needLeftNext = false
			d.next(d.left, &d.leftVal)
		}
		if d.nextRightNext {
			d.nextRightNext = false
			d.next(d.right, &d.rightVal)
		}
		switch d.getState() {
		case done:
			d.currentVal = nil
			return false
		case sameParts:
			d.nextPart(d.left, &d.leftVal)
			d.nextPart(d.right, &d.rightVal)
			continue
		case sameKeys:
			// same keys on different parts
			d.needLeftNext = true
			d.nextRightNext = true
			d.currentVal = diff(d.rightVal, graveler.DiffTypeChanged)
			return true
		case sameIdentities:
			d.needLeftNext = true
			d.nextRightNext = true
			continue
		case needStartPartLeft:
			d.needLeftNext = true
		case needStartPartRight:
			d.nextRightNext = true
		case leftBeforeRight:
			// nothing on right, or left before right
			d.needLeftNext = true
			d.currentVal = diff(d.leftVal, graveler.DiffTypeRemoved)
			return true
		case rightBeforeLeft:
			// nothing on left, or right before left
			d.nextRightNext = true
			d.currentVal = diff(d.rightVal, graveler.DiffTypeAdded)
			return true
		}
	}
}

func diff(val *IteratorValue, typ graveler.DiffType) *graveler.Diff {
	return &graveler.Diff{
		Type:  typ,
		Key:   val.record.Key,
		Value: val.record.Value,
	}
}

func (d *diffIterator) SeekGE(id graveler.Key) {
	d.left.SeekGE(id)
	d.right.SeekGE(id)
	d.currentVal = nil
	d.leftVal = &IteratorValue{}
	d.rightVal = &IteratorValue{}
	d.err = nil
	d.nextRightNext = true
	d.needLeftNext = true
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
