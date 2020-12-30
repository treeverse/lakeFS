package tree

import (
	"bytes"

	"github.com/treeverse/lakefs/graveler"
)

type IteratorValue struct {
	record *graveler.ValueRecord
	part   *Part
}

func (iv *IteratorValue) asDiff(typ graveler.DiffType) *graveler.Diff {
	return &graveler.Diff{
		Type:  typ,
		Key:   iv.record.Key,
		Value: iv.record.Value,
	}
}

type diffIterator struct {
	left       Iterator
	right      Iterator
	leftVal    *IteratorValue
	rightVal   *IteratorValue
	currentVal *graveler.Diff
	err        error
	state      diffIteratorState
}

type diffIteratorState int

const (
	beforeInit diffIteratorState = iota
	afterInit
	done
	closed

	sameParts
	sameIdentities
	sameKeys
	needStartPartBoth
	needStartPartLeft
	needStartPartRight
	leftBeforeRight
	rightBeforeLeft
)

func NewDiffIterator(left Iterator, right Iterator) graveler.DiffIterator {
	return &diffIterator{
		left:     left,
		right:    right,
		leftVal:  &IteratorValue{},
		rightVal: &IteratorValue{},
		state:    beforeInit,
	}
}

func (d *diffIterator) next(it Iterator) (*graveler.ValueRecord, *Part, error) {
	if d.err != nil {
		return nil, nil, d.err
	}
	if it.Next() {
		rec, part := it.Value()
		return rec, part, nil
	}
	return nil, nil, it.Err()
}

func (d *diffIterator) nextPart(it Iterator) (*Part, error) {
	if d.err != nil {
		return nil, d.err
	}
	if it.NextPart() {
		_, part := it.Value()
		return part, nil
	}
	return nil, it.Err()
}

func (d *diffIterator) compareKeys() int {
	if d.leftVal.part == nil {
		return 1
	}
	if d.rightVal.part == nil {
		return -1
	}
	return bytes.Compare(getCurrentKey(d.left), getCurrentKey(d.right))
}

func (d *diffIterator) updateState() {
	if d.state == beforeInit {
		return
	}
	comp := d.compareKeys()
	leftStartPart := d.leftVal.part != nil && d.leftVal.record == nil
	rightStartPart := d.rightVal.part != nil && d.rightVal.record == nil
	switch {
	case d.leftVal.part == nil && d.rightVal.part == nil:
		d.state = done
	case d.leftVal.part != nil && d.rightVal.part != nil && d.leftVal.part.ID == d.rightVal.part.ID:
		d.state = sameParts
	case leftStartPart && rightStartPart && comp == 0:
		d.state = needStartPartBoth
	case leftStartPart && comp <= 0:
		d.state = needStartPartLeft
	case rightStartPart && comp >= 0:
		d.state = needStartPartRight
	case comp == 0 && bytes.Equal(d.leftVal.record.Identity, d.rightVal.record.Identity):
		d.state = sameIdentities
	case comp == 0:
		d.state = sameKeys
	case comp < 0:
		d.state = leftBeforeRight
	default:
		d.state = rightBeforeLeft
	}
}

func (d *diffIterator) Next() bool {
	if d.state == closed {
		return false
	}
	for {
		if d.err != nil {
			return false
		}
		d.updateState()
		switch d.state {
		case done:
			d.currentVal = nil
			return false
		case sameParts:
			d.leftVal.part, d.err = d.nextPart(d.left)
			d.rightVal.part, d.err = d.nextPart(d.right)
		case sameKeys:
			// same keys on different parts
			d.currentVal = d.rightVal.asDiff(graveler.DiffTypeChanged)
			d.leftVal.record, d.leftVal.part, d.err = d.next(d.left)
			d.rightVal.record, d.rightVal.part, d.err = d.next(d.right)
			return true
		case beforeInit:
			d.state = afterInit
			fallthrough
		case sameIdentities, needStartPartBoth:
			d.leftVal.record, d.leftVal.part, d.err = d.next(d.left)
			d.rightVal.record, d.rightVal.part, d.err = d.next(d.right)
		case needStartPartLeft:
			d.leftVal.record, d.leftVal.part, d.err = d.next(d.left)
		case needStartPartRight:
			d.rightVal.record, d.rightVal.part, d.err = d.next(d.right)
		case leftBeforeRight:
			// nothing on right, or left before right
			d.currentVal = d.leftVal.asDiff(graveler.DiffTypeRemoved)
			d.leftVal.record, d.leftVal.part, d.err = d.next(d.left)
			return true
		case rightBeforeLeft:
			// nothing on left, or right before left
			d.currentVal = d.rightVal.asDiff(graveler.DiffTypeAdded)
			d.rightVal.record, d.rightVal.part, d.err = d.next(d.right)
			return true
		}
	}
}

func (d *diffIterator) SeekGE(id graveler.Key) {
	d.left.SeekGE(id)
	d.right.SeekGE(id)
	d.currentVal = nil
	d.leftVal = &IteratorValue{}
	d.rightVal = &IteratorValue{}
	d.err = nil
	d.state = beforeInit
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
	d.currentVal = nil
	d.err = nil
	d.state = closed
}

func getCurrentKey(it Iterator) []byte {
	val, part := it.Value()
	if val == nil {
		return part.MinKey
	}
	return val.Key
}
