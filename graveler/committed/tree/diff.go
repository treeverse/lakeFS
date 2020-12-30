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
	diffItBeforeInit diffIteratorState = iota
	diffItInitialized
	diffItClosed
)

type diffIteratorCompareResult int

const (
	diffItCompareResultDone diffIteratorCompareResult = iota
	diffItCompareResultSameParts
	diffItCompareResultSameIdentities
	diffItCompareResultSameKeys
	diffItCompareResultNeedStartPartBoth
	diffItCompareResultNeedStartPartLeft
	diffItCompareResultNeedStartPartRight
	diffItCompareResultLeftBeforeRight
	diffItCompareResultRightBeforeLeft
)

func NewDiffIterator(left Iterator, right Iterator) graveler.DiffIterator {
	return &diffIterator{
		left:     left,
		right:    right,
		leftVal:  &IteratorValue{},
		rightVal: &IteratorValue{},
		state:    diffItBeforeInit,
	}
}

func (d *diffIterator) diffIteratorNextValue(it Iterator) (*graveler.ValueRecord, *Part, error) {
	if d.err != nil {
		return nil, nil, d.err
	}
	if it.Next() {
		rec, part := it.Value()
		return rec, part, nil
	}
	return nil, nil, it.Err()
}

func (d *diffIterator) diffIteratorNextPart(it Iterator) (*Part, error) {
	if d.err != nil {
		return nil, d.err
	}
	if it.NextPart() {
		_, part := it.Value()
		return part, nil
	}
	return nil, it.Err()
}

func (d *diffIterator) compareDiffKeys() int {
	if d.leftVal.part == nil {
		return 1
	}
	if d.rightVal.part == nil {
		return -1
	}
	return bytes.Compare(getCurrentKey(d.left), getCurrentKey(d.right))
}

func (d *diffIterator) compareDiffIterators() diffIteratorCompareResult {
	if d.state == diffItBeforeInit {
		return -1
	}
	if d.leftVal.part == nil && d.rightVal.part == nil {
		return diffItCompareResultDone
	}
	if d.leftVal.part != nil && d.rightVal.part != nil && d.leftVal.part.ID == d.rightVal.part.ID {
		return diffItCompareResultSameParts
	}
	leftStartPart := d.leftVal.part != nil && d.leftVal.record == nil
	rightStartPart := d.rightVal.part != nil && d.rightVal.record == nil
	comp := d.compareDiffKeys()
	switch {
	case leftStartPart && rightStartPart && comp == 0:
		return diffItCompareResultNeedStartPartBoth
	case leftStartPart && comp <= 0:
		return diffItCompareResultNeedStartPartLeft
	case rightStartPart && comp >= 0:
		return diffItCompareResultNeedStartPartRight
	case comp == 0 && bytes.Equal(d.leftVal.record.Identity, d.rightVal.record.Identity):
		return diffItCompareResultSameIdentities
	case comp == 0:
		return diffItCompareResultSameKeys
	case comp < 0:
		return diffItCompareResultLeftBeforeRight
	default:
		return diffItCompareResultRightBeforeLeft
	}
}

func (d *diffIterator) Next() bool {
	if d.state == diffItClosed {
		return false
	}
	if d.state == diffItBeforeInit {
		d.state = diffItInitialized
		d.leftVal.record, d.leftVal.part, d.err = d.diffIteratorNextValue(d.left)
		d.rightVal.record, d.rightVal.part, d.err = d.diffIteratorNextValue(d.right)
	}
	for {
		if d.err != nil {
			return false
		}
		compareResult := d.compareDiffIterators()
		switch compareResult {
		case diffItCompareResultDone:
			d.currentVal = nil
			return false
		case diffItCompareResultSameParts:
			d.leftVal.part, d.err = d.diffIteratorNextPart(d.left)
			d.rightVal.part, d.err = d.diffIteratorNextPart(d.right)
		case diffItCompareResultSameKeys:
			// same keys on different parts
			d.currentVal = d.rightVal.asDiff(graveler.DiffTypeChanged)
			d.leftVal.record, d.leftVal.part, d.err = d.diffIteratorNextValue(d.left)
			d.rightVal.record, d.rightVal.part, d.err = d.diffIteratorNextValue(d.right)
			return true
		case diffItCompareResultSameIdentities, diffItCompareResultNeedStartPartBoth:
			d.leftVal.record, d.leftVal.part, d.err = d.diffIteratorNextValue(d.left)
			d.rightVal.record, d.rightVal.part, d.err = d.diffIteratorNextValue(d.right)
		case diffItCompareResultNeedStartPartLeft:
			d.leftVal.record, d.leftVal.part, d.err = d.diffIteratorNextValue(d.left)
		case diffItCompareResultNeedStartPartRight:
			d.rightVal.record, d.rightVal.part, d.err = d.diffIteratorNextValue(d.right)
		case diffItCompareResultLeftBeforeRight:
			// nothing on right, or left before right
			d.currentVal = d.leftVal.asDiff(graveler.DiffTypeRemoved)
			d.leftVal.record, d.leftVal.part, d.err = d.diffIteratorNextValue(d.left)
			return true
		case diffItCompareResultRightBeforeLeft:
			// nothing on left, or right before left
			d.currentVal = d.rightVal.asDiff(graveler.DiffTypeAdded)
			d.rightVal.record, d.rightVal.part, d.err = d.diffIteratorNextValue(d.right)
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
	d.state = diffItBeforeInit
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
	d.state = diffItClosed
}

func getCurrentKey(it Iterator) []byte {
	val, part := it.Value()
	if val == nil {
		return part.MinKey
	}
	return val.Key
}
