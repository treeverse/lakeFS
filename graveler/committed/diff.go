package committed

import (
	"bytes"

	"github.com/treeverse/lakefs/graveler"
)

type IteratorValue struct {
	record *graveler.ValueRecord
	rng    *Range
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
	diffItCompareResultSameRanges
	diffItCompareResultSameIdentities
	diffItCompareResultSameKeys
	diffItCompareResultNeedStartRangeBoth
	diffItCompareResultNeedStartRangeLeft
	diffItCompareResultNeedStartRangeRight
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

func (d *diffIterator) diffIteratorNextValue(it Iterator) (*graveler.ValueRecord, *Range, error) {
	if d.err != nil {
		return nil, nil, d.err
	}
	if it.Next() {
		rec, rng := it.Value()
		return rec, rng, nil
	}
	return nil, nil, it.Err()
}

func (d *diffIterator) diffIteratorNextRange(it Iterator) (*Range, error) {
	if d.err != nil {
		return nil, d.err
	}
	if it.NextRange() {
		_, rng := it.Value()
		return rng, nil
	}
	return nil, it.Err()
}

func (d *diffIterator) compareDiffKeys() int {
	if d.leftVal.rng == nil {
		return 1
	}
	if d.rightVal.rng == nil {
		return -1
	}
	return bytes.Compare(getCurrentKey(d.left), getCurrentKey(d.right))
}

func (d *diffIterator) compareDiffIterators() diffIteratorCompareResult {
	if d.state == diffItBeforeInit {
		return -1
	}
	if d.leftVal.rng == nil && d.rightVal.rng == nil {
		return diffItCompareResultDone
	}
	if d.leftVal.rng != nil && d.rightVal.rng != nil && d.leftVal.rng.ID == d.rightVal.rng.ID {
		return diffItCompareResultSameRanges
	}
	leftStartRange := d.leftVal.rng != nil && d.leftVal.record == nil
	rightStartRange := d.rightVal.rng != nil && d.rightVal.record == nil
	comp := d.compareDiffKeys()
	switch {
	case leftStartRange && rightStartRange && comp == 0:
		return diffItCompareResultNeedStartRangeBoth
	case leftStartRange && comp <= 0:
		return diffItCompareResultNeedStartRangeLeft
	case rightStartRange && comp >= 0:
		return diffItCompareResultNeedStartRangeRight
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
		d.leftVal.record, d.leftVal.rng, d.err = d.diffIteratorNextValue(d.left)
		d.rightVal.record, d.rightVal.rng, d.err = d.diffIteratorNextValue(d.right)
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
		case diffItCompareResultSameRanges:
			d.leftVal.rng, d.err = d.diffIteratorNextRange(d.left)
			d.rightVal.rng, d.err = d.diffIteratorNextRange(d.right)
		case diffItCompareResultSameKeys:
			// same keys on different ranges
			d.currentVal = d.rightVal.asDiff(graveler.DiffTypeChanged)
			d.leftVal.record, d.leftVal.rng, d.err = d.diffIteratorNextValue(d.left)
			d.rightVal.record, d.rightVal.rng, d.err = d.diffIteratorNextValue(d.right)
			return true
		case diffItCompareResultSameIdentities, diffItCompareResultNeedStartRangeBoth:
			d.leftVal.record, d.leftVal.rng, d.err = d.diffIteratorNextValue(d.left)
			d.rightVal.record, d.rightVal.rng, d.err = d.diffIteratorNextValue(d.right)
		case diffItCompareResultNeedStartRangeLeft:
			d.leftVal.record, d.leftVal.rng, d.err = d.diffIteratorNextValue(d.left)
		case diffItCompareResultNeedStartRangeRight:
			d.rightVal.record, d.rightVal.rng, d.err = d.diffIteratorNextValue(d.right)
		case diffItCompareResultLeftBeforeRight:
			// nothing on right, or left before right
			d.currentVal = d.leftVal.asDiff(graveler.DiffTypeRemoved)
			d.leftVal.record, d.leftVal.rng, d.err = d.diffIteratorNextValue(d.left)
			return true
		case diffItCompareResultRightBeforeLeft:
			// nothing on left, or right before left
			d.currentVal = d.rightVal.asDiff(graveler.DiffTypeAdded)
			d.rightVal.record, d.rightVal.rng, d.err = d.diffIteratorNextValue(d.right)
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
	val, rng := it.Value()
	if val == nil {
		return rng.MinKey
	}
	return val.Key
}
