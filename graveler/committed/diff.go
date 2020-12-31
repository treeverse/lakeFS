package committed

import (
	"bytes"

	"github.com/treeverse/lakefs/graveler"
)

type iteratorValue struct {
	rng    *Range
	record *graveler.ValueRecord
	err    error
}

type diffIterator struct {
	left        Iterator
	right       Iterator
	leftValue   iteratorValue
	rightValue  iteratorValue
	currentDiff *graveler.Diff
	err         error
	state       diffIteratorState
}

type diffIteratorState int

const (
	diffIteratorStatePreInit diffIteratorState = iota
	diffIteratorStateOpen
	diffIteratorStateClosed
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
		left:  left,
		right: right,
	}
}

func diffIteratorNextValue(it Iterator) (*graveler.ValueRecord, *Range, error) {
	if it.Next() {
		rec, rng := it.Value()
		return rec, rng, nil
	}
	return nil, nil, it.Err()
}

func diffIteratorNextRange(it Iterator) (*Range, error) {
	if it.NextRange() {
		_, rng := it.Value()
		return rng, nil
	}
	return nil, it.Err()
}

func (d *diffIterator) compareDiffKeys() int {
	if d.leftValue.rng == nil {
		return 1
	}
	if d.rightValue.rng == nil {
		return -1
	}
	return bytes.Compare(getCurrentKey(d.left), getCurrentKey(d.right))
}

func (d *diffIterator) compareDiffIterators() diffIteratorCompareResult {
	if d.leftValue.rng == nil && d.rightValue.rng == nil {
		return diffItCompareResultDone
	}
	if d.leftValue.rng != nil && d.rightValue.rng != nil && d.leftValue.rng.ID == d.rightValue.rng.ID {
		return diffItCompareResultSameRanges
	}
	leftStartRange := d.leftValue.rng != nil && d.leftValue.record == nil
	rightStartRange := d.rightValue.rng != nil && d.rightValue.record == nil
	comp := d.compareDiffKeys()
	switch {
	case leftStartRange && rightStartRange && comp == 0:
		return diffItCompareResultNeedStartRangeBoth
	case leftStartRange && comp <= 0:
		return diffItCompareResultNeedStartRangeLeft
	case rightStartRange && comp >= 0:
		return diffItCompareResultNeedStartRangeRight
	case comp == 0 && bytes.Equal(d.leftValue.record.Identity, d.rightValue.record.Identity):
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
	if d.state == diffIteratorStateClosed {
		return false
	}
	if d.state == diffIteratorStatePreInit {
		d.state = diffIteratorStateOpen
		d.leftValue.record, d.leftValue.rng, d.leftValue.err = diffIteratorNextValue(d.left)
		d.rightValue.record, d.rightValue.rng, d.rightValue.err = diffIteratorNextValue(d.right)
	}
	for {
		if d.rightValue.err != nil {
			d.err = d.rightValue.err
		}
		if d.leftValue.err != nil {
			d.err = d.leftValue.err
		}
		if d.err != nil {
			d.currentDiff = nil
			return false
		}
		compareResult := d.compareDiffIterators()
		switch compareResult {
		case diffItCompareResultDone:
			d.currentDiff = nil
			return false
		case diffItCompareResultSameRanges:
			d.leftValue.rng, d.leftValue.err = diffIteratorNextRange(d.left)
			d.rightValue.rng, d.rightValue.err = diffIteratorNextRange(d.right)
		case diffItCompareResultSameKeys:
			// same keys on different ranges
			d.currentDiff = &graveler.Diff{Type: graveler.DiffTypeChanged, Key: d.rightValue.record.Key, Value: d.rightValue.record.Value, OldIdentity: d.leftValue.record.Identity}
			d.leftValue.record, d.leftValue.rng, d.leftValue.err = diffIteratorNextValue(d.left)
			d.rightValue.record, d.rightValue.rng, d.rightValue.err = diffIteratorNextValue(d.right)
			return true
		case diffItCompareResultSameIdentities, diffItCompareResultNeedStartRangeBoth:
			d.leftValue.record, d.leftValue.rng, d.leftValue.err = diffIteratorNextValue(d.left)
			d.rightValue.record, d.rightValue.rng, d.rightValue.err = diffIteratorNextValue(d.right)
		case diffItCompareResultNeedStartRangeLeft:
			d.leftValue.record, d.leftValue.rng, d.leftValue.err = diffIteratorNextValue(d.left)
		case diffItCompareResultNeedStartRangeRight:
			d.rightValue.record, d.rightValue.rng, d.rightValue.err = diffIteratorNextValue(d.right)
		case diffItCompareResultLeftBeforeRight:
			// nothing on right, or left before right
			d.currentDiff = &graveler.Diff{Type: graveler.DiffTypeRemoved, Key: d.leftValue.record.Key, Value: d.leftValue.record.Value, OldIdentity: d.leftValue.record.Identity}
			d.leftValue.record, d.leftValue.rng, d.leftValue.err = diffIteratorNextValue(d.left)
			return true
		case diffItCompareResultRightBeforeLeft:
			// nothing on left, or right before left
			d.currentDiff = &graveler.Diff{Type: graveler.DiffTypeAdded, Key: d.rightValue.record.Key, Value: d.rightValue.record.Value}
			d.rightValue.record, d.rightValue.rng, d.rightValue.err = diffIteratorNextValue(d.right)
			return true
		}
	}
}

func (d *diffIterator) SeekGE(id graveler.Key) {
	d.left.SeekGE(id)
	d.right.SeekGE(id)
	d.currentDiff = nil
	d.leftValue = iteratorValue{}
	d.rightValue = iteratorValue{}
	d.err = nil
	d.state = diffIteratorStatePreInit
}

func (d *diffIterator) Value() *graveler.Diff {
	return d.currentDiff
}

func (d *diffIterator) Err() error {
	return d.err
}

func (d *diffIterator) Close() {
	d.left.Close()
	d.right.Close()
	d.currentDiff = nil
	d.err = nil
	d.state = diffIteratorStateClosed
}

func getCurrentKey(it Iterator) []byte {
	val, rng := it.Value()
	if val == nil {
		return rng.MinKey
	}
	return val.Key
}
