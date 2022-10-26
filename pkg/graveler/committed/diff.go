package committed

import (
	"bytes"
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type iteratorValue struct {
	rng    *Range
	record *graveler.ValueRecord
	err    error
}

// ErrNoRange occurs when calling nextRange while not in a range, could happen when the diff is currently comparing keys in two different ranges
var ErrNoRange = errors.New("diff is not currently in a range")

// currentRangeData holds state of the current RangeDiff
type currentRangeData struct {
	iter             Iterator
	value            *iteratorValue
	currentRangeDiff *RangeDiff
}

type diffIterator struct {
	ctx          context.Context
	left         Iterator
	right        Iterator
	leftValue    iteratorValue
	rightValue   iteratorValue
	currentRange currentRangeData
	currentDiff  *graveler.Diff
	err          error
	state        diffIteratorState
}

// currentRangeLeftIdentity returns the current range identity in case the current range is the left range, otherwise returns nil
func (d diffIterator) currentRangeLeftIdentity() (res []byte) {
	if d.currentRange.iter == d.left {
		res = make([]byte, len(d.currentRange.value.record.Identity))
		copy(res, d.currentRange.value.record.Identity)
	}
	return
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
	diffItCompareResultRightRangeBeforeLeft
	diffItCompareResultLeftRangeBeforeRight
	diffItCompareResultSameBounds
)

func NewDiffIterator(ctx context.Context, left Iterator, right Iterator) DiffIterator {
	return &diffIterator{
		ctx:   ctx,
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

func diffIteratorNextRange(it Iterator) (*graveler.ValueRecord, *Range, error) {
	if it.NextRange() {
		val, rng := it.Value()
		return val, rng, nil
	}
	return nil, nil, it.Err()
}

func (d *diffIterator) setCurrentRangeRight() {
	d.currentRange.iter = d.right
	d.currentRange.value = &d.rightValue
	d.currentRange.currentRangeDiff = &RangeDiff{
		Type:  graveler.DiffTypeAdded,
		Range: d.rightValue.rng.Copy(),
	}
	d.currentDiff = nil
}

func (d *diffIterator) setCurrentRangeBoth() {
	d.currentRange.iter = nil
	d.currentRange.value = &d.rightValue
	d.currentRange.currentRangeDiff = &RangeDiff{
		Type:         graveler.DiffTypeChanged,
		Range:        d.rightValue.rng.Copy(),
		LeftIdentity: d.leftValue.rng.ID,
	}
	d.currentDiff = nil
}

func (d *diffIterator) setCurrentRangeLeft() {
	d.currentRange.iter = d.left
	d.currentRange.value = &d.leftValue
	d.currentRange.currentRangeDiff = &RangeDiff{
		Type:  graveler.DiffTypeRemoved,
		Range: d.leftValue.rng.Copy(),
	}
	d.leftValue.record = nil
	d.currentDiff = nil
}

func (d *diffIterator) clearCurrentRange() {
	d.currentRange.iter = nil
	d.currentRange.value = nil
	d.currentRange.currentRangeDiff = nil
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
	leftRange := d.leftValue.rng
	rightRange := d.rightValue.rng
	if leftRange == nil && rightRange == nil {
		return diffItCompareResultDone
	}
	if leftRange != nil && rightRange != nil && leftRange.ID == rightRange.ID {
		return diffItCompareResultSameRanges
	}
	leftStartRange := leftRange != nil && d.leftValue.record == nil
	rightStartRange := rightRange != nil && d.rightValue.record == nil
	leftBeforeRight := leftStartRange && rightRange == nil
	rightBeforeLeft := rightStartRange && leftRange == nil
	sameBounds := false
	if leftStartRange && rightStartRange {
		leftBeforeRight = bytes.Compare(leftRange.MaxKey, rightRange.MinKey) < 0
		rightBeforeLeft = bytes.Compare(rightRange.MaxKey, leftRange.MinKey) < 0
		sameBounds = bytes.Equal(leftRange.MinKey, rightRange.MinKey) && bytes.Equal(leftRange.MaxKey, rightRange.MaxKey)
	}
	comp := d.compareDiffKeys()
	switch {
	case leftBeforeRight:
		return diffItCompareResultLeftRangeBeforeRight
	case rightBeforeLeft:
		return diffItCompareResultRightRangeBeforeLeft
	case leftStartRange && rightStartRange && comp == 0 && sameBounds:
		return diffItCompareResultSameBounds
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
	if d.state == diffIteratorStateClosed || d.err != nil {
		return false
	}
	if d.state == diffIteratorStatePreInit {
		d.state = diffIteratorStateOpen
		d.clearCurrentRange()
		d.leftValue.record, d.leftValue.rng, d.leftValue.err = diffIteratorNextValue(d.left)
		d.rightValue.record, d.rightValue.rng, d.rightValue.err = diffIteratorNextValue(d.right)
	}
	if d.currentRange.iter != nil {
		// we are currently inside a range
		d.currentRange.value.record, d.currentRange.value.rng, d.currentRange.value.err = diffIteratorNextValue(d.currentRange.iter)
		if d.currentRange.value.err != nil {
			d.err = d.currentRange.value.err
			d.currentDiff = nil
			d.clearCurrentRange()
			return false
		}
		if d.currentRange.value.record != nil {
			leftIdentity := d.currentRangeLeftIdentity()
			d.currentDiff = &graveler.Diff{Type: d.currentRange.currentRangeDiff.Type, Key: d.currentRange.value.record.Key.Copy(), Value: d.currentRange.value.record.Value, LeftIdentity: leftIdentity}
			return true
		}
		// current diff range over - clear current range and continue to get next range/value
		d.clearCurrentRange()
	}
	select {
	case <-d.ctx.Done():
		d.err = d.ctx.Err()
		return false
	default:
		for {
			if d.rightValue.err != nil {
				d.err = d.rightValue.err
			}
			if d.leftValue.err != nil {
				d.err = d.leftValue.err
			}
			if d.err != nil {
				d.currentDiff = nil
				d.clearCurrentRange()
				return false
			}
			compareResult := d.compareDiffIterators()
			switch compareResult {
			case diffItCompareResultDone:
				d.currentDiff = nil
				return false
			case diffItCompareResultSameRanges:
				d.leftValue.record, d.leftValue.rng, d.leftValue.err = diffIteratorNextRange(d.left)
				d.rightValue.record, d.rightValue.rng, d.rightValue.err = diffIteratorNextRange(d.right)
			case diffItCompareResultLeftRangeBeforeRight:
				d.setCurrentRangeLeft()
				return true
			case diffItCompareResultRightRangeBeforeLeft:
				d.setCurrentRangeRight()
				return true
			case diffItCompareResultSameBounds:
				d.setCurrentRangeBoth()
				d.state = diffIteratorStatePreInit
				return true
			case diffItCompareResultSameKeys:
				// same keys on different ranges
				d.currentDiff = &graveler.Diff{Type: graveler.DiffTypeChanged, Key: d.rightValue.record.Key.Copy(), Value: d.rightValue.record.Value, LeftIdentity: d.leftValue.record.Identity}
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
				d.currentDiff = &graveler.Diff{Type: graveler.DiffTypeRemoved, Key: d.leftValue.record.Key.Copy(), Value: d.leftValue.record.Value, LeftIdentity: d.leftValue.record.Identity}
				d.leftValue.record, d.leftValue.rng, d.leftValue.err = diffIteratorNextValue(d.left)
				return true
			case diffItCompareResultRightBeforeLeft:
				// nothing on left, or right before left
				d.currentDiff = &graveler.Diff{Type: graveler.DiffTypeAdded, Key: d.rightValue.record.Key.Copy(), Value: d.rightValue.record.Value}
				d.rightValue.record, d.rightValue.rng, d.rightValue.err = diffIteratorNextValue(d.right)
				return true
			}
		}
	}
}

func (d *diffIterator) NextRange() bool {
	if d.currentRange.currentRangeDiff != nil && d.currentRange.currentRangeDiff.Type == graveler.DiffTypeChanged {
		d.leftValue.record, d.leftValue.rng, d.leftValue.err = diffIteratorNextRange(d.left)
		d.rightValue.record, d.rightValue.rng, d.rightValue.err = diffIteratorNextRange(d.right)
		d.clearCurrentRange()
		d.state = diffIteratorStateOpen
		return d.Next()
	}
	if d.currentRange.iter == nil {
		d.err = ErrNoRange
		return false
	}
	d.currentRange.value.record, d.currentRange.value.rng, d.currentRange.value.err = diffIteratorNextRange(d.currentRange.iter)
	d.clearCurrentRange()
	return d.Next()
}

func (d *diffIterator) SeekGE(id graveler.Key) {
	d.left.SeekGE(id)
	d.right.SeekGE(id)
	d.currentDiff = nil
	d.leftValue = iteratorValue{}
	d.rightValue = iteratorValue{}
	d.err = nil
	d.state = diffIteratorStatePreInit
	d.currentRange = currentRangeData{}
}

func (d *diffIterator) Value() (*graveler.Diff, *RangeDiff) {
	return d.currentDiff, d.currentRange.currentRangeDiff
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
