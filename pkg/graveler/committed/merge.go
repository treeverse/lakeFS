package committed

import (
	"bytes"
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type mergeState int

const (
	mergeStateInvalid mergeState = iota
	// Range states
	mergeStateInitial
	mergeStateRangeAddedBySource
	mergeStateRangeAddedByDest
	mergeStateRangeNoChange
	mergeStateRangeRemovedBySource
	mergeStateRangeRemovedByDest
	mergeStateRangeConflict
	mergeStateValueAddedBySource
	mergeStateValueAddedByDest
	mergeStateValueRemovedBySource
	mergeStateValueRemovedByDest
	mergeStateValueChangedBySource
	mergeStateValueChangedByDest
	mergeStateValueNoChange
	mergeStateValueConflicting

	mergeStateEnterSourceRange
	mergeStateEnterDestRange
)

type mergeSt struct {
	baseIter, sourceIter, destIter    Iterator
	baseNext, sourceNext, destNext    bool
	baseValue, sourceValue, destValue *graveler.ValueRecord
	baseRange, sourceRange, destRange *Range
}

func Merge(ctx context.Context, writer MetaRangeWriter, baseIter Iterator, sourceIter Iterator, destIter Iterator, opts *ApplyOptions) (graveler.DiffSummary, error) {
	ret := graveler.DiffSummary{Count: make(map[graveler.DiffType]int)}
	var sourceValue, destValue *graveler.ValueRecord
	var sourceRange, destRange *Range
	sourceNext, destNext := sourceIter.Next(), destIter.Next()
	for sourceNext && destNext {
		select {
		case <-ctx.Done():
			return graveler.DiffSummary{}, ctx.Err()
		default:
		}
		sourceValue, sourceRange = sourceIter.Value()
		destValue, destRange = destIter.Value()
		state, err := getState(baseIter, sourceValue, sourceRange, destValue, destRange)
		if err != nil {
			return graveler.DiffSummary{}, err
		}
		switch state {
		case mergeStateRangeAddedBySource:
			ret.Count[graveler.DiffTypeAdded] += int(sourceRange.Count)
			sourceNext = sourceIter.NextRange()
			err = writer.WriteRange(*sourceRange)
		case mergeStateRangeAddedByDest:
			destNext = destIter.NextRange()
			err = writer.WriteRange(*destRange)
		case mergeStateRangeNoChange:
			destNext = destIter.NextRange()
			err = writer.WriteRange(*destRange)
		case mergeStateRangeRemovedBySource:
			ret.Count[graveler.DiffTypeRemoved] += int(sourceRange.Count)
			destNext = destIter.NextRange()
		case mergeStateRangeRemovedByDest:
			sourceNext = sourceIter.NextRange()
		case mergeStateRangeConflict:
			destNext = destIter.Next()
			sourceNext = sourceIter.Next()
		case mergeStateValueAddedBySource:
			err = writer.WriteRecord(*sourceValue)
			ret.Count[graveler.DiffTypeAdded]++
			sourceNext = sourceIter.Next()
		case mergeStateValueAddedByDest:
			destNext = destIter.Next()
			err = writer.WriteRecord(*destValue)
		case mergeStateValueRemovedBySource:
			ret.Count[graveler.DiffTypeRemoved]++
			sourceNext = sourceIter.Next()
		case mergeStateValueRemovedByDest:
			destNext = destIter.Next()
		case mergeStateValueNoChange:
			destNext = destIter.Next()
			sourceNext = sourceIter.Next()
			err = writer.WriteRecord(*destValue)
		case mergeStateValueChangedByDest:
			destNext = destIter.Next()
			sourceNext = sourceIter.Next()
			err = writer.WriteRecord(*destValue)
		case mergeStateValueChangedBySource:
			ret.Count[graveler.DiffTypeChanged]++
			destNext = destIter.Next()
			sourceNext = sourceIter.Next()
			err = writer.WriteRecord(*sourceValue)
		case mergeStateValueConflicting:
			return graveler.DiffSummary{}, graveler.ErrConflictFound
		case mergeStateEnterSourceRange:
			sourceNext = sourceIter.Next()
		case mergeStateEnterDestRange:
			destNext = destIter.Next()
		default:
			// error unknown state
		}
		if err != nil {
			return graveler.DiffSummary{}, err
		}
	}

	if sourceNext {

	}

	if destNext {

	}

	if opts != nil && !opts.AllowEmpty && ret.Count[graveler.DiffTypeAdded]+ret.Count[graveler.DiffTypeRemoved]+ret.Count[graveler.DiffTypeChanged] == 0 {
		return ret, graveler.ErrNoChanges
	}
	// handle leftovers

	// only base left -> removed by both (some kind of conflict ) - consider what to do in this case
	return ret, nil
}

type compareResult int

const (
	compareResultRangeConflict compareResult = iota
	compareResultRangeSourceBefore
	compareResultRangeSourceBeforeValue
	compareResultRangeDestBefore
	compareResultRangeDestBeforeValue
	compareResultRangeIdentical
	compareResultValueSourceBefore
	compareResultValueDestBefore
	compareResultValueIdentical
	compareResultValueConflicting
)

func compareValues(sourceValue *graveler.ValueRecord, destValue *graveler.ValueRecord) compareResult {
	c := bytes.Compare(sourceValue.Key, destValue.Key)
	switch {
	case c < 0:
		return compareResultValueSourceBefore
	case c > 0:
		return compareResultValueDestBefore
	case bytes.Equal(sourceValue.Identity, destValue.Identity):
		return compareResultValueIdentical
	default:
		return compareResultValueConflicting
	}
}

func compare(sourceValue *graveler.ValueRecord, sourceRange *Range, destValue *graveler.ValueRecord, destRange *Range) compareResult {
	switch {
	case sourceValue != nil && destValue != nil:
		return compareValues(sourceValue, destValue)
	case sourceValue != nil && bytes.Compare(sourceValue.Key, destRange.MinKey) < 0:
		return compareResultValueSourceBefore
	case sourceValue != nil && bytes.Compare(destRange.MaxKey, sourceValue.Key) < 0:
		return compareResultRangeDestBefore
	case sourceValue != nil:
		return compareResultRangeDestBeforeValue
	case destValue != nil && bytes.Compare(destValue.Key, sourceRange.MinKey) < 0:
		return compareResultValueDestBefore
	case destValue != nil && bytes.Compare(sourceRange.MaxKey, destValue.Key) < 0:
		return compareResultRangeSourceBefore
	case destValue != nil:
		return compareResultRangeSourceBeforeValue
	case bytes.Compare(sourceRange.MaxKey, destRange.MinKey) < 0:
		return compareResultRangeSourceBefore
	case bytes.Compare(destRange.MaxKey, sourceRange.MinKey) < 0:
		return compareResultRangeDestBefore
	case destRange.ID == sourceRange.ID:
		return compareResultRangeIdentical
	default:
		return compareResultRangeConflict
		// ranges are overlapping with different IDs
	}
}

func proceedToRange(iter Iterator, key Key) error {
	hasMore := true
	for hasMore {
		_, rng := iter.Value()
		if rng == nil {
			hasMore = iter.Next() // TODO(Guys): change
		}
		if rng == nil || bytes.Compare(key, rng.MaxKey) <= 0 {
			return nil
		}
		hasMore = iter.NextRange()
	}
	return iter.Err()
}

func proceedToValue(iter Iterator, key Key) error {
	hasMore := true
	for hasMore {
		record, rng := iter.Value()
		if record == nil && rng == nil {
			hasMore = iter.Next() // TODO(Guys): change
			continue
		}
		switch {
		case bytes.Compare(rng.MaxKey, key) <= 0:
			hasMore = iter.NextRange()
		case bytes.Compare(key, rng.MinKey) <= 0:
			return nil
		case record != nil && bytes.Compare(key, record.Key) <= 0:
			return nil
		default:
			hasMore = iter.Next()
		}

	}
	return iter.Err()
}
func getState(baseIter Iterator, sourceValue *graveler.ValueRecord, sourceRange *Range, destValue *graveler.ValueRecord, destRange *Range) (mergeState, error) {
	compareState := compare(sourceValue, sourceRange, destValue, destRange)
	switch compareState {
	case compareResultRangeConflict:
		return mergeStateRangeConflict, nil
	case compareResultRangeSourceBefore:
		err := proceedToRange(baseIter, sourceRange.MinKey)
		if err != nil {
			return mergeStateInvalid, err
		}
		_, baseRange := baseIter.Value()
		if baseRange == nil || bytes.Compare(sourceRange.MaxKey, baseRange.MinKey) < 0 {
			return mergeStateRangeAddedBySource, nil
		} else if sourceRange.ID == baseRange.ID {
			return mergeStateRangeRemovedByDest, nil
		} else {
			return mergeStateEnterSourceRange, nil
		}
	case compareResultRangeSourceBeforeValue:
		return mergeStateEnterSourceRange, nil
	case compareResultRangeDestBefore:
		err := proceedToRange(baseIter, destRange.MinKey)
		if err != nil {
			return mergeStateInvalid, err
		}
		_, baseRange := baseIter.Value()
		if baseRange == nil || bytes.Compare(destRange.MaxKey, baseRange.MinKey) < 0 {
			return mergeStateRangeAddedByDest, nil
		} else if destRange.ID == baseRange.ID {
			return mergeStateRangeRemovedBySource, nil // TODO(Guys): should be added by dest
		} else {
			return mergeStateEnterDestRange, nil
		}
	case compareResultRangeDestBeforeValue:
		return mergeStateEnterDestRange, nil
	case compareResultRangeIdentical:
		return mergeStateRangeNoChange, nil
	case compareResultValueSourceBefore:
		err := proceedToValue(baseIter, sourceRange.MinKey)
		if err != nil {
			return mergeStateInvalid, err
		}
		baseValue, _ := baseIter.Value()
		if baseValue != nil && bytes.Equal(sourceValue.Identity, baseValue.Identity) {
			return mergeStateValueRemovedByDest, nil
		} else if baseValue != nil && bytes.Equal(sourceValue.Key, baseValue.Key) {
			return mergeStateValueConflicting, nil // changed by source and removed by dest, nil
		} else {
			return mergeStateValueAddedBySource, nil
		}
	case compareResultValueDestBefore:
		err := proceedToValue(baseIter, destRange.MinKey)
		if err != nil {
			return mergeStateInvalid, err
		}
		baseValue, _ := baseIter.Value()
		if bytes.Equal(destValue.Identity, baseValue.Identity) {
			return mergeStateValueRemovedBySource, nil
		} else if bytes.Equal(destValue.Key, baseValue.Key) {
			return mergeStateValueConflicting, nil // changed by source and removed by dest, nil
		} else {
			return mergeStateValueAddedByDest, nil
		}
	case compareResultValueIdentical:
		return mergeStateValueNoChange, nil
	case compareResultValueConflicting:
		err := proceedToValue(baseIter, sourceRange.MinKey)
		if err != nil {
			return mergeStateInvalid, err
		}
		baseValue, _ := baseIter.Value()
		// base has no key -> error
		if bytes.Equal(sourceValue.Identity, baseValue.Identity) {
			return mergeStateValueChangedByDest, nil
		} else if bytes.Equal(destValue.Identity, baseValue.Identity) {
			return mergeStateValueChangedBySource, nil
		}
		return mergeStateValueConflicting, nil
	}
	return mergeStateInvalid, nil
}
