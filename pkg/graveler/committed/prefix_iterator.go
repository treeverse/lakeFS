package committed

import (
	"strings"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type rangeValue struct {
	r  *Range
	vr *graveler.ValueRecord
}

const done = -1

type ImportIterator interface {
	IsCurrentRangeBoundedByPath() bool
	IsCurrentPathIncludedInRange() bool
}

type PrefixIterator struct {
	position          int // current path position in the slice or -1 if no prefixes left
	prefixes          []graveler.Prefix
	rangeIterator     Iterator
	currentRangeValue rangeValue
	hasNext           bool
}

func (ipi *PrefixIterator) Value() (*graveler.ValueRecord, *Range) {
	return ipi.currentRangeValue.vr, ipi.currentRangeValue.r
}

func (ipi *PrefixIterator) updateValue() (*graveler.ValueRecord, *Range) {
	vr, r := ipi.rangeIterator.Value()
	ipi.currentRangeValue = rangeValue{
		r,
		vr,
	}
	return vr, r
}

func (ipi *PrefixIterator) Err() error {
	return ipi.rangeIterator.Err()
}
func (ipi *PrefixIterator) Close() {
	ipi.rangeIterator.Close()
}
func (ipi *PrefixIterator) SeekGE(id graveler.Key) {
	ipi.rangeIterator.SeekGE(id)
}

func (ipi *PrefixIterator) Next() bool {
	ipi.hasNext = ipi.rangeIterator.Next()
	if !ipi.hasNext {
		return false
	}
	vr, r := ipi.updateValue()
	ipi.updatePath()

	if vr == nil && r != nil { // head of range
		for ipi.IsCurrentRangeBoundedByPath() {
			if ipi.rangeIterator.Err() != nil {
				return false
			}
			ipi.hasNext = ipi.rangeIterator.NextRange()
			if !ipi.hasNext {
				return false
			}
			ipi.updateValue()
			ipi.updatePath()
		}
	} else {
		for vr != nil && ipi.position != done && strings.HasPrefix(vr.Key.String(), string(ipi.prefixes[ipi.position])) {
			if ipi.rangeIterator.Err() != nil {
				return false
			}
			ipi.hasNext = ipi.rangeIterator.Next()
			if !ipi.hasNext {
				return false
			}
			vr, r = ipi.updateValue()
			ipi.updatePath()
		}
	}
	return true
}

func (ipi *PrefixIterator) NextRange() bool {
	ipi.hasNext = ipi.rangeIterator.NextRange()
	if !ipi.hasNext {
		return false
	}
	ipi.updateValue()
	ipi.updatePath()

	for ipi.IsCurrentRangeBoundedByPath() {
		if ipi.rangeIterator.Err() != nil {
			return false
		}
		ipi.hasNext = ipi.rangeIterator.NextRange()
		if !ipi.hasNext {
			return false
		}
		ipi.updateValue()
		ipi.position = ipi.position + 1
		if ipi.position >= len(ipi.prefixes) {
			ipi.position = done
		}
		//ipi.updatePath()
	}
	// Problem: if the range contains the prefix, yet not bounded by it, it will be returned. This is fine, unless
	// this range comes before the source range. This will trigger the "write dest before source scenario" which will be
	// incorrect since we still need to go over the range and ignore all prefixes in it.
	return true
}

func NewPrefixIterator(prefixes []graveler.Prefix, rangeIterator Iterator) *PrefixIterator {
	return &PrefixIterator{prefixes: prefixes, position: 0, rangeIterator: rangeIterator}
}

func (ipi *PrefixIterator) updatePath() {
	if ipi.position == done {
		return
	}
	currMinKey := string(ipi.currentRangeValue.r.MinKey)
	if ipi.currentRangeValue.vr != nil {
		currMinKey = string(ipi.currentRangeValue.vr.Key)
	}
	// If the position is smaller or doesn't have the prefix of the current key, get the next prefix.
	// At the end of this function we'll have either a path that is a prefix of the current key, or bigger than the key
	for string(ipi.prefixes[ipi.position]) < currMinKey &&
		!strings.HasPrefix(currMinKey, string(ipi.prefixes[ipi.position])) {
		p := ipi.position + 1
		switch {
		case p >= len(ipi.prefixes): // No more comparable prefixes
			ipi.position = done
			return
		case string(ipi.prefixes[p]) <= string(ipi.currentRangeValue.r.MaxKey):
			ipi.position = p
		default:
			return
		}
	}
}

func (ipi *PrefixIterator) getPrefix() *graveler.Prefix {
	if ipi.position == done {
		return nil
	}
	return &ipi.prefixes[ipi.position]
}

// IsCurrentRangeBoundedByPath returns true if both the range's max and min keys have the current path as their prefix
func (ipi *PrefixIterator) IsCurrentRangeBoundedByPath() bool {
	p := ipi.getPrefix()
	if p == nil {
		return false
	}
	r := ipi.currentRangeValue.r
	return strings.HasPrefix(string(r.MinKey), string(*p)) && strings.HasPrefix(string(r.MaxKey), string(*p))
}

// IsCurrentPathIncludedInRange returns true if the examined path is either a prefix of the range's min or max key,
// or if the path is between the range's min and max keys.
func (ipi *PrefixIterator) IsCurrentPathIncludedInRange() bool {
	p := ipi.getPrefix()
	if p == nil {
		return false
	}
	r := ipi.currentRangeValue.r
	hasPrefix := strings.HasPrefix(string(r.MinKey), string(*p)) || strings.HasPrefix(string(r.MaxKey), string(*p))
	intersects := strings.Compare(string(*p), string(r.MinKey)) >= 0 && strings.Compare(string(*p), string(r.MaxKey)) <= 0
	return hasPrefix || intersects
}
