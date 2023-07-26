package committed

import (
	"sort"
	"strings"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type rangeValue struct {
	r  *Range
	vr *graveler.ValueRecord
}

type ImportIterator interface {
	IsCurrentRangeBoundedByPath() bool
	IsCurrentPathIncludedInRange() bool
}

type PrefixIterator struct {
	currentPrefixIndex int
	prefixes           []graveler.Prefix
	rangeIterator      Iterator
	currentRangeValue  rangeValue
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
	hasNext := ipi.rangeIterator.Next()
	if !hasNext {
		return false
	}
	vr, r := ipi.updateValue()
	ipi.updatePath()

	if vr == nil && r != nil { // head of range
		for ipi.IsCurrentRangeBoundedByPath() {
			if ipi.rangeIterator.Err() != nil {
				return false
			}
			hasNext = ipi.rangeIterator.NextRange()
			if !hasNext {
				return false
			}
			ipi.updateValue()
			ipi.updatePath()
		}
	} else {
		prefixLen := len(ipi.prefixes)
		for vr != nil && ipi.currentPrefixIndex < prefixLen && strings.HasPrefix(vr.Key.String(), string(ipi.prefixes[ipi.currentPrefixIndex])) {
			if ipi.rangeIterator.Err() != nil {
				return false
			}
			hasNext = ipi.rangeIterator.Next()
			if !hasNext {
				return false
			}
			vr, _ = ipi.updateValue()
			ipi.updatePath()
		}
	}
	return true
}

func (ipi *PrefixIterator) NextRange() bool {
	hasNext := ipi.rangeIterator.NextRange()
	if !hasNext {
		return false
	}
	ipi.updateValue()
	ipi.updatePath()

	for ipi.IsCurrentRangeBoundedByPath() {
		if ipi.rangeIterator.Err() != nil {
			return false
		}
		hasNext = ipi.rangeIterator.NextRange()
		if !hasNext {
			return false
		}
		ipi.updateValue()
		ipi.currentPrefixIndex++
	}
	return true
}

func NewPrefixIterator(prefixes []graveler.Prefix, rangeIterator Iterator) *PrefixIterator {
	sort.Slice(prefixes, func(i, j int) bool {
		return prefixes[i] < prefixes[j]
	})
	return &PrefixIterator{prefixes: prefixes, currentPrefixIndex: 0, rangeIterator: rangeIterator}
}

func (ipi *PrefixIterator) updatePath() {
	if ipi.currentPrefixIndex >= len(ipi.prefixes) {
		return
	}
	currMinKey := string(ipi.currentRangeValue.r.MinKey)
	if ipi.currentRangeValue.vr != nil {
		currMinKey = string(ipi.currentRangeValue.vr.Key)
	}
	// If the position is smaller or doesn't have the prefix of the current key, get the next prefix.
	// At the end of this function we'll have either a path that is a prefix of the current key, or bigger than the key
	for string(ipi.prefixes[ipi.currentPrefixIndex]) < currMinKey &&
		!strings.HasPrefix(currMinKey, string(ipi.prefixes[ipi.currentPrefixIndex])) {
		p := ipi.currentPrefixIndex + 1
		switch {
		case p >= len(ipi.prefixes): // No more comparable prefixes
			return
		case string(ipi.prefixes[p]) <= string(ipi.currentRangeValue.r.MaxKey):
			ipi.currentPrefixIndex = p
		default:
			return
		}
	}
}

func (ipi *PrefixIterator) getPrefix() *graveler.Prefix {
	if ipi.currentPrefixIndex >= len(ipi.prefixes) {
		return nil
	}
	return &ipi.prefixes[ipi.currentPrefixIndex]
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
	return strings.HasPrefix(string(r.MinKey), string(*p)) || strings.Compare(string(*p), string(r.MaxKey)) <= 0
}
