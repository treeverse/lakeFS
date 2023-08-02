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
	IsCurrentRangeBoundedByPrefix() bool
	IsCurrentPrefixIncludedInRange() bool
}

type SkipPrefixIterator struct {
	currentPrefixIndex int
	prefixes           []graveler.Prefix
	rangeIterator      Iterator
	currentRangeValue  rangeValue
}

func (ipi *SkipPrefixIterator) Value() (*graveler.ValueRecord, *Range) {
	return ipi.currentRangeValue.vr, ipi.currentRangeValue.r
}

func (ipi *SkipPrefixIterator) updateValue() (*graveler.ValueRecord, *Range) {
	vr, r := ipi.rangeIterator.Value()
	ipi.currentRangeValue = rangeValue{
		r,
		vr,
	}
	return vr, r
}

func (ipi *SkipPrefixIterator) Err() error {
	return ipi.rangeIterator.Err()
}
func (ipi *SkipPrefixIterator) Close() {
	ipi.rangeIterator.Close()
}
func (ipi *SkipPrefixIterator) SeekGE(id graveler.Key) {
	ipi.rangeIterator.SeekGE(id)
}

func (ipi *SkipPrefixIterator) Next() bool {
	if !ipi.rangeIterator.Next() {
		return false
	}
	vr, r := ipi.updateValue()
	ipi.updatePrefix()

	if vr == nil && r != nil { // head of range
		for ipi.IsCurrentRangeBoundedByPrefix() {
			if !ipi.rangeIterator.NextRange() {
				return false
			}
			ipi.updateValue()
			ipi.updatePrefix()
		}
	} else {
		prefixLen := len(ipi.prefixes)
		for vr != nil && ipi.currentPrefixIndex < prefixLen && strings.HasPrefix(vr.Key.String(), string(ipi.prefixes[ipi.currentPrefixIndex])) {
			if !ipi.rangeIterator.Next() {
				return false
			}
			vr, _ = ipi.updateValue()
			ipi.updatePrefix()
		}
	}
	return true
}

func (ipi *SkipPrefixIterator) NextRange() bool {
	if !ipi.rangeIterator.NextRange() {
		return false
	}
	ipi.updateValue()
	ipi.updatePrefix()

	for ipi.IsCurrentRangeBoundedByPrefix() {
		if !ipi.rangeIterator.NextRange() {
			return false
		}
		ipi.updateValue()
		ipi.updatePrefix()
	}
	return true
}

func (ipi *SkipPrefixIterator) updatePrefix() {
	if ipi.currentPrefixIndex >= len(ipi.prefixes) {
		return
	}
	currMinKey := string(ipi.currentRangeValue.r.MinKey)
	if ipi.currentRangeValue.vr != nil {
		currMinKey = string(ipi.currentRangeValue.vr.Key)
	}
	// If the current prefix is smaller and isn't the prefix of the currentMinKey, get the next prefix.
	// By the end of this loop, the examined prefix will either be the prefix of the currentMinKey, or
	// lexicographically bigger than it.
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

func (ipi *SkipPrefixIterator) getPrefix() *graveler.Prefix {
	if ipi.currentPrefixIndex >= len(ipi.prefixes) {
		return nil
	}
	return &ipi.prefixes[ipi.currentPrefixIndex]
}

// IsCurrentRangeBoundedByPrefix returns true if both the range's max and min keys have the current prefix.
func (ipi *SkipPrefixIterator) IsCurrentRangeBoundedByPrefix() bool {
	p := ipi.getPrefix()
	if p == nil {
		return false
	}
	r := ipi.currentRangeValue.r
	return strings.HasPrefix(string(r.MinKey), string(*p)) && strings.HasPrefix(string(r.MaxKey), string(*p))
}

// IsCurrentPrefixIncludedInRange returns true if the examined prefix is either a prefix of the range's min or max key,
// or if the prefix is between the range's min and max keys.
func (ipi *SkipPrefixIterator) IsCurrentPrefixIncludedInRange() bool {
	p := ipi.getPrefix()
	if p == nil {
		return false
	}
	r := ipi.currentRangeValue.r
	inRange := strings.Compare(string(*p), string(r.MinKey)) >= 0 && strings.Compare(string(*p), string(r.MaxKey)) <= 0
	return strings.HasPrefix(string(r.MinKey), string(*p)) || inRange
}

func NewSkipPrefixIterator(prefixes []graveler.Prefix, rangeIterator Iterator) *SkipPrefixIterator {
	sort.Slice(prefixes, func(i, j int) bool {
		return prefixes[i] < prefixes[j]
	})
	return &SkipPrefixIterator{prefixes: prefixes, currentPrefixIndex: 0, rangeIterator: rangeIterator}
}
