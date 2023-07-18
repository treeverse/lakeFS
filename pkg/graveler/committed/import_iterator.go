package committed

import (
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type rangeValue struct {
	r  *Range
	vr *graveler.ValueRecord
}

const done = -1

type ImportPathsIterator struct {
	position          int // current path position in the slice or -1 if no paths left
	paths             []graveler.ImportPath
	rangeIterator     Iterator
	currentRangeValue rangeValue
	writer            MetaRangeWriter
	hasNext           bool
	head              bool
}

func newImportPathsIterator(paths []graveler.ImportPath, rangeIterator Iterator, writer MetaRangeWriter) ImportPathsIterator {
	return ImportPathsIterator{paths: paths, position: 0, rangeIterator: rangeIterator, writer: writer}
}

func (ipi *ImportPathsIterator) err() error {
	return ipi.rangeIterator.Err()
}

func (ipi *ImportPathsIterator) next() bool {
	ipi.hasNext = ipi.rangeIterator.Next()
	if !ipi.hasNext {
		return false
	}
	vr, r := ipi.rangeIterator.Value()
	ipi.head = vr == nil && r != nil
	if ipi.head { // Head of a new range
		// Progress to the first record in the range
		ipi.hasNext = ipi.rangeIterator.Next()
		vr, r = ipi.rangeIterator.Value()
	}
	ipi.currentRangeValue = rangeValue{
		r,
		vr,
	}
	ipi.updatePath()
	return true
}

func (ipi *ImportPathsIterator) nextRange() bool {
	ipi.hasNext = ipi.rangeIterator.NextRange()
	if !ipi.hasNext {
		return false
	}
	ipi.hasNext = ipi.rangeIterator.Next()
	vr, r := ipi.rangeIterator.Value()
	ipi.currentRangeValue = rangeValue{
		r,
		vr,
	}
	ipi.head = true
	ipi.updatePath()
	return true
}

func (ipi *ImportPathsIterator) updatePath() {
	if ipi.position == done {
		return
	}
	// If the position is smaller or doesn't have the prefix of the current key, get the next prefix.
	// At the end of this function we'll have either a path that is a prefix of the current key, or bigger than the key
	for ipi.paths[ipi.position].Destination < string(ipi.currentRangeValue.vr.Key) &&
		!strings.HasPrefix(string(ipi.currentRangeValue.vr.Key), ipi.paths[ipi.position].Destination) {
		p := ipi.position + 1
		if p >= len(ipi.paths) { // No more comparable paths
			ipi.position = done
			break
		} else if ipi.head || (!ipi.head && ipi.paths[p].Destination <= string(ipi.currentRangeValue.r.MaxKey)) {
			ipi.position = p
		} else {
			break
		}
	}
}

func (ipi *ImportPathsIterator) getPath() *graveler.ImportPath {
	if ipi.position == done {
		return nil
	}
	return &ipi.paths[ipi.position]
}

func (ipi *ImportPathsIterator) getValueAndRange() (*graveler.ValueRecord, *Range) {
	return ipi.currentRangeValue.vr, ipi.currentRangeValue.r
}

// isCurrentRangeBoundedByPath returns true if both the range's max and min keys have the current path as their prefix
func (ipi *ImportPathsIterator) isCurrentRangeBoundedByPath() bool {
	p := ipi.getPath()
	if p == nil {
		return false
	}
	r := ipi.currentRangeValue.r
	return strings.HasPrefix(string(r.MinKey), p.Destination) && strings.HasPrefix(string(r.MaxKey), p.Destination)
}

// isCurrentPathIncludedInRange returns true if the examined path is either a prefix of the range's min or max key,
// or if the path is between the range's min and max keys.
func (ipi *ImportPathsIterator) isCurrentPathIncludedInRange() bool {
	p := ipi.getPath()
	if p == nil {
		return false
	}
	r := ipi.currentRangeValue.r
	hasPrefix := strings.HasPrefix(string(r.MinKey), p.Destination) || strings.HasPrefix(string(r.MaxKey), p.Destination)
	intersects := strings.Compare(p.Destination, string(r.MinKey)) >= 0 && strings.Compare(p.Destination, string(r.MaxKey)) <= 0
	return hasPrefix || intersects
}

// isCurrentPathIncludedInValueRecord returns true if the examined path is a prefix of the current examined value record
func (ipi *ImportPathsIterator) isCurrentPathIncludedInValueRecord() bool {
	p := ipi.getPath()
	if p == nil {
		return false
	}
	vr := ipi.currentRangeValue.vr
	return strings.HasPrefix(string(vr.Key), p.Destination)
}

func (ipi *ImportPathsIterator) isRangeSmallerThanPath() bool {
	p := ipi.getPath()
	if p == nil {
		return false
	}
	r := ipi.currentRangeValue.r
	return strings.Compare(string(r.MaxKey), p.Destination) < 0
}

func (ipi *ImportPathsIterator) writeRangeAndProgress() error {
	if err := ipi.writer.WriteRange(*ipi.currentRangeValue.r); err != nil {
		return err
	}
	ipi.nextRange()
	return nil
}

func (ipi *ImportPathsIterator) writeRecordAndProgress() error {
	if err := ipi.writer.WriteRecord(*ipi.currentRangeValue.vr); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	ipi.next()
	return nil
}
