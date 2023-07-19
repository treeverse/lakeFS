package committed

import (
	"bytes"
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

func importByPaths(ctx context.Context, destRangeIter, sourceRangeIter Iterator, writer MetaRangeWriter, paths []graveler.ImportPath) error {
	sourceImportPathsIter := newImportPathsIterator(paths, sourceRangeIter, writer)
	destImportPathsIter := newImportPathsIterator(paths, destRangeIter, writer)
	sourceImportPathsIter.next()
	destImportPathsIter.next()
	if err := sourceImportPathsIter.err(); err != nil {
		return err
	}
	if err := destImportPathsIter.err(); err != nil {
		return err
	}

	for sourceImportPathsIter.hasNext && destImportPathsIter.hasNext {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		sourceRange, isSourceHead := sourceImportPathsIter.getRange()
		destRange, isDestHead := destImportPathsIter.getRange()
		switch {
		// Source range before dest range- write it (since it's imported):
		case range1BeforeRange2(sourceRange, destRange) && isSourceHead:
			if err := sourceImportPathsIter.writeRangeAndProgress(); err != nil {
				return err
			}
		// Dest range before the compared path, and at the beginning of the range:
		case destImportPathsIter.isRangeSmallerThanPath() && isDestHead:
			if err := destImportPathsIter.writeRangeAndProgress(); err != nil {
				return err
			}
		// The same range -> write it and skip to the next source and dest ranges:
		case sourceRange.ID == destRange.ID:
			if err := sourceImportPathsIter.writeRangeAndProgress(); err != nil {
				return err
			}
			destImportPathsIter.nextRange()
		default: // Source and destination ranges OR destination range and path intersect
			err := handleIntersection(sourceImportPathsIter, destImportPathsIter)
			if err != nil {
				return err
			}
		}
	}
	if sourceImportPathsIter.hasNext {
		if err := completeSourceImportIteratorRun(sourceImportPathsIter); err != nil {
			return err
		}
	} else if destImportPathsIter.hasNext {
		if err := completeDestImportIteratorRun(destImportPathsIter); err != nil {
			return err
		}
	}
	return nil
}

func handleIntersection(sourceImportPathsIter, destImportPathsIter ImportPathsIterator) error {
	// The dest range's prefix was imported- skip to the next range:
	if destImportPathsIter.isCurrentRangeBoundedByPath() {
		destImportPathsIter.nextRange()
		return nil
	}
	sourceValue, err := sourceImportPathsIter.getValue()
	destValue, err := destImportPathsIter.getValue()
	if err != nil {
		return err
	}
	switch {
	case !isValue1BeforeValue2(destValue, sourceValue): // Source value <= Dest value
		if err := sourceImportPathsIter.writeRecordAndProgress(); err != nil {
			return err
		}
		// Source value == Dest value, progress dest iter as well:
		if bytes.Equal(sourceValue.Key, destValue.Key) {
			destImportPathsIter.next()
		}
	default: // Dest value < Source value
		if !destImportPathsIter.isCurrentPathIncludedInValueRecord() { // Non-imported path
			if err := destImportPathsIter.writeRecordAndProgress(); err != nil {
				return err
			}
		} else { // Imported path
			destImportPathsIter.next()
		}
	}
	return nil
}

func completeSourceImportIteratorRun(sourceImportPathsIter ImportPathsIterator) error {
	for sourceImportPathsIter.hasNext {
		var err error = nil
		_, isSourceHead := sourceImportPathsIter.getRange()
		if isSourceHead {
			err = sourceImportPathsIter.writeRangeAndProgress()
		} else {
			err = sourceImportPathsIter.writeRecordAndProgress()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func completeDestImportIteratorRun(destImportPathsIter ImportPathsIterator) error {
	for destImportPathsIter.hasNext {
		_, isDestHead := destImportPathsIter.getRange()
		switch {
		case !destImportPathsIter.isCurrentPathIncludedInRange() && isDestHead:
			if err := destImportPathsIter.writeRangeAndProgress(); err != nil {
				return err
			}
		case destImportPathsIter.isCurrentRangeBoundedByPath():
			destImportPathsIter.nextRange()
		case !destImportPathsIter.isCurrentPathIncludedInValueRecord():
			if err := destImportPathsIter.writeRecordAndProgress(); err != nil {
				return err
			}
		default:
			destImportPathsIter.next()
		}
	}
	return nil
}
