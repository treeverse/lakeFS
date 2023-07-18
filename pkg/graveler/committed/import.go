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
		sourceValue, sourceRange := sourceImportPathsIter.getValueAndRange()
		destValue, destRange := destImportPathsIter.getValueAndRange()
		switch {
		// Source range before dest range- write it (since it's imported):
		case range1BeforeRange2(sourceRange, destRange) && sourceImportPathsIter.head:
			if err := sourceImportPathsIter.writeRangeAndProgress(); err != nil {
				return err
			}
		// Dest range before the compared path, and at the beginning of the range:
		case destImportPathsIter.isRangeSmallerThanPath() && destImportPathsIter.head:
			if err := destImportPathsIter.writeRangeAndProgress(); err != nil {
				return err
			}
		// The same range -> write it and skip to the next source and dest ranges:
		case sourceRange.ID == destRange.ID:
			if err := sourceImportPathsIter.writeRangeAndProgress(); err != nil {
				return err
			}
			destImportPathsIter.next()
		default: // Source and destination ranges OR destination range and path intersect
			// The dest range's prefix was imported- skip to the next range:
			if destImportPathsIter.isCurrentRangeBoundedByPath() {
				destImportPathsIter.nextRange()
			} else if !isValue1BeforeValue2(destValue, sourceValue) { // Source value <= Dest value
				if err := sourceImportPathsIter.writeRecordAndProgress(); err != nil {
					return err
				}
				// Source value == Dest value, progress dest iter as well:
				if bytes.Equal(sourceValue.Key, destValue.Key) {
					destImportPathsIter.next()
				}
			} else { // Dest value < Source value
				if !destImportPathsIter.isCurrentPathIncludedInValueRecord() { // Non-imported path
					if err := destImportPathsIter.writeRecordAndProgress(); err != nil {
						return err
					}
				} else { // Imported path
					destImportPathsIter.next()
				}
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

func completeSourceImportIteratorRun(sourceImportPathsIter ImportPathsIterator) error {
	for sourceImportPathsIter.hasNext {
		var err error = nil
		if sourceImportPathsIter.head {
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
		switch {
		case !destImportPathsIter.isCurrentPathIncludedInRange() && destImportPathsIter.head:
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
