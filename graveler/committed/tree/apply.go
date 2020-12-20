package tree

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
)

func isTombstone(v *graveler.ValueRecord) bool {
	return v.Value == nil
}

// Just a sketch of Apply; it will probably be much harder to write...
func Apply(ctx context.Context, writer Writer, source PartsAndValuesIterator, diffs graveler.ValueIterator) error {
	sourceDone, diffsDone := false, false
	for !sourceDone && !diffsDone {
		sourceValue, sourcePart := source.Value()
		diffValue := diffs.Value()
		if sourceValue == nil {
			if bytes.Compare(sourcePart.MaxKey, diffValue.Key) < 0 {
				// Source at start of part which we do not need to scan --
				// write and skip that entire part.
				if err := writer.AddParts([]Part{*sourcePart}); err != nil {
					return fmt.Errorf("copy source part %s: %w", sourcePart.Name, err)
				}
				if ok := source.NextPart(); !ok {
					sourceDone = true
				}
			} else if ok := source.Next(); !ok {
				// Source at start of part which we need to scan.
				sourceDone = true
			}
			continue
		}
		c := bytes.Compare(sourceValue.Key, diffValue.Key)
		switch {
		case c < 0:
			if err := writer.WriteRecord(*sourceValue); err != nil {
				return fmt.Errorf("write source record: %w", err)
			}
			if ok := source.Next(); !ok {
				sourceDone = true
			}
		case c >= 0:
			if !isTombstone(diffValue) {
				if err := writer.WriteRecord(*diffValue); err != nil {
					return fmt.Errorf("write added record: %w", err)
				}
			}
			if ok := diffs.Next(); !ok {
				diffsDone = true
			}
			if c == 0 {
				// Also advance source
				if ok := source.Next(); !ok {
					sourceDone = true
				}
			}
		}
	}
	for !sourceDone {
		sourceValue, sourcePart := source.Value()
		if sourceValue == nil {
			if err := writer.AddParts([]Part{*sourcePart}); err != nil {
				return fmt.Errorf("copy source part %s: %w", sourcePart.Name, err)
			}
			if ok := source.NextPart(); !ok {
				sourceDone = true
			}
		} else {
			if err := writer.WriteRecord(*sourceValue); err != nil {
				return fmt.Errorf("write source record: %w", err)
			}
			if ok := source.Next(); !ok {
				sourceDone = true
			}
		}
	}
	for !diffsDone {
		diffValue := diffs.Value()
		if err := writer.WriteRecord(*diffValue); err != nil {
			return fmt.Errorf("write added record: %w", err)
		}
		if ok := diffs.Next(); !ok {
			diffsDone = true
		}
	}
	return nil
}
