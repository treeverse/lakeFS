package tree

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
)

// Just a sketch of Apply; it will probably be much harder to write...
func Apply(ctx context.Context, writer Writer, source Iterator, diffs graveler.ValueIterator) error {
	logger := logging.FromContext(ctx)
	sourceDone, diffsDone := !source.Next(), !diffs.Next()
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
				if !source.NextPart() {
					sourceDone = true
				}
			} else {
				// Source at start of part which we need to scan.
				if !source.Next() {
					sourceDone = true
				}
			}
			continue
		}
		c := bytes.Compare(sourceValue.Key, diffValue.Key)
		if c < 0 {
			// select record from source
			if err := writer.WriteRecord(*sourceValue); err != nil {
				return fmt.Errorf("write source record: %w", err)
			}
		} else {
			// select record from diffs, possibly (c==0) overwiting source
			if !diffValue.IsTombstone() {
				if err := writer.WriteRecord(*diffValue); err != nil {
					return fmt.Errorf("write added record: %w", err)
				}
			} else if c > 0 {
				// internal error but no data lost: deletion requested of a
				// file that was not there.
				logger.WithField("id", string(diffValue.Identity)).Warn("[I] unmatched delete")
			}
		}
		if c >= 0 {
			// used up this record from diffs
			if !diffs.Next() {
				diffsDone = true
			}
		}
		if c <= 0 {
			// used up this record from source
			if !source.Next() {
				sourceDone = true
			}
		}
	}
	for !sourceDone {
		sourceValue, sourcePart := source.Value()
		if sourceValue == nil {
			if err := writer.AddParts([]Part{*sourcePart}); err != nil {
				return fmt.Errorf("copy source part %s: %w", sourcePart.Name, err)
			}
			if !source.NextPart() {
				sourceDone = true
			}
		} else {
			if err := writer.WriteRecord(*sourceValue); err != nil {
				return fmt.Errorf("write source record: %w", err)
			}
			if !source.Next() {
				sourceDone = true
			}
		}
	}
	for !diffsDone {
		diffValue := diffs.Value()
		if diffValue.IsTombstone() {
			// internal error but no data lost: deletion requested of a
			// file that was not there.
			logger.WithField("id", string(diffValue.Identity)).Warn("[I] unmatched delete")
			continue
		}
		if err := writer.WriteRecord(*diffValue); err != nil {
			return fmt.Errorf("write added record: %w", err)
		}
		if !diffs.Next() {
			diffsDone = true
		}
	}
	return nil
}
