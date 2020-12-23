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
	haveSource, haveDiffs := source.Next(), diffs.Next()
	for haveSource && haveDiffs {
		sourceValue, sourcePart := source.Value()
		diffValue := diffs.Value()
		if sourceValue == nil {
			if bytes.Compare(sourcePart.MaxKey, diffValue.Key) < 0 {
				// Source at start of part which we do not need to scan --
				// write and skip that entire part.
				if err := writer.AddPart(*sourcePart); err != nil {
					return fmt.Errorf("copy source part %s: %w", sourcePart.Name, err)
				}
				haveSource = source.NextPart()
			} else {
				// Source is at start of part which we need to scan, enter it.
				haveSource = source.Next()
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
			// select record from diffs, possibly (c==0) overwriting source
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
			haveDiffs = diffs.Next()
		}
		if c <= 0 {
			// used up this record from source
			haveSource = source.Next()
		}
	}
	for haveSource {
		sourceValue, sourcePart := source.Value()
		if sourceValue == nil {
			if err := writer.AddPart(*sourcePart); err != nil {
				return fmt.Errorf("copy source part %s: %w", sourcePart.Name, err)
			}
			haveSource = source.NextPart()
		} else {
			if err := writer.WriteRecord(*sourceValue); err != nil {
				return fmt.Errorf("write source record: %w", err)
			}
			haveSource = source.Next()
		}
	}
	for haveDiffs {
		diffValue := diffs.Value()
		haveDiffs = diffs.Next()
		if diffValue.IsTombstone() {
			// internal error but no data lost: deletion requested of a
			// file that was not there.
			logger.WithField("id", string(diffValue.Identity)).Warn("[I] unmatched delete")
			continue
		}
		if err := writer.WriteRecord(*diffValue); err != nil {
			return fmt.Errorf("write added record: %w", err)
		}
	}
	return nil
}
