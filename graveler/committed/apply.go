package committed

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
)

func Apply(ctx context.Context, writer MetaRangeWriter, source Iterator, diffs graveler.ValueIterator) error {
	logger := logging.FromContext(ctx)
	haveSource, haveDiffs := source.Next(), diffs.Next()
	for haveSource && haveDiffs {
		sourceValue, sourceRange := source.Value()
		diffValue := diffs.Value()
		if sourceValue == nil {
			if bytes.Compare(sourceRange.MaxKey, diffValue.Key) < 0 {
				// Source at start of range which we do not need to scan --
				// write and skip that entire range.
				logger.WithFields(logging.Fields{
					"from": string(sourceRange.MinKey),
					"to":   string(sourceRange.MaxKey),
					"ID":   sourceRange.ID,
				}).Trace("copy entire source range")

				if err := writer.WriteRange(*sourceRange); err != nil {
					return fmt.Errorf("copy source range %s: %w", sourceRange.ID, err)
				}
				haveSource = source.NextRange()
			} else {
				// Source is at start of range which we need to scan, enter it.
				haveSource = source.Next()
			}
			continue
		}
		c := bytes.Compare(sourceValue.Key, diffValue.Key)
		if c < 0 {
			// select record from source
			logger.WithFields(logging.Fields{
				"key": string(sourceValue.Key),
				"ID":  string(sourceValue.Identity),
			}).Trace("write key from source")
			if err := writer.WriteRecord(*sourceValue); err != nil {
				return fmt.Errorf("write source record: %w", err)
			}
		} else {
			// select record from diffs, possibly (c==0) overwriting source
			if !diffValue.IsTombstone() {
				logger.WithFields(logging.Fields{
					"key":       string(diffValue.Key),
					"ID":        string(diffValue.Identity),
					"tombstone": diffValue.IsTombstone(),
				}).Trace("write key from diffs")
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
		sourceValue, sourceRange := source.Value()
		if sourceValue == nil {
			logger.WithFields(logging.Fields{
				"from": string(sourceRange.MinKey),
				"to":   string(sourceRange.MaxKey),
				"ID":   sourceRange.ID,
			}).Trace("copy entire source range at end")
			if err := writer.WriteRange(*sourceRange); err != nil {
				return fmt.Errorf("copy source range %s: %w", sourceRange.ID, err)
			}
			haveSource = source.NextRange()
		} else {
			logger.WithFields(logging.Fields{
				"key": string(sourceValue.Key),
				"ID":  string(sourceValue.Identity),
			}).Trace("write key from source at end")
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
		logger.WithFields(logging.Fields{
			"key":       string(diffValue.Key),
			"ID":        string(diffValue.Identity),
			"tombstone": diffValue.IsTombstone(),
		}).Trace("write key from diffs at end")
		if err := writer.WriteRecord(*diffValue); err != nil {
			return fmt.Errorf("write added record: %w", err)
		}
	}
	return nil
}
