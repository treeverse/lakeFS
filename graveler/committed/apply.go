package committed

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
)

type ApplyOptions struct {
	// Set to allow commits that change nothing (otherwise ErrNoChanges)
	AllowEmpty bool
}

func addIntoDiffSummary(d *graveler.DiffSummary, typ graveler.DiffType, n int) {
	if d.Count != nil {
		d.Count[typ] += n
	}
}

func incrementDiffSummary(d *graveler.DiffSummary, typ graveler.DiffType) {
	addIntoDiffSummary(d, typ, 1)
}

// ReferenceType represents the type of the reference

// applyFromSource applies all changes from source to writer.
func applyFromSource(ctx context.Context, logger logging.Logger, writer MetaRangeWriter, source Iterator) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		sourceValue, sourceRange := source.Value()
		if sourceValue == nil {
			if logger.IsTracing() {
				logger.WithFields(logging.Fields{
					"from": string(sourceRange.MinKey),
					"to":   string(sourceRange.MaxKey),
					"ID":   sourceRange.ID,
				}).Trace("copy entire source range at end")
			}
			if err := writer.WriteRange(*sourceRange); err != nil {
				return fmt.Errorf("copy source range %s: %w", sourceRange.ID, err)
			}
			if !source.NextRange() {
				break
			}
		} else {
			if logger.IsTracing() {
				logger.WithFields(logging.Fields{
					"key": string(sourceValue.Key),
					"ID":  string(sourceValue.Identity),
				}).Trace("write key from source at end")
			}
			if err := writer.WriteRecord(*sourceValue); err != nil {
				return fmt.Errorf("write source record: %w", err)
			}
			if !source.Next() {
				break
			}
		}
	}
	return source.Err()
}

// applyFromDiffs applies all changes from diffs to writer and returns the number elements it
// added.
func applyFromDiffs(ctx context.Context, logger logging.Logger, writer MetaRangeWriter, diffs graveler.ValueIterator) (int, error) {
	numAdded := 0
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		diffValue, haveDiffs := diffs.Value(), diffs.Next()
		if diffValue.IsTombstone() {
			// internal error but no data lost: deletion requested of a
			// file that was not there.
			logger.WithField("id", string(diffValue.Identity)).Warn("[I] unmatched delete")
			continue
		}
		if logger.IsTracing() {
			logger.WithFields(logging.Fields{
				"key":       string(diffValue.Key),
				"ID":        string(diffValue.Identity),
				"tombstone": diffValue.IsTombstone(),
			}).Trace("write key from diffs at end")
		}
		if err := writer.WriteRecord(*diffValue); err != nil {
			return 0, fmt.Errorf("write added record: %w", err)
		}
		numAdded++
		if !haveDiffs {
			break
		}
	}
	return numAdded, nil
}

func Apply(ctx context.Context, writer MetaRangeWriter, source Iterator, diffs graveler.ValueIterator, opts *ApplyOptions) (graveler.DiffSummary, error) {
	logger := logging.FromContext(ctx)
	ret := graveler.DiffSummary{Count: make(map[graveler.DiffType]int)}
	haveSource, haveDiffs := source.Next(), diffs.Next()
	changed := false
	for haveSource && haveDiffs {
		select {
		case <-ctx.Done():
			return ret, ctx.Err()
		default:
		}

		sourceValue, sourceRange := source.Value()
		diffValue := diffs.Value()
		if sourceValue == nil {
			if bytes.Compare(sourceRange.MaxKey, diffValue.Key) < 0 {
				// Source at start of range which we do not need to scan --
				// write and skip that entire range.
				if logger.IsTracing() {
					logger.WithFields(logging.Fields{
						"from": string(sourceRange.MinKey),
						"to":   string(sourceRange.MaxKey),
						"ID":   sourceRange.ID,
					}).Trace("copy entire source range")
				}

				if err := writer.WriteRange(*sourceRange); err != nil {
					return ret, fmt.Errorf("copy source range %s: %w", sourceRange.ID, err)
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
			if logger.IsTracing() {
				logger.WithFields(logging.Fields{
					"key": string(sourceValue.Key),
					"ID":  string(sourceValue.Identity),
				}).Trace("write key from source")
			}
			if err := writer.WriteRecord(*sourceValue); err != nil {
				return ret, fmt.Errorf("write source record: %w", err)
			}
		} else {
			// select record from diffs, possibly (c==0) overwriting source
			changed = true
			switch {
			case !diffValue.IsTombstone():
				if logger.IsTracing() {
					logger.WithFields(logging.Fields{
						"key":       string(diffValue.Key),
						"ID":        string(diffValue.Identity),
						"tombstone": diffValue.IsTombstone(),
					}).Trace("write key from diffs")
				}
				if err := writer.WriteRecord(*diffValue); err != nil {
					return ret, fmt.Errorf("write added record: %w", err)
				}
				diffType := graveler.DiffTypeAdded
				if c == 0 {
					diffType = graveler.DiffTypeChanged
				}
				incrementDiffSummary(&ret, diffType)
			case c > 0:
				// internal error but no data lost: deletion requested of a
				// file that was not there.
				logger.WithField("id", string(diffValue.Identity)).Warn("[I] unmatched delete")
			default:
				// Delete: simply don't copy to output.
				incrementDiffSummary(&ret, graveler.DiffTypeRemoved)
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
	if err := source.Err(); err != nil {
		return ret, err
	}
	if err := diffs.Err(); err != nil {
		return ret, err
	}
	if haveSource {
		if err := applyFromSource(ctx, logger, writer, source); err != nil {
			return ret, err
		}
	}

	if haveDiffs {
		numAdded, err := applyFromDiffs(ctx, logger, writer, diffs)
		if err != nil {
			return ret, err
		}
		changed = changed || (numAdded > 0)
		addIntoDiffSummary(&ret, graveler.DiffTypeAdded, numAdded)
	}

	if !opts.AllowEmpty && !changed {
		return ret, graveler.ErrNoChanges
	}
	return ret, diffs.Err()
}
