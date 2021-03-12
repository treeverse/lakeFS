package committed

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
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

// applyFromIter applies all changes from Iterator to writer.
// returns the number of writes
func applyFromIter(ctx context.Context, logger logging.Logger, writer MetaRangeWriter, iter Iterator) (int, error) {
	var count int
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		iterValue, iterRange := iter.Value()
		if iterValue == nil {
			if iterRange.IsTombstone() {
				// internal error but no data lost: deletion requested of a
				// file that was not there.
				logger.WithField("id", string(iterRange.ID)).Warn("[I] unmatched delete")
			} else {
				if logger.IsTracing() {
					logger.WithFields(logging.Fields{
						"from": string(iterRange.MinKey),
						"to":   string(iterRange.MaxKey),
						"ID":   iterRange.ID,
					}).Trace("copy entire range at end")
				}
				if err := writer.WriteRange(*iterRange); err != nil {
					return 0, fmt.Errorf("copy iter range %s: %w", iterRange.ID, err)
				}
				count += int(iterRange.Count)
			}
			if !iter.NextRange() {
				break
			}
		} else {
			if logger.IsTracing() {
				if iterValue.IsTombstone() {
					// internal error but no data lost: deletion requested of a
					// file that was not there.
					logger.WithField("id", string(iterValue.Identity)).Warn("[I] unmatched delete")
					continue
				}
				logger.WithFields(logging.Fields{
					"key": string(iterValue.Key),
					"ID":  string(iterValue.Identity),
				}).Trace("write key from iter at end")
			}
			if err := writer.WriteRecord(*iterValue); err != nil {
				return 0, fmt.Errorf("write iter record: %w", err)
			}
			count++
			if !iter.Next() {
				break
			}
		}
	}
	return count, iter.Err()
}

func hasChanges(summary graveler.DiffSummary) bool {
	for _, changes := range summary.Count {
		if changes > 0 {
			return true
		}
	}
	return false
}

func Apply(ctx context.Context, writer MetaRangeWriter, source Iterator, diffs Iterator, opts *ApplyOptions) (graveler.DiffSummary, error) {
	logger := logging.FromContext(ctx)
	ret := graveler.DiffSummary{Count: make(map[graveler.DiffType]int)}
	haveSource, haveDiffs := source.Next(), diffs.Next()
	for haveSource && haveDiffs {
		select {
		case <-ctx.Done():
			return ret, ctx.Err()
		default:
		}
		sourceValue, sourceRange := source.Value()
		diffValue, diffRange := diffs.Value()
		switch {
		case diffValue == nil && sourceValue == nil:
			switch {
			case bytes.Compare(diffRange.MaxKey, sourceRange.MinKey) < 0:
				// insert diff
				writer.WriteRange(*diffRange)
				addIntoDiffSummary(&ret, graveler.DiffTypeAdded, int(diffRange.Count))
				haveDiffs = diffs.Next()
			case bytes.Compare(sourceRange.MaxKey, diffRange.MinKey) < 0:
				//insert source
				writer.WriteRange(*sourceRange)
				haveSource = source.Next()
			case diffRange.ID == sourceRange.ID && diffRange.IsTombstone():
				addIntoDiffSummary(&ret, graveler.DiffTypeRemoved, int(diffRange.Count))
				haveSource = source.Next()
				haveDiffs = diffs.Next()
			default:
				haveSource = source.Next()
				haveDiffs = diffs.Next()
				// enter both
			}
		case diffValue == nil && sourceValue != nil:
			if bytes.Compare(diffRange.MaxKey, sourceValue.Key) < 0 {
				// diffs at start of range which was completely added or removed --
				// write and skip that entire range.
				if diffRange.IsTombstone() {
					addIntoDiffSummary(&ret, graveler.DiffTypeRemoved, int(diffRange.Count))
				} else {
					if logger.IsTracing() {
						logger.WithFields(logging.Fields{
							"from": string(diffRange.MinKey),
							"to":   string(diffRange.MaxKey),
							"ID":   diffRange.ID,
						}).Trace("copy entire diff range")
					}
					if err := writer.WriteRange(*diffRange); err != nil {
						return ret, fmt.Errorf("copy diff range %s: %w", sourceRange.ID, err)
					}
					haveSource = diffs.NextRange()
				}
			} else {
				// diffs is at start of range which we need to scan, enter it.
				haveDiffs = diffs.Next()
			}
		case sourceValue == nil && diffValue != nil:
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
		default:
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
	}
	if err := source.Err(); err != nil {
		return ret, err
	}
	if err := diffs.Err(); err != nil {
		return ret, err
	}
	if haveSource {
		if _, err := applyFromIter(ctx, logger, writer, source); err != nil {
			return ret, err
		}
	}

	if haveDiffs {
		numAdded, err := applyFromIter(ctx, logger, writer, diffs)
		if err != nil {
			return ret, err
		}
		addIntoDiffSummary(&ret, graveler.DiffTypeAdded, numAdded)
	}

	if !opts.AllowEmpty && !hasChanges(ret) {
		return ret, graveler.ErrNoChanges
	}
	return ret, diffs.Err()
}
