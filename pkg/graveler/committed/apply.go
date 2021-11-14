package committed

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type ApplyOptions struct {
	// Set to allow commits that change nothing (otherwise ErrNoChanges)
	AllowEmpty bool
}

// ReferenceType represents the type of the reference

type applier struct {
	ctx                   context.Context
	logger                logging.Logger
	writer                MetaRangeWriter
	source                Iterator
	diffs                 Iterator
	opts                  *ApplyOptions
	summary               graveler.DiffSummary
	haveDiffs, haveSource bool
}

// applyAll applies all changes from Iterator to writer and returns the number of writes
func (a *applier) applyAll(iter Iterator) (int, error) {
	var count int
	for {
		select {
		case <-a.ctx.Done():
			return 0, a.ctx.Err()
		default:
		}
		iterValue, iterRange := iter.Value()
		if iterValue == nil {
			if iterRange.Tombstone {
				// internal error but no data lost: deletion requested of a
				// file that was not there.
				a.logger.WithFields(logging.Fields{
					"from": string(iterRange.MinKey),
					"to":   string(iterRange.MaxKey),
					"ID":   string(iterRange.ID),
				}).Warn("[I] unmatched delete")
			} else {
				if a.logger.IsTracing() {
					a.logger.WithFields(logging.Fields{
						"from": string(iterRange.MinKey),
						"to":   string(iterRange.MaxKey),
						"ID":   iterRange.ID,
					}).Trace("copy entire range at end")
				}
				if err := a.writer.WriteRange(*iterRange); err != nil {
					return 0, fmt.Errorf("copy iter range %s: %w", iterRange.ID, err)
				}
				count += int(iterRange.Count)
			}
			if !iter.NextRange() {
				break
			}
		} else {
			if a.logger.IsTracing() {
				if iterValue.IsTombstone() {
					// internal error but no data lost: deletion requested of a
					// file that was not there.
					a.logger.WithField("id", string(iterValue.Identity)).Warn("[I] unmatched delete")
					continue
				}
				a.logger.WithFields(logging.Fields{
					"key": string(iterValue.Key),
					"ID":  string(iterValue.Identity),
				}).Trace("write key from iter at end")
			}
			if err := a.writer.WriteRecord(*iterValue); err != nil {
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

func (a *applier) hasChanges(summary graveler.DiffSummary) bool {
	for _, changes := range summary.Count {
		if changes > 0 {
			return true
		}
	}
	return false
}

func (a *applier) addIntoDiffSummary(typ graveler.DiffType, n int) {
	if a.summary.Count != nil {
		a.summary.Count[typ] += n
	}
}

func (a *applier) incrementDiffSummary(typ graveler.DiffType) {
	a.addIntoDiffSummary(typ, 1)
}

func (a *applier) apply() error {
	a.haveSource, a.haveDiffs = a.source.Next(), a.diffs.Next()
	for a.haveSource && a.haveDiffs {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		default:
		}
		sourceValue, sourceRange := a.source.Value()
		diffValue, diffRange := a.diffs.Value()
		var err error
		switch {
		case diffValue == nil && sourceValue == nil:
			err = a.applyBothRanges(diffRange, sourceRange)
		case diffValue == nil && sourceValue != nil:
			err = a.applyDiffRangeSourceKey(diffRange, sourceValue)
		case sourceValue == nil && diffValue != nil:
			err = a.applySourceRangeDiffKey(sourceRange, diffValue)
		default:
			err = a.applyBothKeys(sourceValue, diffValue)
		}
		if err != nil {
			return err
		}
	}
	if err := a.source.Err(); err != nil {
		return err
	}
	if err := a.diffs.Err(); err != nil {
		if errors.Is(err, graveler.ErrConflictFound) {
			a.incrementDiffSummary(graveler.DiffTypeConflict)
		}
		return err
	}
	if a.haveSource {
		if _, err := a.applyAll(a.source); err != nil {
			return err
		}
	}

	if a.haveDiffs {
		numAdded, err := a.applyAll(a.diffs)
		if err != nil {
			return err
		}
		if numAdded > 0 {
			a.addIntoDiffSummary(graveler.DiffTypeAdded, numAdded)
		}
	}

	if !a.opts.AllowEmpty && !a.hasChanges(a.summary) {
		return graveler.ErrNoChanges
	}
	return a.diffs.Err()
}

func (a *applier) applyBothKeys(sourceValue *graveler.ValueRecord, diffValue *graveler.ValueRecord) error {
	c := bytes.Compare(sourceValue.Key, diffValue.Key)
	if c < 0 {
		// select record from source
		if a.logger.IsTracing() {
			a.logger.WithFields(logging.Fields{
				"key": string(sourceValue.Key),
				"ID":  string(sourceValue.Identity),
			}).Trace("write key from source")
		}
		if err := a.writer.WriteRecord(*sourceValue); err != nil {
			return fmt.Errorf("write source record: %w", err)
		}
	} else {
		// select record from diffs, possibly (c==0) overwriting source
		switch {
		case !diffValue.IsTombstone():
			if a.logger.IsTracing() {
				a.logger.WithFields(logging.Fields{
					"key":       string(diffValue.Key),
					"ID":        string(diffValue.Identity),
					"tombstone": diffValue.IsTombstone(),
				}).Trace("write key from diffs")
			}
			if err := a.writer.WriteRecord(*diffValue); err != nil {
				return fmt.Errorf("write added record: %w", err)
			}
			diffType := graveler.DiffTypeAdded
			if c == 0 {
				diffType = graveler.DiffTypeChanged
			}
			a.incrementDiffSummary(diffType)
		case c > 0:
			// internal error but no data lost: deletion requested of a
			// file that was not there.
			a.logger.WithField("id", string(diffValue.Identity)).Warn("[I] unmatched delete")
		default:
			// Delete: simply don't copy to output.
			a.incrementDiffSummary(graveler.DiffTypeRemoved)
		}
	}
	if c >= 0 {
		// used up this record from diffs
		a.haveDiffs = a.diffs.Next()
	}
	if c <= 0 {
		// used up this record from source
		a.haveSource = a.source.Next()
	}
	return nil
}

func (a *applier) applySourceRangeDiffKey(sourceRange *Range, diffValue *graveler.ValueRecord) error {
	if bytes.Compare(sourceRange.MaxKey, diffValue.Key) < 0 {
		// Source at start of range which we do not need to scan --
		// write and skip that entire range.
		if a.logger.IsTracing() {
			a.logger.WithFields(logging.Fields{
				"from": string(sourceRange.MinKey),
				"to":   string(sourceRange.MaxKey),
				"ID":   sourceRange.ID,
			}).Trace("copy entire source range")
		}

		if err := a.writer.WriteRange(*sourceRange); err != nil {
			return fmt.Errorf("copy source range %s: %w", sourceRange.ID, err)
		}
		a.haveSource = a.source.NextRange()
	} else {
		// Source is at start of range which we need to scan, enter it.
		a.haveSource = a.source.Next()
	}
	return nil
}

func (a *applier) applyDiffRangeSourceKey(diffRange *Range, sourceValue *graveler.ValueRecord) error {
	if bytes.Compare(diffRange.MaxKey, sourceValue.Key) >= 0 {
		// diffs is at start of range which we need to scan, enter it.
		a.haveDiffs = a.diffs.Next()
		return nil
	}
	// diffs at start of range which was completely added or removed --
	// write and skip that entire range.
	if diffRange.Tombstone {
		a.addIntoDiffSummary(graveler.DiffTypeRemoved, int(diffRange.Count))
	} else {
		if a.logger.IsTracing() {
			a.logger.WithFields(logging.Fields{
				"from": string(diffRange.MinKey),
				"to":   string(diffRange.MaxKey),
				"ID":   diffRange.ID,
			}).Trace("copy entire diff range")
		}
		if err := a.writer.WriteRange(*diffRange); err != nil {
			return fmt.Errorf("copy diff range %s: %w", diffRange.ID, err)
		}
		a.addIntoDiffSummary(graveler.DiffTypeAdded, int(diffRange.Count))
		a.haveDiffs = a.diffs.NextRange()
	}
	return nil
}

func (a *applier) applyBothRanges(diffRange *Range, sourceRange *Range) error {
	switch {
	case bytes.Compare(diffRange.MaxKey, sourceRange.MinKey) < 0 && diffRange.Tombstone:
		// internal error but no data lost: deletion requested of a
		// range that was not there.
		a.logger.WithFields(logging.Fields{
			"from": string(diffRange.MinKey),
			"to":   string(diffRange.MaxKey),
			"ID":   string(diffRange.ID),
		}).Warn("[I] unmatched delete")
	case bytes.Compare(diffRange.MaxKey, sourceRange.MinKey) < 0:
		// insert diff
		if err := a.writer.WriteRange(*diffRange); err != nil {
			return fmt.Errorf("copy diff range %s: %w", diffRange.ID, err)
		}
		a.addIntoDiffSummary(graveler.DiffTypeAdded, int(diffRange.Count))
		a.haveDiffs = a.diffs.NextRange()
	case bytes.Compare(sourceRange.MaxKey, diffRange.MinKey) < 0:
		// insert source
		if err := a.writer.WriteRange(*sourceRange); err != nil {
			return fmt.Errorf("copy source range %s: %w", sourceRange.ID, err)
		}
		a.haveSource = a.source.NextRange()
	case diffRange.ID == sourceRange.ID && diffRange.Tombstone:
		a.addIntoDiffSummary(graveler.DiffTypeRemoved, int(diffRange.Count))
		a.haveSource = a.source.NextRange()
		a.haveDiffs = a.diffs.NextRange()
	default:
		a.haveSource = a.source.Next()
		a.haveDiffs = a.diffs.Next()
		// enter both
	}
	return nil
}

func Apply(ctx context.Context, writer MetaRangeWriter, source Iterator, diffs Iterator, opts *ApplyOptions) (graveler.DiffSummary, error) {
	a := applier{
		ctx:     ctx,
		logger:  logging.FromContext(ctx),
		writer:  writer,
		source:  source,
		diffs:   diffs,
		opts:    opts,
		summary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int)},
	}
	return a.summary, a.apply()
}
