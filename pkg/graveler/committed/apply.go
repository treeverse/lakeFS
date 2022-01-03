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

var ErrInvalidState = errors.New("invalid apply state")

// ReferenceType represents the type of the reference

type applier struct {
	ctx                   context.Context
	logger                logging.Logger
	writer                MetaRangeWriter
	base                  Iterator
	changes               Iterator
	opts                  *ApplyOptions
	summary               graveler.DiffSummary
	haveChanges, haveBase bool
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
	if a.summary.Incomplete {
		// range optimization was used
		return true
	}
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

func (a *applier) setMissingInfo() {
	a.summary.Incomplete = true
}

func (a *applier) incrementDiffSummary(typ graveler.DiffType) {
	a.addIntoDiffSummary(typ, 1)
}

func (a *applier) apply() error {
	a.haveBase, a.haveChanges = a.base.Next(), a.changes.Next()
	for a.haveBase && a.haveChanges {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		default:
		}
		baseValue, baseRange := a.base.Value()
		changeValue, changeRange := a.changes.Value()
		var err error
		switch {
		case changeValue == nil && baseValue == nil:
			err = a.applyBothRanges(changeRange, baseRange)
		case changeValue == nil && baseValue != nil:
			err = a.applyChangeRangeBaseKey(changeRange, baseValue)
		case baseValue == nil && changeValue != nil:
			err = a.applyBaseRangeChangeKey(baseRange, changeValue)
		default:
			err = a.applyBothKeys(baseValue, changeValue)
		}
		if err != nil {
			return err
		}
	}
	if err := a.base.Err(); err != nil {
		return err
	}
	if err := a.changes.Err(); err != nil {
		if errors.Is(err, graveler.ErrConflictFound) {
			a.incrementDiffSummary(graveler.DiffTypeConflict)
		}
		return err
	}
	if a.haveBase {
		if _, err := a.applyAll(a.base); err != nil {
			return err
		}
	}

	if a.haveChanges {
		numAdded, err := a.applyAll(a.changes)
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
	return a.changes.Err()
}

func (a *applier) applyBothKeys(baseValue *graveler.ValueRecord, changeValue *graveler.ValueRecord) error {
	c := bytes.Compare(baseValue.Key, changeValue.Key)
	if c < 0 {
		// select record from base
		if a.logger.IsTracing() {
			a.logger.WithFields(logging.Fields{
				"key": string(baseValue.Key),
				"ID":  string(baseValue.Identity),
			}).Trace("write key from base")
		}
		if err := a.writer.WriteRecord(*baseValue); err != nil {
			return fmt.Errorf("write base record: %w", err)
		}
	} else {
		// select record from changes, possibly (c==0) overwriting base
		switch {
		case !changeValue.IsTombstone():
			if a.logger.IsTracing() {
				a.logger.WithFields(logging.Fields{
					"key":       string(changeValue.Key),
					"ID":        string(changeValue.Identity),
					"tombstone": changeValue.IsTombstone(),
				}).Trace("write key from changes")
			}
			if err := a.writer.WriteRecord(*changeValue); err != nil {
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
			a.logger.WithField("id", string(changeValue.Identity)).Warn("[I] unmatched delete")
		default:
			// Delete: simply don't copy to output.
			a.incrementDiffSummary(graveler.DiffTypeRemoved)
		}
	}
	if c >= 0 {
		// used up this record from changes
		a.haveChanges = a.changes.Next()
	}
	if c <= 0 {
		// used up this record from base
		a.haveBase = a.base.Next()
	}
	return nil
}

func (a *applier) applyBaseRangeChangeKey(baseRange *Range, changeValue *graveler.ValueRecord) error {
	if bytes.Compare(baseRange.MaxKey, changeValue.Key) < 0 {
		// Base at start of range which we do not need to scan --
		// write and skip that entire range.
		if a.logger.IsTracing() {
			a.logger.WithFields(logging.Fields{
				"from": string(baseRange.MinKey),
				"to":   string(baseRange.MaxKey),
				"ID":   baseRange.ID,
			}).Trace("copy entire base range")
		}

		if err := a.writer.WriteRange(*baseRange); err != nil {
			return fmt.Errorf("copy base range %s: %w", baseRange.ID, err)
		}
		a.haveBase = a.base.NextRange()
	} else {
		// Base is at start of range which we need to scan, enter it.
		a.haveBase = a.base.Next()
	}
	return nil
}

func (a *applier) applyChangeRangeBaseKey(changeRange *Range, baseValue *graveler.ValueRecord) error {
	if bytes.Compare(changeRange.MinKey, baseValue.Key) > 0 {
		// base is before change range
		if err := a.writer.WriteRecord(*baseValue); err != nil {
			return fmt.Errorf("write base record: %w", err)
		}
		a.haveBase = a.base.Next()
		return nil
	}
	if bytes.Compare(changeRange.MaxKey, baseValue.Key) >= 0 {
		// changes is at start of range which we need to scan, enter it.
		a.haveChanges = a.changes.Next()
		return nil
	}
	// changes at start of range which was completely added or removed --
	// write and skip that entire range.
	if changeRange.Tombstone {
		a.addIntoDiffSummary(graveler.DiffTypeRemoved, int(changeRange.Count))
	} else {
		if a.logger.IsTracing() {
			a.logger.WithFields(logging.Fields{
				"from": string(changeRange.MinKey),
				"to":   string(changeRange.MaxKey),
				"ID":   changeRange.ID,
			}).Trace("copy entire change range")
		}
		if err := a.writer.WriteRange(*changeRange); err != nil {
			return fmt.Errorf("copy change range %s: %w", changeRange.ID, err)
		}
		a.addIntoDiffSummary(graveler.DiffTypeAdded, int(changeRange.Count))
		a.haveChanges = a.changes.NextRange()
	}
	return nil
}

type RangeCompareSate int

const (
	RangeCompareBefore RangeCompareSate = iota
	RangeCompareAfter
	RangeCompareSameID
	RangeCompareSameBounds
	RangeCompareOverlapping
)

func CompareRanges(rangeA, rangeB *Range) RangeCompareSate {
	switch {
	case rangeA.ID == rangeB.ID:
		return RangeCompareSameID
	case bytes.Compare(rangeA.MaxKey, rangeB.MinKey) < 0:
		return RangeCompareBefore
	case bytes.Compare(rangeB.MaxKey, rangeA.MinKey) < 0:
		return RangeCompareAfter
	case bytes.Equal(rangeA.MinKey, rangeB.MinKey) && bytes.Equal(rangeA.MaxKey, rangeB.MaxKey):
		return RangeCompareSameBounds
	default:
		return RangeCompareOverlapping
	}
}

func (a *applier) applyBothRanges(changeRange *Range, baseRange *Range) error {
	comp := CompareRanges(baseRange, changeRange)
	switch comp {
	case RangeCompareSameID:
		if !changeRange.Tombstone {
			return fmt.Errorf("%w - range already exists in apply base", ErrInvalidState)
		}
		a.addIntoDiffSummary(graveler.DiffTypeRemoved, int(changeRange.Count))
		a.haveBase = a.base.NextRange()
		a.haveChanges = a.changes.NextRange()

	case RangeCompareSameBounds:
		// insert change move both
		if err := a.writer.WriteRange(*changeRange); err != nil {
			return fmt.Errorf("copy change range %s: %w", changeRange.ID, err)
		}
		// When this optimization (inserting the whole range in case base and base are identical) is used. we can't be sure of the diff summary. e.g 4 files added 2 removed or 3 files changed.
		a.setMissingInfo()
		a.haveChanges = a.changes.NextRange()
		a.haveBase = a.base.NextRange()

	case RangeCompareAfter:
		if changeRange.Tombstone {
			// internal error but no data lost: deletion requested of a
			// range that was not there.
			a.logger.WithFields(logging.Fields{
				"from": string(changeRange.MinKey),
				"to":   string(changeRange.MaxKey),
				"ID":   string(changeRange.ID),
			}).Warn("[I] unmatched delete")
		} else {
			// insert change
			if err := a.writer.WriteRange(*changeRange); err != nil {
				return fmt.Errorf("copy change range %s: %w", changeRange.ID, err)
			}
			a.addIntoDiffSummary(graveler.DiffTypeAdded, int(changeRange.Count))
		}
		a.haveChanges = a.changes.NextRange()
	case RangeCompareBefore:
		// insert base
		if err := a.writer.WriteRange(*baseRange); err != nil {
			return fmt.Errorf("copy base range %s: %w", baseRange.ID, err)
		}
		a.haveBase = a.base.NextRange()
	case RangeCompareOverlapping:
		a.haveBase = a.base.Next()
		a.haveChanges = a.changes.Next()
		// enter both
	}
	return nil
}

func Apply(ctx context.Context, writer MetaRangeWriter, base Iterator, changes Iterator, opts *ApplyOptions) (graveler.DiffSummary, error) {
	a := applier{
		ctx:     ctx,
		logger:  logging.FromContext(ctx),
		writer:  writer,
		base:    base,
		changes: changes,
		opts:    opts,
		summary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int)},
	}
	return a.summary, a.apply()
}
