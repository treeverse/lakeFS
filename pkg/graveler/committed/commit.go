package committed

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type CommitOptions struct {
	// Set to allow commits that change nothing (otherwise ErrNoChanges)
	AllowEmpty bool
}

type committer struct {
	ctx    context.Context
	logger logging.Logger

	writer                MetaRangeWriter
	base                  Iterator
	changes               graveler.ValueIterator
	opts                  *CommitOptions
	summary               graveler.DiffSummary
	haveChanges, haveBase bool
}

// applyAllBase writes all remaining changes from Base Iterator to writer
func (a *committer) applyAllBase(iter Iterator) error {
	for {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		default:
		}
		iterValue, iterRange := iter.Value()
		if iterValue == nil {
			if a.logger.IsTracing() {
				a.logger.WithFields(logging.Fields{
					"from": string(iterRange.MinKey),
					"to":   string(iterRange.MaxKey),
					"ID":   iterRange.ID,
				}).Trace("copy entire range at end")
			}
			if err := a.writer.WriteRange(*iterRange); err != nil {
				return fmt.Errorf("copy iter range %s: %w", iterRange.ID, err)
			}
			if !iter.NextRange() {
				break
			}
		} else {
			if a.logger.IsTracing() {
				a.logger.WithFields(logging.Fields{
					"key": string(iterValue.Key),
					"ID":  string(iterValue.Identity),
				}).Trace("write key from iter at end")
			}
			if err := a.writer.WriteRecord(*iterValue); err != nil {
				return fmt.Errorf("write iter record: %w", err)
			}
			if !iter.Next() {
				break
			}
		}
	}
	return iter.Err()
}

// applyAllChanges writes all remaining changes from Changes Iterator to writer and returns the number of writes
func (a *committer) applyAllChanges(iter graveler.ValueIterator) (int, error) {
	var count int
	for {
		select {
		case <-a.ctx.Done():
			return 0, a.ctx.Err()
		default:
		}
		iterValue := iter.Value()
		if iterValue.IsTombstone() {
			// internal error but no data lost: deletion requested of a
			// file that was not there.
			if a.logger.IsTracing() {
				a.logger.WithField("id", string(iterValue.Identity)).Warn("[I] unmatched delete")
			}
		} else {
			if a.logger.IsTracing() {
				a.logger.WithFields(logging.Fields{
					"key": string(iterValue.Key),
					"ID":  string(iterValue.Identity),
				}).Trace("write key from iter at end")
			}
			if err := a.writer.WriteRecord(*iterValue); err != nil {
				return 0, fmt.Errorf("write iter record: %w", err)
			}
			count++
		}
		if !iter.Next() {
			break
		}
	}
	return count, iter.Err()
}

func (a *committer) hasChanges(summary graveler.DiffSummary) bool {
	for _, changes := range summary.Count {
		if changes > 0 {
			return true
		}
	}
	return false
}

func (a *committer) addIntoDiffSummary(typ graveler.DiffType, n int) {
	a.summary.Count[typ] += n
}

func (a *committer) incrementDiffSummary(typ graveler.DiffType) {
	a.addIntoDiffSummary(typ, 1)
}

func (a *committer) applyBaseRange(baseRange *Range, changeValue *graveler.ValueRecord) error {
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

func (a *committer) applyNextKey(baseValue *graveler.ValueRecord, changeValue *graveler.ValueRecord) error {
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
			if changeValue != nil {
				a.logger.WithField("key", string(changeValue.Key)).Warn("[I] unmatched delete")
			}
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

func (a *committer) commit() error {
	a.haveBase, a.haveChanges = a.base.Next(), a.changes.Next()
	for a.haveBase && a.haveChanges {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		default:
		}
		baseValue, baseRange := a.base.Value()
		changeValue := a.changes.Value()
		var err error
		if baseValue == nil {
			err = a.applyBaseRange(baseRange, changeValue)
		} else {
			err = a.applyNextKey(baseValue, changeValue)
		}
		if err != nil {
			return err
		}
	}
	if err := a.base.Err(); err != nil {
		return err
	}
	if err := a.changes.Err(); err != nil {
		return err
	}
	if a.haveBase {
		if err := a.applyAllBase(a.base); err != nil {
			return err
		}
	}

	if a.haveChanges {
		numAdded, err := a.applyAllChanges(a.changes)
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

func Commit(ctx context.Context, writer MetaRangeWriter, base Iterator, changes graveler.ValueIterator, opts *CommitOptions) (graveler.DiffSummary, error) {
	c := committer{
		ctx:     ctx,
		logger:  logging.FromContext(ctx),
		writer:  writer,
		base:    base,
		changes: changes,
		opts:    opts,
		summary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int)},
	}
	return c.summary, c.commit()
}
