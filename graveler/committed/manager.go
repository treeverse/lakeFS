package committed

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/graveler"
)

type committedManager struct {
	metaRangeManager MetaRangeManager
	logger           logging.Logger
}

func NewCommittedManager(m MetaRangeManager) graveler.CommittedManager {
	return &committedManager{metaRangeManager: m, logger: logging.Default()}
}

func (c *committedManager) Exists(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID) (bool, error) {
	return c.metaRangeManager.Exists(ctx, ns, id)
}

func (c *committedManager) Get(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID, key graveler.Key) (*graveler.Value, error) {
	it, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, rangeID)
	if err != nil {
		return nil, err
	}
	valIt := NewValueIterator(it)
	defer valIt.Close()
	valIt.SeekGE(key)
	// return the next value
	if !valIt.Next() {
		// error or not found
		if err := valIt.Err(); err != nil {
			return nil, err
		}
		return nil, graveler.ErrNotFound
	}
	// compare the key we found
	rec := valIt.Value()
	if !bytes.Equal(rec.Key, key) {
		return nil, graveler.ErrNotFound
	}
	return rec.Value, nil
}

func (c *committedManager) List(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID) (graveler.ValueIterator, error) {
	it, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, rangeID)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(it), nil
}

func (c *committedManager) WriteMetaRange(ctx context.Context, ns graveler.StorageNamespace, it graveler.ValueIterator) (*graveler.MetaRangeID, error) {
	writer := c.metaRangeManager.NewWriter(ctx, ns)
	defer func() {
		if err := writer.Abort(); err != nil {
			c.logger.Errorf("Aborting write to meta range: %w", err)
		}
	}()

	for it.Next() {
		if err := writer.WriteRecord(*it.Value()); err != nil {
			return nil, fmt.Errorf("writing record: %w", err)
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("getting value from iterator: %w", err)
	}
	id, err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("closing writer: %w", err)
	}

	return id, nil
}

func (c *committedManager) Diff(ctx context.Context, ns graveler.StorageNamespace, left, right graveler.MetaRangeID) (graveler.DiffIterator, error) {
	leftIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, left)
	if err != nil {
		return nil, err
	}
	rightIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, right)
	if err != nil {
		return nil, err
	}
	return NewDiffIterator(leftIt, rightIt), nil
}

func (c *committedManager) Merge(ctx context.Context, ns graveler.StorageNamespace, theirs, ours, base graveler.MetaRangeID) (graveler.MetaRangeID, error) {
	diffIt, err := c.Diff(ctx, ns, theirs, ours)
	if err != nil {
		return "", fmt.Errorf("diff: %w", err)
	}
	defer diffIt.Close()
	baseIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, base)
	if err != nil {
		return "", fmt.Errorf("get base iterator: %w", err)
	}
	defer baseIt.Close()
	patchIterator := NewMergeIterator(diffIt, baseIt)
	defer patchIterator.Close()
	return c.Apply(ctx, ns, theirs, patchIterator)
}

func (c *committedManager) Apply(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID, diffs graveler.ValueIterator) (graveler.MetaRangeID, error) {
	mwWriter := c.metaRangeManager.NewWriter(ctx, ns)
	defer func() {
		err := mwWriter.Abort()
		if err != nil {
			c.logger.WithError(err).Error("Abort failed after Apply")
		}
	}()
	metaRangeIterator, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, rangeID)
	if err != nil {
		return "", fmt.Errorf("get metarange ns=%s id=%s: %w", ns, rangeID, err)
	}
	defer metaRangeIterator.Close()
	err = Apply(ctx, mwWriter, metaRangeIterator, diffs)
	if err != nil {
		return "", fmt.Errorf("apply ns=%s id=%s: %w", ns, rangeID, err)
	}
	newID, err := mwWriter.Close()
	if newID == nil {
		return "", fmt.Errorf("close writer ns=%s id=%s: %w", ns, rangeID, err)
	}
	return *newID, err
}

func (c *committedManager) Compare(ctx context.Context, ns graveler.StorageNamespace, theirs, ours, base graveler.MetaRangeID) (graveler.DiffIterator, error) {
	diffIt, err := c.Diff(ctx, ns, theirs, ours)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}
	baseIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, base)
	if err != nil {
		return nil, fmt.Errorf("get base iterator: %w", err)
	}
	return NewCompareIterator(diffIt, baseIt), nil
}
