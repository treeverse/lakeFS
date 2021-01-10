package committed

import (
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

func (c *committedManager) Exists(ns graveler.StorageNamespace, id graveler.MetaRangeID) (bool, error) {
	return c.metaRangeManager.Exists(ns, id)
}

func (c *committedManager) Get(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID, key graveler.Key) (*graveler.Value, error) {
	panic("implement me")
}

func (c *committedManager) List(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID) (graveler.ValueIterator, error) {
	panic("implement me")
}

func (c *committedManager) WriteMetaRange(ctx context.Context, ns graveler.StorageNamespace, it graveler.ValueIterator) (*graveler.MetaRangeID, error) {
	writer := c.metaRangeManager.NewWriter(ns)
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
	leftIt, err := c.metaRangeManager.NewMetaRangeIterator(ns, left)
	if err != nil {
		return nil, err
	}
	rightIt, err := c.metaRangeManager.NewMetaRangeIterator(ns, right)
	if err != nil {
		return nil, err
	}
	return NewDiffIterator(leftIt, rightIt), nil
}

func (c *committedManager) Merge(ctx context.Context, ns graveler.StorageNamespace, left, right, base graveler.MetaRangeID, committer string, message string, metadata graveler.Metadata) (graveler.MetaRangeID, error) {
	panic("implement me")
}

func (c *committedManager) Apply(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID, diffs graveler.ValueIterator) (graveler.MetaRangeID, error) {
	mwWriter := c.metaRangeManager.NewWriter(ns)
	defer func() {
		err := mwWriter.Abort()
		if err != nil {
			c.logger.WithError(err).Error("Abort failed after Apply")
		}
	}()
	metaRangeIterator, err := c.metaRangeManager.NewMetaRangeIterator(ns, rangeID)
	if err != nil {
		return "", fmt.Errorf("get metarange ns=%s id=%s: %w", ns, rangeID, err)
	}
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
