package committed

import (
	"context"

	"github.com/treeverse/lakefs/graveler"
)

type committedManager struct {
	metaRangeManager MetaRangeManager
}

func NewCommittedManager() graveler.CommittedManager {
	return &committedManager{}
}

func (c *committedManager) GetMetaRange(ns graveler.StorageNamespace, rangeID graveler.MetaRangeID) (graveler.MetaRange, error) {
	panic("implement me")
}

func (c *committedManager) Get(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID, key graveler.Key) (*graveler.Value, error) {
	panic("implement me")
}

func (c *committedManager) List(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID) (graveler.ValueIterator, error) {
	panic("implement me")
}

func (c *committedManager) Diff(ctx context.Context, ns graveler.StorageNamespace, left, right graveler.MetaRangeID) (graveler.DiffIterator, error) {
	leftIt, err := c.metaRangeManager.NewMetaRangeIterator(ns, left, nil)
	if err != nil {
		return nil, err
	}
	rightIt, err := c.metaRangeManager.NewMetaRangeIterator(ns, right, nil)
	if err != nil {
		return nil, err
	}
	return NewDiffIterator(leftIt, rightIt), nil
}

func (c *committedManager) Merge(ctx context.Context, ns graveler.StorageNamespace, left, right, base graveler.MetaRangeID, committer string, message string, metadata graveler.Metadata) (graveler.MetaRangeID, error) {
	panic("implement me")
}

func (c *committedManager) Apply(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID, iterator graveler.ValueIterator) (graveler.MetaRangeID, error) {
	panic("implement me")
}
