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

func (c *committedManager) GetMetaRange(ns graveler.StorageNamespace, rangeID graveler.RangeID) (graveler.MetaRange, error) {
	panic("implement me")
}

func (c *committedManager) Get(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.RangeID, key graveler.Key) (*graveler.Value, error) {
	panic("implement me")
}

func (c *committedManager) List(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.RangeID) (graveler.ValueIterator, error) {
	panic("implement me")
}

func (c *committedManager) Diff(ctx context.Context, ns graveler.StorageNamespace, left, right graveler.RangeID) (graveler.DiffIterator, error) {
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

func (c *committedManager) Merge(ctx context.Context, ns graveler.StorageNamespace, left, right, base graveler.RangeID, committer string, message string, metadata graveler.Metadata) (graveler.RangeID, error) {
	panic("implement me")
}

func (c *committedManager) Apply(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.RangeID, iterator graveler.ValueIterator) (graveler.RangeID, error) {
	panic("implement me")
}
