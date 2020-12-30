package committed

import (
	"context"
	"fmt"

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

func (c *committedManager) WriteMetaRange(ctx context.Context, ns graveler.StorageNamespace, it graveler.ValueIterator) (*graveler.MetaRangeID, error) {
	writer := c.metaRangeManager.NewWriter(ns)
	for it.Next() {
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("getting value from iterator: %w", err)
		}
		if err := writer.WriteRecord(*it.Value()); err != nil {
			return nil, fmt.Errorf("writing record: %w", err)
		}
	}
	id, err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("closing writer: %w", err)
	}

	return id, nil
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
