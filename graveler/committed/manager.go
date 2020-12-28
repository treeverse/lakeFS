package committed

import (
	"context"

	"github.com/treeverse/lakefs/graveler"
)

type committedManager struct {
}

func NewCommittedManager() graveler.CommittedManager {
	return &committedManager{}
}

func (c *committedManager) Get(ctx context.Context, ns graveler.StorageNamespace, treeID graveler.TreeID, key graveler.Key) (*graveler.Value, error) {
	panic("implement me")
}

func (c *committedManager) List(ctx context.Context, ns graveler.StorageNamespace, treeID graveler.TreeID) (graveler.ValueIterator, error) {
	panic("implement me")
}

func (c *committedManager) Diff(ctx context.Context, ns graveler.StorageNamespace, left, right graveler.TreeID) (graveler.DiffIterator, error) {
	return nil, nil
}

func (c *committedManager) Merge(ctx context.Context, ns graveler.StorageNamespace, left, right, base graveler.TreeID, committer string, message string, metadata graveler.Metadata) (graveler.TreeID, error) {
	panic("implement me")
}

func (c *committedManager) Apply(ctx context.Context, ns graveler.StorageNamespace, treeID graveler.TreeID, iterator graveler.ValueIterator) (graveler.TreeID, error) {
	panic("implement me")
}
