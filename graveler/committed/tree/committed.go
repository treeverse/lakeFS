package tree

import (
	"context"

	"github.com/treeverse/lakefs/graveler"
)

type committedManager struct {
	tr Repo
}

func NewCommittedManager(tr Repo) graveler.CommittedManager {
	return &committedManager{tr: tr}
}

func (c *committedManager) Get(ctx context.Context, ns graveler.StorageNamespace, treeID graveler.TreeID, key graveler.Key) (*graveler.Value, error) {
	panic("implement me")
}

func (c *committedManager) List(ctx context.Context, ns graveler.StorageNamespace, treeID graveler.TreeID, from graveler.Key) (graveler.ValueIterator, error) {
	panic("implement me")
}

func (c *committedManager) Diff(ctx context.Context, ns graveler.StorageNamespace, left, right graveler.TreeID, from graveler.Key) (graveler.DiffIterator, error) {
	leftIt, err := c.tr.NewIterator(left, from)
	if err != nil {
		return nil, err
	}
	rightIt, err := c.tr.NewIterator(right, from)
	if err != nil {
		return nil, err
	}
	return graveler.NewDiffIterator(leftIt, rightIt), nil
}

func (c *committedManager) Merge(ctx context.Context, ns graveler.StorageNamespace, left, right, base graveler.TreeID) (graveler.TreeID, error) {
	panic("implement me")
}

func (c *committedManager) Apply(ctx context.Context, ns graveler.StorageNamespace, treeID graveler.TreeID, iterator graveler.ValueIterator) (graveler.TreeID, error) {
	panic("implement me")
}
