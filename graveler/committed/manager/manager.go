package manager

import (
	"context"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/tree"
)

type committedManager struct {
	repoProvider tree.RepoProvider
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
	repo := c.repoProvider.GetRepo(ns)
	leftIt, err := repo.NewIterator(left, nil)
	if err != nil {
		return nil, err
	}
	rightIt, err := repo.NewIterator(right, nil)
	if err != nil {
		return nil, err
	}
	return tree.NewDiffIterator(leftIt, rightIt), nil
}

func (c *committedManager) Merge(ctx context.Context, ns graveler.StorageNamespace, left, right, base graveler.TreeID, committer string, message string, metadata graveler.Metadata) (graveler.TreeID, error) {
	panic("implement me")
}

func (c *committedManager) Apply(ctx context.Context, ns graveler.StorageNamespace, treeID graveler.TreeID, iterator graveler.ValueIterator) (graveler.TreeID, error) {
	panic("implement me")
}
