package graveler

import (
	"context"

	"github.com/treeverse/lakefs/graveler/committed/tree"
)

type committedManager struct {
	tr tree.Repo
}

func NewCommittedManager(tr tree.Repo) CommittedManager {
	return &committedManager{tr: tr}
}

func (c *committedManager) Get(ctx context.Context, ns StorageNamespace, treeID TreeID, key Key) (*Value, error) {
	panic("implement me")
}

func (c *committedManager) List(ctx context.Context, ns StorageNamespace, treeID TreeID, from Key) (ValueIterator, error) {
	panic("implement me")
}

func (c *committedManager) Diff(ctx context.Context, ns StorageNamespace, left, right TreeID, from Key) (DiffIterator, error) {
	leftIt, err := c.tr.NewIterator(left, from)
	if err != nil {
		return nil, err
	}
	rightIt, err := c.tr.NewIterator(right, from)
	if err != nil {
		return nil, err
	}
	return NewDiffIterator(leftIt, rightIt), nil
}

func (c *committedManager) Merge(ctx context.Context, ns StorageNamespace, left, right, base TreeID) (TreeID, error) {
	panic("implement me")
}

func (c *committedManager) Apply(ctx context.Context, ns StorageNamespace, treeID TreeID, iterator ValueIterator) (TreeID, error) {
	panic("implement me")
}
