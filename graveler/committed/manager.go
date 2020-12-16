package committed

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/tree"
)

type committedManager struct {
	tr tree.Repo
}

func NewCommittedManager(tr tree.Repo) graveler.CommittedManager {
	return &committedManager{tr: tr}
}

func (c *committedManager) Get(ctx context.Context, ns graveler.StorageNamespace, treeID graveler.TreeID, key graveler.Key) (*graveler.Value, error) {
	panic("implement me")
}

func (c *committedManager) List(ctx context.Context, ns graveler.StorageNamespace, treeID graveler.TreeID, from graveler.Key) (graveler.ValueIterator, error) {
	panic("implement me")
}

func (c *committedManager) Diff(ctx context.Context, ns graveler.StorageNamespace, left, right graveler.TreeID, from graveler.Key) (graveler.DiffIterator, error) {
	leftTree, rightTree, err := c.tr.RemoveCommonParts(left, right)
	if err != nil {
		return nil, fmt.Errorf("failed to remove common parts from trees: %w", err)
	}
	leftIt, err := c.tr.NewIteratorFromTree(leftTree, from)
	if err != nil {
		return nil, fmt.Errorf("failed to get iterator for left tree: %w", err)
	}
	rightIt, err := c.tr.NewIteratorFromTree(rightTree, from)
	if err != nil {
		return nil, fmt.Errorf("failed to get iterator for right tree: %w", err)
	}
	return graveler.NewDiffIterator(leftIt, rightIt), nil
}

func (c *committedManager) Merge(ctx context.Context, ns graveler.StorageNamespace, left, right, base graveler.TreeID) (graveler.TreeID, error) {
	panic("implement me")
}

func (c *committedManager) Apply(ctx context.Context, ns graveler.StorageNamespace, treeID graveler.TreeID, iterator graveler.ValueIterator) (graveler.TreeID, error) {
	panic("implement me")
}
