package diff

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
)

type diffManager struct {
	metaRangeManager committed.MetaRangeManager
}

func NewManager(metaRangeManager committed.MetaRangeManager) *diffManager {
	return &diffManager{metaRangeManager: metaRangeManager}
}

func (c *diffManager) Diff(ctx context.Context, ns graveler.StorageNamespace, left, right graveler.MetaRangeID) (graveler.DiffIterator, error) {
	leftIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, left)
	if err != nil {
		return nil, err
	}
	rightIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, right)
	if err != nil {
		return nil, err
	}
	return NewDiffValueIterator(ctx, leftIt, rightIt), nil
}

func (c *diffManager) DiffWithRanges(ctx context.Context, ns graveler.StorageNamespace, left, right graveler.MetaRangeID) (committed.DiffIterator, error) {
	leftIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, left)
	if err != nil {
		return nil, err
	}
	rightIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, right)
	if err != nil {
		return nil, err
	}
	return NewDiffIterator(ctx, leftIt, rightIt), nil
}

func (c *diffManager) Compare(ctx context.Context, ns graveler.StorageNamespace, destination, source, base graveler.MetaRangeID) (graveler.DiffIterator, error) {
	diffIt, err := c.Diff(ctx, ns, destination, source)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}
	baseIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, base)
	if err != nil {
		return nil, fmt.Errorf("get base iterator: %w", err)
	}
	return committed.NewCompareValueIterator(ctx, committed.NewDiffIteratorWrapper(diffIt), baseIt), nil
}
