package factory

import (
	"context"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

type EmptyEntryConflictsResolver struct {
}

func (r *EmptyEntryConflictsResolver) ResolveConflict(ctx context.Context, oCtx graveler.ObjectContext, strategy graveler.MergeStrategy, sourceEntry, destEntry *catalog.DBEntry) (*catalog.DBEntry, error) {
	return nil, nil
}

func BuildEntryConflictsResolver(block block.Adapter) catalog.EntryConflictsResolver {
	return &EmptyEntryConflictsResolver{}
}
