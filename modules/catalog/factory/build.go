package factory

import (
	"context"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
)

func BuildConflictResolvers(ctx context.Context, cfg config.Config, block block.Adapter) []graveler.ConflictResolver {
	return nil
}

func BuildExtendedOperations(_ *catalog.Catalog) catalog.ExtendedOperations {
	return catalog.NewNoopExtendedOperations()
}
