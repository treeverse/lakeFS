package factory

import (
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
)

func BuildConflictResolvers(cfg config.Config, block block.Adapter) []graveler.ConflictResolver {
	return nil
}

func NewAsyncOperationsHandler(_ *catalog.Catalog) catalog.AsyncOperationsHandler {
	return catalog.NewNoopAsyncOperationsHandler()
}
