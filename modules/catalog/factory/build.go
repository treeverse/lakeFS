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

func BuildExtendedOperations(_ *catalog.Catalog) catalog.ExtendedOperations {
	return catalog.NewNoopExtendedOperations()
}

// BuildTaskObserver returns the task observer for async operation lifecycle events.
// OSS returns a no-op observer. Enterprise overrides this to provide metrics.
func BuildTaskObserver() catalog.TaskObserver {
	return &catalog.NoOpTaskObserver{}
}
