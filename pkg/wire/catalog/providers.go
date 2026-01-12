package catalog

import (
	"github.com/goforj/wire"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// CatalogSet provides catalog-related factory functions.
// External projects can replace this set to provide custom catalog implementations.
var CatalogSet = wire.NewSet(
	BuildConflictResolvers,
	BuildExtendedOperations,
)

// BuildConflictResolvers returns the conflict resolvers for merge operations.
// The default open-source implementation returns nil (no custom resolvers).
func BuildConflictResolvers(_ config.Config, _ block.Adapter) []graveler.ConflictResolver {
	return nil
}

// BuildExtendedOperations returns extended catalog operations.
// The default open-source implementation returns a no-op implementation.
func BuildExtendedOperations(_ *catalog.Catalog) catalog.ExtendedOperations {
	return catalog.NewNoopExtendedOperations()
}
