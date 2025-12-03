package factory

import (
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/entensionhooks"
	"github.com/treeverse/lakefs/pkg/graveler"
)

func BuildConflictResolvers(cfg config.Config, block block.Adapter) []graveler.ConflictResolver {
	if hook := entensionhooks.Get().Catalog; hook != nil {
		return hook.BuildConflictResolvers(cfg, block)
	}
	return nil
}
