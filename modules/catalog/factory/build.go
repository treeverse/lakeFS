package factory

import (
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
)

func BuildConflictResolvers(block block.Adapter) []graveler.ConflictResolver {
	return nil
}
