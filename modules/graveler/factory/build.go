package factory

import (
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
)

func BuildConflictsResolver(blockAdapter block.Adapter, valueToPath graveler.ValueToPathFunc) graveler.ConflictsResolver {
	return nil
}
