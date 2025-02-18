package gs

import (
	"github.com/treeverse/lakefs/pkg/block"
)

var instance *block.Histograms

func NewGSStats(blockstoreTag *string) block.Histograms {
	if instance == nil {
		inst := block.BuildHistogramsInstance("gs", blockstoreTag)
		instance = &inst
	}
	return *instance
}
