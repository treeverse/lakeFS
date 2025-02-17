package gs

import (
	"github.com/treeverse/lakefs/pkg/block"
)

var instance *block.Histograms

func NewGSStats(tag *string) block.Histograms {
	if instance == nil {
		inst := block.BuildHistogramsInstance("gs", tag)
		instance = &inst
	}
	return *instance
}
