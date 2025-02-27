package gs

import (
	"github.com/treeverse/lakefs/pkg/block"
)

var instance *block.Histograms

func NewGSStats(adapterStatsID *string) block.Histograms {
	if instance == nil {
		inst := block.BuildHistogramsInstance("gs", adapterStatsID)
		instance = &inst
	}
	return *instance
}
