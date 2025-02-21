package azure

import (
	"github.com/treeverse/lakefs/pkg/block"
)

var instance *block.Histograms

func NewAzureStats(adapterStatsID *string) block.Histograms {
	if instance == nil {
		inst := block.BuildHistogramsInstance("azure", adapterStatsID)
		instance = &inst
	}
	return *instance
}
