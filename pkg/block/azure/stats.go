package azure

import (
	"github.com/treeverse/lakefs/pkg/block"
)

var instance *block.Histograms

func NewAzureStats(tag *string) block.Histograms {
	if instance == nil {
		inst := block.BuildHistogramsInstance("azure", tag)
		instance = &inst
	}
	return *instance
}
