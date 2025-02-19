package azure

import (
	"github.com/treeverse/lakefs/pkg/block"
)

var instance *block.Histograms

func NewAzureStats(blockstoreTag *string) block.Histograms {
	if instance == nil {
		inst := block.BuildHistogramsInstance("azure", blockstoreTag)
		instance = &inst
	}
	return *instance
}
