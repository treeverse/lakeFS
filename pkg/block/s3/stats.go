package s3

import (
	"github.com/treeverse/lakefs/pkg/block"
)

var instance *block.Histograms

func NewS3Stats(blockstoreTag *string) block.Histograms {
	if instance == nil {
		inst := block.BuildHistogramsInstance("s3", blockstoreTag)
		instance = &inst
	}
	return *instance
}
