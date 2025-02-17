package s3

import (
	"github.com/treeverse/lakefs/pkg/block"
)

var instance *block.Histograms

func NewS3Stats(tag *string) block.Histograms {
	if instance == nil {
		inst := block.BuildHistogramsInstance("s3", tag)
		instance = &inst
	}
	return *instance
}
