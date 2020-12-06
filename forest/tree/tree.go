package tree

import (
	"bytes"

	"github.com/treeverse/lakefs/forest/sstable"
	gr "github.com/treeverse/lakefs/graveler"
)

var minimalKey = gr.Key("")

type TreePartType struct {
	PartName sstable.SSTableID `json:"part_name"`
	MaxKey   gr.Key            `json:"max_path"`
}
type TreeType []TreePartType

type TreeContainer struct {
	TreeID    gr.TreeID
	TreeParts TreeType
}

type TreesRepoType struct {
	TreesMap   map[gr.TreeID]TreeContainer
	PartManger sstable.Manager
}

func LessThan(a, b []byte) bool {
	return bytes.Compare(a, b) < 0
}
func LessThanOrEqual(a, b []byte) bool {
	return bytes.Compare(a, b) <= 0
}

func Equal(a, b []byte) bool {
	return bytes.Compare(a, b) == 0
}
