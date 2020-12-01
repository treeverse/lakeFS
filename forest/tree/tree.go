package tree

import (
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/forest/sstable"
)

const (
	minimalPath = rocks.Path("")
)

type TreePartType struct {
	PartName sstable.SSTableID `json:"part_name"`
	MaxPath  rocks.Path        `json:"max_path"`
}
type TreeType []TreePartType

type TreeContainer struct {
	TreeID    rocks.TreeID
	TreeParts TreeType
}

type TreesRepoType struct {
	TreesMap   map[rocks.TreeID]TreeContainer
	PartManger sstable.Manager
}
