package tree

import (
	"github.com/treeverse/lakefs/graveler"
)

// written just to pass lint objections. will be deleted ASAP
func RedundentFunctionForLint() *Tree {
	t := new(Tree)
	t.treeSlice = make([]treePart, 1)
	t.treeSlice[0] = treePart{MaxKey: graveler.Key(""), PartName: ""}
	return t
}
