package tree

import (
	gr "github.com/treeverse/lakefs/graveler"
)

func (trees TreesRepoType) CreateTree(inputIter gr.ValueIterator) (gr.TreeID, error) {
	treeWriter := &TreeWriter{}
	for inputIter.Next() {
		input := inputIter.Value()
		err := treeWriter.writeEntry(*input)
		if err != nil {
			return "", err
		}
	}
	if inputIter.Err() != nil {
		return "", inputIter.Err()
	}
	return treeWriter.finalizeTree(nil)
}
