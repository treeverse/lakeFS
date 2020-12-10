package tree

import (
	gr "github.com/treeverse/lakefs/graveler"
)

func (trees treesRepo) CreateTree(inputIter gr.ValueIterator) (gr.TreeID, error) {
	treeWriter := &treeWriter{}
	for inputIter.Next() {
		input := inputIter.Value()
		err := treeWriter.WriteEntry(*input)
		if err != nil {
			return "", err
		}
	}
	if inputIter.Err() != nil {
		return "", inputIter.Err()
	}
	return treeWriter.SaveTree(TreeType{})
}
