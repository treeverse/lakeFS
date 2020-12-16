package tree

import (
	gr "github.com/treeverse/lakefs/graveler"
)

func (trees treeRepo) CreateTree(inputIter gr.ValueIterator) (gr.TreeID, error) {
	treeWriter := &treeWriter{}
	for inputIter.Next() {
		input := inputIter.Value()
		err := treeWriter.WriteValue(*input)
		if err != nil {
			return "", err
		}
	}
	if inputIter.Err() != nil {
		return "", inputIter.Err()
	}
	return treeWriter.SaveTree()
}
