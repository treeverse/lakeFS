package tree

import (
	"fmt"

	"github.com/treeverse/lakefs/catalog/rocks"
)

func (trees TreesRepoType) Apply(baseTreeID rocks.TreeID, inputIter rocks.EntryIterator) (rocks.TreeID, error) {
	var basePartIter *pushBackEntryIterator
	var maxCurrentBaseKey rocks.Path
	var err error
	// INITIALIZATION
	pushbackInputIter := newPushbackEntryIterator(inputIter)
	baseTreeManager, err := trees.newBaseTreeManager(baseTreeID)
	if err != nil {
		return "", err
	}
	treeWriter := &TreeWriter{}
	// PROCESS INPUT
	for pushbackInputIter.Next() { // check exit handling
		input := pushbackInputIter.Value()
		// check if this input is higher than the current base part maximum
		if maxCurrentBaseKey < input.Path {
			//todo: solve the issue of writing to the last part

			// flush all updates that remained in  current base part
			if basePartIter != nil { // nil  basePartIter indicates this is first iteration
				err = treeWriter.flushIterToNewTree(basePartIter)
				if err != nil {
					return "", err
				}
				basePartIter.Close()
			}
			if treeWriter.hasOpenWriter() {
				// indicates that writing to this file did not close naturally with a splitter
				if !baseTreeManager.isPathInNextPart(input.Path) {
					// next update will go past the next part of base tree. This means that the next part has no
					//updates and can be reused in the new tree. for that to happen - the current part must be closed
					//so we prefer to force close
					treeWriter.forceCloseCurrentPart()
				}
			}
			basePartIter, maxCurrentBaseKey, err = baseTreeManager.getBasePartForPath(input.Path)
			if err == InfoBaseTreeExhausted {
				err = pushbackInputIter.pushBack()
				if err != nil {
					return "", err
				}
				break
			} else if err != nil {
				return "", err
			}
		}
		// handle single input update
		//assert: input path is smaller than max path of current base part. It is not possible that base part
		// will be exhausted (Next return false) before reaching an entry that >= input path.
		for {
			if !basePartIter.Next() {
				return "", fmt.Errorf("base reading ends before reaching input key : %w", basePartIter.Err())
			}
			base := basePartIter.Value()
			if base.Path < input.Path {

				err = treeWriter.writeEntry(*base)
				if err != nil {
					return "", err
				}
			} else { // reached insertion point of input record
				if input.Entry != nil { // not a delete operation
					err = treeWriter.writeEntry(*input)
					if err != nil {
						return "", err
					}
				}
				if base.Path != input.Path {
					// base iterator already contains a path bigger than the current input path, it has to be processed in the next cycle
					err := basePartIter.pushBack()
					if err != nil {
						return "", err
					}
				}
				break // after handling input raw, exit base reading loop, go back to reading
			}
		}
	}
	if pushbackInputIter.Err() != nil {
		return "", fmt.Errorf(" apply input erroe: %w", pushbackInputIter.Err())
	}
	err = treeWriter.flushIterToNewTree(pushbackInputIter)
	if err != nil {
		return "", fmt.Errorf(" input flushing error : %w", err)
	}
	return treeWriter.finalizeTree(baseTreeManager.getPartsForReuse())

}
