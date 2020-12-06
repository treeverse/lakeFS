package tree

import (
	"fmt"

	"github.com/treeverse/lakefs/catalog/rocks"
)

func (trees TreesRepoType) Apply(baseTreeID rocks.TreeID, inputIter rocks.EntryIterator) (rocks.TreeID, error) {
	var basePartIter *pushBackValueIterator
	var maxCurrentBaseKey rocks.Path
	var err error
	// INITIALIZATION
	pushbackInputIter := newPushbackEntryIterator(inputIter)
	baseTreeManager, err := trees.newBaseTreeManager(baseTreeID)
	if err != nil {
		return "", err
	}
	treeWriter := &TreeWriter{}
	baseTreeMaxPath := baseTreeManager.getBaseMaxKey()
	// PROCESS INPUT
	// Create a new tree by  merging an input entries stream with an existing  tree.
	// entries from the input are merged into parts of the base tree.
	// if a part of the base tree does not contain any updates from input stream, it can be copied as is into
	// the resulting tree.
	// the high level flow is:
	// 1. Read an entry from the input stream.
	// 2. If it matches the currently open base part - merge the input with the existing part into the new tree
	// 3. Else - match the input with another base part, and merge into it
	for pushbackInputIter.Next() { // check exit handling
		input := pushbackInputIter.Value()
		// adjust the input to the correct base and output parts
		if maxCurrentBaseKey < input.Path { // not is current base part
			// flush all updates that remained in  current base part
			if basePartIter != nil { // nil  basePartIter indicates this is first iteration
				err = treeWriter.flushIterToNewTree(basePartIter)
				if err != nil {
					return "", err
				}
				basePartIter.Close()
			}
			// indicates that writing to this part did not close naturally with a splitter,even though a base part was finished
			if treeWriter.hasOpenWriter() {
				// next update will go past the next part of base tree. This means that the next part has no
				// updates and can be reused in the new tree. for that to happen - the current output part must be closed
				// so we close the current part
				// a special case is when the current base part is the last in the tree, and the new path is bigger than
				// any path in the tree. This too is considered as it the path is in next part
				if !baseTreeManager.isPathInNextPart(input.Path) {
					treeWriter.forceCloseCurrentPart()
				}
			}
			// a special case when the input path is bigger than maximum part in the base tree
			if baseTreeMaxPath < input.Path {
				// we want to insert the new entries into the last part of the base tree, so we do not generate tiny
				// parts each run that gets to this situation
				if !baseTreeManager.wasLastPartProcessed() {
					basePartIter, err = baseTreeManager.getLastPartIter()
					if err != nil {
						return "", err
					}
				}
				if basePartIter != nil {
					err = treeWriter.flushIterToNewTree(basePartIter)
					if err != nil {
						return "", err
					}
					basePartIter.Close()
				}
				err = pushbackInputIter.pushBack()
				if err != nil {
					return "", err
				}
				break // exit the main loop after all base was read
			}
			basePartIter, maxCurrentBaseKey, err = baseTreeManager.getBasePartForPath(input.Path)
			if err != nil {
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
