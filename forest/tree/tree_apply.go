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
	newTreeWriter := &newTreeManager{
		statusChan: make(chan closeReply, MaxStatusChan),
	}
	// PROCESS INPUT
	for pushbackInputIter.Next() { // check exit handling
		input := pushbackInputIter.Value()
		// check if this input is higher than the current base part maximum
		if maxCurrentBaseKey < input.Path {
			// flush all updates targeted comming from  current base part
			if basePartIter != nil { // nil  basePartIter indicates this is first iteration
				err = newTreeWriter.flushIterToNewTree(basePartIter)
				if err != nil {
					return "", err
				}
				basePartIter.Close()
			}
			if newTreeWriter.hasOpenWriter() {
				// indicates that writing to this file did not close naturally with a splitter
				if !baseTreeManager.isPathInNextPart(input.Path) {
					// next update will go past the next part of base tree. This means that the next part has no
					//updates and can be reused in the new tree. for that to happen - the current part must be closed
					//so we prefer to force close
					newTreeWriter.forceCloseCurrentPart()
				}
			}
			basePartIter, maxCurrentBaseKey, err = baseTreeManager.getBasePartForPath(input.Path)
			if err == InfoNoTreeParts {
				pushbackInputIter.pushBack()
				err = newTreeWriter.flushIterToNewTree(pushbackInputIter)
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
		// will be exausted (Next return false) before reaching an entry that >= input path.
		for {
			if !basePartIter.Next() {
				return "", fmt.Errorf("base reading ends before reaching input key : %w", basePartIter.Err())
			}
			base := basePartIter.Value()
			if base.Path < input.Path {
				err = newTreeWriter.writeEntry(base.Path, base.Entry)
				if err != nil {
					return "", err
				}
			} else { // reached inserion point of input record
				if input.Entry != nil { // not a delete operation
					err = newTreeWriter.writeEntry(input.Path, input.Entry)
					if err != nil {
						return "", err
					}
				}
				if base.Path != input.Path {
					// base iterator already contains a path bigger than the current input path, it has to be processed in the next cycle
					basePartIter.pushBack()
				}
				break // after handling input raw, exit base reading loop, go back to reading
			}
		}
	}
	if pushbackInputIter.Err() != nil {
		return "", fmt.Errorf(" apply input erroe: %w", pushbackInputIter.Err())
	}
	err = newTreeWriter.flushIterToNewTree(pushbackInputIter) // todo: is redundent?
	if err != nil {
		return "", fmt.Errorf(" input flushing error : %w", err)
	}
	return newTreeWriter.finalizeTree(baseTreeManager.getPartsForReuse())

}
