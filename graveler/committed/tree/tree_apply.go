package tree

import (
	"bytes"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/sstable"
	//"github.com/treeverse/lakefs/catalog/rocks"
)

func (trees treeRepo) Apply(baseTreeID graveler.TreeID, inputIter graveler.ValueIterator) (graveler.TreeID, error) {
	var basePartIter *pushBackValueIterator
	var maxCurrentBaseKey graveler.Key
	var err error
	// INITIALIZATION
	pushbackInputIter := newPushbackEntryIterator(inputIter)
	baseTreeManager, err := trees.newBaseTreeManager(baseTreeID)
	if err != nil {
		return "", err
	}
	// todo: replace with real implementation of sstable.BatchWriterCloser
	treeWriter := trees.NewTreeWriter(SplitFactor, sstable.BatchWriterCloser)
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
		if bytes.Compare(maxCurrentBaseKey, input.Key) < 0 { // not is current base part
			basePartIter, err, id, err2 := baseTreeManager.advanceBasePart(basePartIter, treeWriter, input, baseTreeMaxPath, maxCurrentBaseKey, pushbackInputIter)
			if err2 != nil {
				return id, err2
			}
		}
		// handle single input update
		//assert: input path is smaller-equal than max key of current base part. It is not possible that base part
		// will be exhausted (Next return false) before reaching an entry that >= input path.
		for {
			if !basePartIter.Next() {
				return "", fmt.Errorf("base part reading ends or error occured before reaching input key : %w", basePartIter.Err())
			}
			base := basePartIter.Value()
			if bytes.Compare(base.Key, input.Key) < 0 {
				err = treeWriter.WriteValue(*base)
				if err != nil {
					return "", err
				}
			} else { // reached insertion point of input record
				if input.Value != nil { // not a delete operation
					err = treeWriter.WriteValue(*input)
					if err != nil {
						return "", err
					}
				}
				if !bytes.Equal(base.Key, input.Key) {
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
	err = treeWriter.FlushIterToTree(pushbackInputIter)
	if err != nil {
		return "", fmt.Errorf(" input flushing error : %w", err)
	}
	return treeWriter.saveTreeWithReuseParts(baseTreeManager.getPartsForReuse())
}

func (base *BaseTreeManager) advanceBasePart(basePartIter *pushBackValueIterator, treeWriter TreeWriter, input *graveler.ValueRecord, baseTreeMaxPath graveler.Key, maxCurrentBaseKey graveler.Key, pushbackInputIter *pushBackValueIterator) (*pushBackValueIterator, error, graveler.TreeID, error) {
	var err error
	// flush all updates that remained in  current base part
	if basePartIter != nil { // nil  basePartIter indicates this is first iteration
		err = treeWriter.FlushIterToTree(basePartIter)
		if err != nil {
			return nil, nil, "", err
		}
		basePartIter.Close()
		basePartIter = nil
	}
	// indicates that writing to this part did not close naturally with a splitter,even though a base part was finished
	if treeWriter.HasOpenWriter() {
		// next update will go past the next part of base tree. This means that the next part has no
		// updates and can be reused in the new tree. for that to happen - the current output part must be closed
		// so we close the current part
		// a special case is when the current base part is the last in the tree, and the new path is bigger than
		// any path in the tree. This too is considered as it the path is in next part
		if !base.isPathInNextPart(input.Key) {
			treeWriter.ClosePart()
		}
	}
	if bytes.Compare(baseTreeMaxPath, input.Key) >= 0 {
		// common case - input key falls within the range of keys in base tree ( less than maximum key in tree)
		basePartIter, maxCurrentBaseKey, err = base.getBasePartForPath(input.Key)
		if err != nil {
			return nil, nil, "", err
		}
	} else {
		// a special case when the input path is bigger than maximum part in the base tree
		// Insert the new entries into the last part of the base tree, and dont create a tiny last part
		// when some keys  are bigger than keys accepted before (e.g. when the keys are based on date)
		// one thing we know about the last part: there is very tiny chance the last key is a split key. so adding keys
		// to the last part make sense.
		// There are two possibilities:
		// 1. the input had entries that went into the last part. so last part should be open for writing now.
		// 2. the last input key was smaller than the minimum key in the last part, so last base part should be opened
		//    and copied into the output tree. After that- exit the loop and finish writing the input iterator
		//    into the new tree
		if !base.wasLastPartProcessed() {
			basePartIter, err = base.getLastPartIter()
			if err != nil {
				return nil, nil, "", err
			}
			err = treeWriter.FlushIterToTree(basePartIter)
			if err != nil {
				return nil, nil, "", err
			}
			basePartIter.Close()
			basePartIter = nil
		}
		err = pushbackInputIter.pushBack()
		if err != nil {
			return nil, nil, "", err
		}
		break // exit the main loop after all base was read
	}
	return basePartIter, err, "", nil
}
