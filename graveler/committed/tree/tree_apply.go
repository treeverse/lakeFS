package tree

import (
	"bytes"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/sstable"
)

func (trees treeRepo) Apply(baseTreeID graveler.TreeID, inputIter graveler.ValueIterator) (graveler.TreeID, error) {
	var basePartIter *pushBackValueIterator
	var maxCurrentBaseKey graveler.Key
	var err error
	var baseFinished bool
	// INITIALIZATION
	baseTreeManager, err := trees.newBaseTreeManager(baseTreeID)
	if err != nil {
		return "", err
	}
	// todo: replace with real implementation of sstable.BatchWriterCloser
	treeWriter := trees.NewTreeWriter(SplitFactor, sstable.BatchWriterCloser)
	baseTreeMaxKey := baseTreeManager.getBaseMaxKey()
	// PROCESS INPUT
	// Create a new tree by  merging an input entries stream with an existing  tree.
	// entries from the input are merged into parts of the base tree.
	// if a part of the base tree does not contain any updates from input stream, it can be copied as is into
	// the resulting tree.
	// the high level flow is:
	// 1. Read an entry from the input stream.
	// 2. check if the input key falls within the currently open base part. if not:
	//		a. handle the termination of the current part
	//	    b. open tha matching part the matching part
	// 3.  merge the input with the open part into the new tree
	for inputIter.Next() { // check exit handling
		inputRecord := inputIter.Value()
		// adjust the input to the correct base and output parts
		if bytes.Compare(maxCurrentBaseKey, inputRecord.Key) < 0 { // not is current base part
			basePartIter, maxCurrentBaseKey, baseFinished, err = advanceBasePart(basePartIter, treeWriter, inputRecord, baseTreeMaxKey, baseTreeManager)
			if err != nil {
				return "", err
			}
			if baseFinished {
				err = treeWriter.WriteValue(*inputRecord)
				if err != nil {
					return "", err
				}
				err = treeWriter.FlushIterToTree(inputIter)
				if err != nil {
					return "", fmt.Errorf(" input flushing error : %w", err)
				}
				break // exit the main loop after all base tree was processed
			}
		}
		// handle single input update
		err = handleSingleInputRecord(basePartIter, inputRecord, treeWriter)
		if err != nil {
			return "", err
		}
	}
	// base tree was exhausted.
	// 1. write any remaining base records into the result tree
	// 2. save the tree and return its ID
	if inputIter.Err() != nil {
		return "", fmt.Errorf(" apply input erroe: %w", inputIter.Err())
	}
	if !baseFinished && basePartIter != nil {
		err = treeWriter.FlushIterToTree(basePartIter)
		if err != nil {
			return "", fmt.Errorf(" base flushing error : %w", err)
		}
	}
	return treeWriter.saveTreeWithReuseParts(baseTreeManager.getPartsForReuse())
}

func advanceBasePart(basePartIter *pushBackValueIterator, treeWriter TreeWriter, inputRecord *graveler.ValueRecord, baseTreeMaxKey graveler.Key,
	base *BaseTreeManager) (*pushBackValueIterator, graveler.Key, bool, error) {
	var err error
	// flush all updates that remained in  current base part
	if basePartIter != nil { // nil  basePartIter indicates this is first iteration
		err = treeWriter.FlushIterToTree(basePartIter)
		if err != nil {
			return nil, nil, false, err
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
		if !base.isPathInNextPart(inputRecord.Key) {
			treeWriter.ClosePart()
		}
	}
	if bytes.Compare(baseTreeMaxKey, inputRecord.Key) >= 0 {
		// common case - input key falls within the range of keys in base tree ( less-equal  maximum key in tree)
		basePartIter, maxCurrentBaseKey, err := base.getBasePartForPath(inputRecord.Key)
		if err == nil {
			return basePartIter, maxCurrentBaseKey, false, nil
		} else {
			return nil, nil, false, err
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
				return nil, nil, false, err
			}
			err = treeWriter.FlushIterToTree(basePartIter)
			if err != nil {
				return nil, nil, false, err
			}
			basePartIter.Close()
			basePartIter = nil
		}
		return nil, nil, true, nil // since all input was finished, the only relevant return value is the boolean
		// indicating that base was finished
	}
}

func handleSingleInputRecord(basePartIter *pushBackValueIterator, inputRecord *graveler.ValueRecord, treeWriter TreeWriter) error {
	var err error
	var baseRecord *graveler.ValueRecord
	for {
		//assert: input path is smaller-equal than max key of current base part. It is an error if base part
		// is exhausted (Next return false) before reaching an entry that >= input path.
		if !basePartIter.Next() {
			return fmt.Errorf("base part reading ends or error occured before reaching input key : %w", basePartIter.Err())
		}
		baseRecord = basePartIter.Value()
		if bytes.Compare(baseRecord.Key, inputRecord.Key) < 0 {
			err = treeWriter.WriteValue(*baseRecord)
			if err != nil {
				return err
			}
		} else {
			break // reached insertion point of input record
		}
	}
	// insert the input record
	if inputRecord.Value != nil { // not a delete operation
		err = treeWriter.WriteValue(*inputRecord)
		if err != nil {
			return err
		}
	}
	// check if input updated an existing record, or it is a new record
	if !bytes.Equal(baseRecord.Key, inputRecord.Key) {
		// base iterator already contains a key bigger than the current input key, it has to be processed in the next cycle
		err = basePartIter.pushBack()
		if err != nil {
			return err
		}
	}
	return nil
}
