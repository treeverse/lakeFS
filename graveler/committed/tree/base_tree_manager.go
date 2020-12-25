package tree

import (
	"bytes"

	gr "github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/sstable"
)

type BaseTreeManager struct {
	baseTree      Tree            // tree on which the input iterator will be applied, to create a new tree
	partsForReuse []part          // part that can be copied directly from base tree to new tree
	baseIndex     int             // next base part that should be processed or re-used
	partManager   sstable.Manager // for lack of better place to connect to sstable
}

func (trees *treeRepo) newBaseTreeManager(treeID gr.TreeID) (*BaseTreeManager, error) {
	var baseParts Tree
	var err error
	if treeID == "" {
		baseParts = Tree{}
	} else {
		baseParts, err = trees.GetTree(treeID)
		if err != nil {
			return nil, err
		}
	}
	return &BaseTreeManager{
		baseTree:      baseParts,
		partsForReuse: make([]part, 0),
		partManager:   trees.partManger,
	}, nil
}

func (bm *BaseTreeManager) isEndOfBase() bool {
	return bm.baseIndex >= len(bm.baseTree.treeSlice)
}

// getBasePartForPath is the most important method
// it accepts the next key from the input iterator, and finds the base part where it should be added or updated.
//
func (bm *BaseTreeManager) getBasePartForPath(key gr.Key) (*pushBackValueIterator, gr.Key, error) {
	baseSlice := bm.baseTree.treeSlice
	lenBaseTree := len(baseSlice)
	for ; bm.baseIndex < lenBaseTree && bytes.Compare(baseSlice[bm.baseIndex].MaxKey, key) < 0; bm.baseIndex++ {
		bm.partsForReuse = append(bm.partsForReuse, baseSlice[bm.baseIndex])
	}
	if len(baseSlice) <= bm.baseIndex {
		return nil, nil, ErrPathBiggerThanMaxPath
	}
	p := baseSlice[bm.baseIndex]
	basePartIter, err := bm.partManager.NewSSTableIterator(p.PartName, nil)
	if err != nil {
		return nil, nil, err
	}
	bm.baseIndex++
	return newPushbackEntryIterator(basePartIter), p.MaxKey, nil
}
func (bm *BaseTreeManager) getPartsForReuse() *[]part {
	if bm.baseIndex < len(bm.baseTree.treeSlice) { // the apply loop did not reach the last parts of base, they will be added to reused
		bm.partsForReuse = append(bm.partsForReuse, bm.baseTree.treeSlice[bm.baseIndex:]...)
	}
	return &bm.partsForReuse
}

func (bm *BaseTreeManager) isPathInNextPart(path gr.Key) bool {
	if bm.isEndOfBase() {
		return true // last part of base is the active now. the new path wil be written to it
	} else {
		return bytes.Compare(path, bm.baseTree.treeSlice[bm.baseIndex].MaxKey) <= 0
	}
}

func (bm *BaseTreeManager) getBaseMaxKey() gr.Key {
	return bm.baseTree.treeSlice[len(bm.baseTree.treeSlice)-1].MaxKey
}

func (bm *BaseTreeManager) wasLastPartProcessed() bool {
	return len(bm.baseTree.treeSlice) <= bm.baseIndex
}

func (bm *BaseTreeManager) getLastPartIter() (*pushBackValueIterator, error) {
	baseIter, _, err := bm.getBasePartForPath(bm.baseTree.treeSlice[len(bm.baseTree.treeSlice)-1].MaxKey)
	return baseIter, err
}
