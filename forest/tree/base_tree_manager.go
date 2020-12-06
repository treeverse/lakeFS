package tree

import (
	"bytes"

	"github.com/treeverse/lakefs/forest/sstable"
	gr "github.com/treeverse/lakefs/graveler"
)

type baseTreeManagerType struct {
	baseTree      TreeType
	partsForReuse TreeType
	baseIndex     int
	partManager   sstable.Manager
}

func (trees *TreesRepoType) newBaseTreeManager(treeID gr.TreeID) (*baseTreeManagerType, error) {
	var baseParts TreeType
	var err error
	if treeID == "" {
		baseParts = make(TreeType, 0)
	} else {
		baseParts, err = trees.GetTree(treeID)
		if err != nil {
			return nil, err
		}
	}
	return &baseTreeManagerType{
		baseTree:      baseParts,
		partsForReuse: make(TreeType, 0),
		partManager:   trees.PartManger,
	}, nil
}

func (bm *baseTreeManagerType) isEndOfBase() bool {
	return bm.baseIndex >= len(bm.baseTree)
}

func (bm *baseTreeManagerType) getBasePartForPath(key gr.Key) (*pushBackValueIterator, gr.Key, error) {
	lenBaseTree := len(bm.baseTree)
	for ; bm.baseIndex < lenBaseTree &&
		LessThan(bm.baseTree[bm.baseIndex].MaxKey, key); bm.baseIndex++ {
		bm.partsForReuse = append(bm.partsForReuse, bm.baseTree[bm.baseIndex])
	}
	if len(bm.baseTree) <= bm.baseIndex {
		return nil, minimalKey, InfoBaseTreeExhausted
	}
	p := bm.baseTree[bm.baseIndex]
	basePartIter, err := bm.partManager.NewSSTableIterator(p.PartName, minimalKey)
	bm.baseIndex++
	return newPushbackEntryIterator(basePartIter), p.MaxKey, err
}
func (bm *baseTreeManagerType) getPartsForReuse() *TreeType {
	if bm.baseIndex < len(bm.baseTree)-1 { // the apply loop did not reach the last parts of base, they will be added to reused
		bm.partsForReuse = append(bm.partsForReuse, bm.baseTree[bm.baseIndex:]...)
	}
	return &bm.partsForReuse
}

func (bm *baseTreeManagerType) isPathInNextPart(path gr.Key) bool {
	if bm.isEndOfBase() {
		return true // last part of base is the active now. the new path wil be written to it
	} else {
		return bytes.Compare(path, bm.baseTree[bm.baseIndex].MaxKey) <= 0
	}
}

func (bm *baseTreeManagerType) getBaseMaxKey() gr.Key {
	return bm.baseTree[len(bm.baseTree)-1].MaxKey
}

func (bm *baseTreeManagerType) wasLastPartProcessed() bool {
	return len(bm.baseTree) <= bm.baseIndex
}

func (bm *baseTreeManagerType) getLastPartIter() (*pushBackValueIterator, error) {
	baseIter, _, err := bm.getBasePartForPath(bm.baseTree[len(bm.baseTree)-1].MaxKey)
	return baseIter, err
}
