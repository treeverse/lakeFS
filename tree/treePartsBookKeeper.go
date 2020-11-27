package tree

import (
	"github.com/treeverse/lakefs/catalog/rocks"
)

type TreePartsBookKeeper struct {
	baseTree      TreePartsType
	partsForReuse TreePartsType
	baseIndex     int
	trees         *TreesRepoType
}

func (trees *TreesRepoType) newTreePartsBookKeeper(treeID rocks.TreeID) (*TreePartsBookKeeper, error) {
	var baseParts TreePartsType
	if treeID == "" {
		baseParts = make(TreePartsType, 0)
	} else {
		baseTree, err := trees.loadTreeIfNeeded(treeID)
		if err != nil {
			return "", err
		}
		baseParts = *baseTree.TreeParts
	}
	return &TreePartsBookKeeper{
		baseTree:      baseParts,
		partsForReuse: make(TreePartsType, 0),
		trees:         trees,
	}, nil
}

func (bk *TreePartsBookKeeper) getBasePartForKey(key rocks.Path) (rocks.EntryIterator, rocks.Path, error) {
	for bk.baseIndex < len(bk.baseTree) && bk.baseTree[bk.baseIndex].MaxPath < key {
		bk.partsForReuse = append(bk.partsForReuse, bk.baseTree[bk.baseIndex])
		bk.baseIndex++
	}
	if len(bk.baseTree) <= bk.baseIndex {
		return nil, NilPath, InfoNoTreeParts
	}
	p := bk.baseTree[bk.baseIndex]
	basePartIter, err := bk.trees.PartManger.SSTableIterator(p.PartName, NilPath)
	bk.baseIndex++
	return basePartIter, p.MaxPath, err
}
func (bk *TreePartsBookKeeper) getPartsForReuse() TreePartsType {
	return bk.partsForReuse
}

func (bk *TreePartsBookKeeper) peekToNextPart() *rocks.Path {
	if len(bk.baseTree) <= bk.baseIndex {
		return nil
	} else {
		return &bk.baseTree[bk.baseIndex].MaxPath
	}
}

//func (bk *TreePartsBookKeeper) getNextPart() (rocks.EntryIterator, rocks.Path, error) {
//	if len(bk.baseTree) <= bk.baseIndex {
//		return nil, "", InfoNoTreeParts
//	}
//	return bk.internalGetIterator()
//}
//
//func (bk *TreePartsBookKeeper) internalGetIterator() (rocks.EntryIterator, rocks.Path, error) {
//
//}
