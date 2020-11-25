package tree

type TreePartsBookKeeper struct {
	baseTree  TreePartsType
	newParts  TreePartsType
	baseIndex int
	trees     *TreesRepoType
}

func (trees *TreesRepoType) newTreePartsBookKeeper(treeID TreeID) (*TreePartsBookKeeper, error) {
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
		baseTree: baseParts,
		newParts: make(TreePartsType, 0),
		trees:    trees,
	}, nil
}

func (bk *TreePartsBookKeeper) getPartForKey(key string) (EntryIterator, string, error) {
	for ; bk.baseIndex < len(bk.baseTree) && bk.baseTree[bk.baseIndex].MaxPath < key; bk.baseIndex++ {
		bk.newParts = append(bk.newParts, bk.baseTree[bk.baseIndex])
	}
	if len(bk.baseTree) <= bk.baseIndex {
		return EntryIterator{}, "", InfoNoTreeParts
	}
	return bk.internalGetIterator()
}
func (bk *TreePartsBookKeeper) getNewParts() TreePartsType {
	return bk.newParts
}
func (bk *TreePartsBookKeeper) peekToNextPart() *string {
	if len(bk.baseTree) <= bk.baseIndex {
		return nil
	} else {
		return &bk.baseTree[bk.baseIndex].MaxPath
	}
}
func (bk *TreePartsBookKeeper) getNextPart() (EntryIterator, string, error) {
	if len(bk.baseTree) <= bk.baseIndex {
		return EntryIterator{}, "", InfoNoTreeParts
	}
	return bk.internalGetIterator()
}

func (bk *TreePartsBookKeeper) internalGetIterator() (EntryIterator, string, error) {
	p := bk.baseTree[bk.baseIndex]
	basePartIter, err := bk.trees.PartManger.ListEntries(p.PartName, "")
	bk.baseIndex++
	return basePartIter, p.MaxPath, err
}
