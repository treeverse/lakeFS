package tree

import (
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"sort"

	"github.com/treeverse/lakefs/rocks3"

	"github.com/treeverse/lakefs/tree/sstable"
)

const (
	SplitFactor               = 200_000 // avarage number of entries in a part file. used to compute modulo on hash of path
	SplitMaxfactor            = 3       // a part will be closed if number of entries written to it exceed splitFactor * splitMaxFactor
	SplitMinFactor            = 50      // a part will not be closed in number of rows less than splitFactor / SplitMinFactor
	MaxStatusChan             = 10_000  // practically unlimitted number of close messages waiting for the apply to terminate
	TreeAccessAdditionlWeight = 16
	TreeBufferSize            = 1_000
	TreeBufferTrimOff         = 100 // when buffer eviction kicks in - number of trees that will be removed
)

var largestByteArray = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}

type TreePartType struct {
	PartName string `json:"part_name"`
	MaxPath  string `json:"max_path"`
}
type TreePartsType []TreePartType
type TreeContrainer struct {
	TreeID         TreeID
	evictionWeight int
	TreeParts      *TreePartsType
}

type TreesRepoType struct {
	TreesMap   map[TreeID]TreeContrainer
	PartManger *sstable.PebbleSSTableManager
}

var treesRepository TreesRepoType

func InitTreeRepository() {
	treesRepository = TreesRepoType{
		TreesMap:   make(map[TreeID]TreeContrainer, 100),
		PartManger: &sstable.PebbleSSTableManager{},
	}
}

func (trees TreesRepoType) loadTreeIfNeeded(treeID TreeID) (TreeContrainer, error) {
	t, exists := trees.TreesMap[treeID]
	if exists {
		return t, nil
	}
	fName := hex.EncodeToString(treeID[:]) + ".json"
	jsonBytes, err := ioutil.ReadFile(fName)
	if err != nil {
		return TreeContrainer{}, err
	}
	treeSlice := make(TreePartsType, 0)
	err = json.Unmarshal(jsonBytes, &treeSlice)
	if err != nil {
		return TreeContrainer{}, err
	}
	t = TreeContrainer{
		treeID,
		0,
		&treeSlice,
	}
	trees.TreesMap[treeID] = t
	return t, nil
}

/*
baseExausted
baseIndex ?
outputPratOpen
InputPartOpen
IteratorExausted
*/
func (trees TreesRepoType) Apply(treeID TreeID, inputIter EntryIterator) (TreeID, error) {
	var baseExausted, iteratorExausted bool
	var basePartIter EntryIterator
	var baseIndex int
	var baseTree, newTree TreeContrainer
	var baseParts, newParts TreePartsType
	var doingMerge, terminateApply bool
	var err error
	// INITIALIZATION
	bk, err := trees.newTreePartsBookKeeper(treeID)
	if err != nil {
		return "", err
	}
	//if !inputIter.Next() {
	//	return nil, ErrEmptyInputToApply
	//}
	partsWriter := newPartsWriter()
	maxKeyCurrentPart := ""
	if err != nil {
		return "", err
	}
	// PROCESS INPUT
	firstTime := true
	for inputIter.Next() {
		inputKey, inputEntry := inputIter.Value()
		if maxKeyCurrentPart < inputKey { // new value falls into another part
			continueWithSameBase := false
			if firstTime {
				firstTime = false
			} else { // if not first time - empty remaining in base and check
				err = emptyIterToPartsWriter(partsWriter, basePartIter)
				if err != nil {
					return "", err
				}
			}
			if partsWriter.hasOpenWriter() {
				nextPartMaxKey := bk.peekToNextPart()
				if nextPartMaxKey == nil { // base finished - just copy input to output
					break
				}
				if inputKey > *nextPartMaxKey {
					partsWriter.forceCloseCurrentPart()
				} else {
					basePartIter, maxKeyCurrentPart, err = bk.getNextPart()
					continueWithSameBase = true
				}
			}

			if continueWithSameBase { // writer closed a part with end of base part
				basePartIter, maxKeyCurrentPart, err = bk.getPartForKey(inputKey)
				if err != nil {
					return "", err
				}
			}
		}
		// handle single update
		for basePartIter.Next() {
			baseKey, baseEntry := basePartIter.Value()
			if baseKey < inputKey {
				err = partsWriter.writeEntry(baseKey, baseEntry)
				if err != nil {
					return "", err
				}
				continue
			} else {
				if inputEntry != nil {
					err = partsWriter.writeEntry(inputKey, inputEntry)
					if err != nil {
						return "", err
					}
				}
				if baseKey == inputKey {
					basePartIter.Next() // result of next is dont care
					//if !basePartIter.Next() {
					//	return "", ErrTreeCorrupted // The tree contains a maximum key for this part that is not in it
					//}
				}
			}
		}
		if basePartIter.Error() != nil {
			return "", basePartIter.Error()
		}

		//for len(baseParts) > 0 && baseParts[0].MaxPath < inputKey && !doingMerge {
		//	newParts = append(newParts, baseParts[0])
		//	baseParts = baseParts[1:]
		//	continue
		//}
		//if len(baseParts) == 0 { // wrong
		//	//copyUntil()
		//	break
		//}
		//
		//nextPart := baseParts[0].PartName
		//basePartIter, err = treesRepository.PartManger.ListEntries(nextPart, "")
		//if err != nil {
		//	return "", err
		//}

	}
}

func emptyIterToPartsWriter(pw *partsWriter, iter EntryIterator) error {
	for iter.Next() {
		err := pw.writeEntry(iter.Value())
		if err != nil {
			return err
		}
	}
	return iter.Error()
}

func copyPartUntil(sentinel []byte, newParts *TreePartsType) {

}

func (trees TreesRepoType) NewScanner(tree TreeID, start string) (*treeScanner, error) {
	t, err := trees.loadTreeIfNeeded(tree)
	treeSlice := t.TreeParts
	if err != nil {
		return nil, err
	}
	partNum := findPartNumForPath(treeSlice, start)
	if partNum == len(treeSlice) {
		return nil, ErrPathBiggerThanMaxPath
	}
	partScanner, err := treesRepository.PartManger.ListEntries(treeSlice[partNum].PartName, start)

	scanner := &treeScanner{
		TreeContrainer: t,
		currentIter:    partScanner,
		currentPart:    partNum,
		//currentKey: []byte(start),
	}
	return scanner, nil
}

func findPartNumForPath(tree *TreePartsType, path string) int {
	n := len(*tree)
	pos := sort.Search(n, func(i int) bool {
		return (*tree)[i].MaxPath >= path
	})
	return pos
}

type treeScanner struct {
	TreeContrainer
	currentIter sstable.EntryIterator
	currentPart int
	closed      bool
}

func (t *treeScanner) Next() bool {
	if t.closed {
		return false
	}
	if t.currentIter.Next() {
		return true
	}
	err := t.currentIter.Error()
	t.currentIter.Close()
	// assert: if Next returned false and err == nil - reached end of part
	if err != nil {
		t.closed = true
		return false
	}
	// assert: the current part end of data. Go to next
	if t.currentPart >= len(*t.TreeParts)-1 {
		t.closed = true
		return false
	}
	t.currentPart++
	t.currentIter, err = treesRepository.PartManger.ListEntries(*t.TreeParts[t.currentPart].PartName, "")
	if err != nil {
		t.currentIter.Close()
		t.closed = true
		return false
	}
	return t.currentIter.Next()
}

func (t *treeScanner) Error() error {
	if t.currentIter == nil {
		return ErrScannerIsNil
	}
	return t.currentIter.Error()
}

func (t *treeScanner) Value() (*rocks3.Path, *rocks3.Entry) {
	t.evictionWeight += TreeAccessAdditionlWeight
	return t.currentIter.Value()
}

/*
type EntryIterator interface {
	// SeekGE advances the iterator to point to the given path.
	// Returns true iff if the iterator is pointing at a valid entry.
	SeekGE(rocks3.Path) bool

	// Next advances the iterator.
	// Returns true iff if the iterator is pointing at a valid entry.
	Next() bool

	// Value returns the last read path and entry.
	// Must return non-nil results after Next() or SeekGE(path) returned true.
	Value() (*rocks3.Path, *rocks3.Entry)

	// Error returns any accumulated error.
	Error() error

	io.Closer
}
}
*/
//func (trees TreesRepoType) GetEntry(tree TreeID, path Path) (*Entry, error) {
//	t, exists := trees[tree]
//	if !exists {
//		// load tree into t
//	}
//	// bin search for the right part
//
//	// get entry from part
//
//	return
//}
/*func (trees TreesRepoType) ApplyOld(treeID TreeID, iter EntryIterator) (TreeID, error) {
	var currentBaseTreeIndex int
	var numOfPartsDiff int //???????
	var inPartMerge bool
	var baseExusted bool
	var baseTree TreeContrainer
	var baseSlice *TreePartsType
	if treeID == "" {
		baseExusted = true
	} else {
		baseTree, err := trees.loadTreeIfNeeded(treeID)
		if err != nil {
			return "", err
		}
		baseSlice = baseTree.TreeParts
	}
	newTree := TreeContrainer{TreeParts: new(TreePartsType)}
	newTreeSlice := newTree.TreeParts
	for iter.Next() {
		k, v := iter.Value()
		splitPath := isSplitPath(k)

		if inPartMerge && !baseExusted && k > (*baseSlice)[currentBaseTreeIndex].MaxPath {
			partName, err := copyRemaining()
			if err != nil {
				return "", err
			}
			newPart := TreePartType{MaxPath: (*baseSlice)[currentBaseTreeIndex].MaxPath,
				PartName: partName}
			*newTreeSlice = append(*newTreeSlice, newPart)
			inPartMerge = false
			currentBaseTreeIndex++
			if currentBaseTreeIndex == len(*baseSlice) {
				baseExusted = true
			}
		}
		if !baseExusted && k > (*baseSlice)[currentBaseTreeIndex].MaxPath {
			*newTreeSlice = append(*newTreeSlice)
		}
		for ; k > (*baseSlice)[currentBaseTreeIndex].MaxPath; currentBaseTreeIndex++ {
			*newTreeSlice = append(*newTreeSlice, (*baseSlice)[currentBaseTreeIndex])
		}
		if len(*baseSlice) > currentBaseTreeIndex {

		}
		//newPathBaseTreeIndex := findPartNumForPath(baseTree.TreeParts, k)
		if newPathBaseTreeIndex > currentBaseTreeIndex {

			additionalParts := baseTree
			copySlice := baseSlice[baseTree]
		}

		if baseTreeIndex > newTreeIndex-numOfPartsDiff {

		}

	}
}*/
