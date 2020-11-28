package tree

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"

	"github.com/treeverse/lakefs/catalog/rocks"

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
	TreeInitialWeight         = 64
	NilPath                   = rocks.Path("")
)

var largestByteArray = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}

type TreePartType struct {
	PartName sstable.SSTableID `json:"part_name"`
	MaxPath  rocks.Path        `json:"max_path"`
}
type TreePartsType []TreePartType
type TreeContrainer struct {
	TreeID         rocks.TreeID
	evictionWeight int
	TreeParts      *TreePartsType
}

type TreesRepoType struct {
	TreesMap   map[rocks.TreeID]TreeContrainer
	PartManger *sstable.PebbleSSTableManager
}

var treesRepository TreesRepoType

func InitTreesRepository() {
	treesRepository = TreesRepoType{
		TreesMap:   make(map[rocks.TreeID]TreeContrainer, 100),
		PartManger: &sstable.PebbleSSTableManager{},
	}
}

func (trees TreesRepoType) loadTreeIfNeeded(treeID rocks.TreeID) (TreeContrainer, error) {
	t, exists := trees.TreesMap[treeID]
	if exists {
		return t, nil
	}
	fName := string(treeID) + ".json"
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
		TreeInitialWeight,
		&treeSlice,
	}
	trees.TreesMap[treeID] = t
	return t, nil
}

func (trees TreesRepoType) Apply(treeID rocks.TreeID, InputIter rocks.EntryIterator) (rocks.TreeID, error) {
	var basePartIter *pushBackEntryIterator
	var maxKeyCurrentPart rocks.Path
	var err error
	pushbackInputIter := newPushbackEntryIterator(InputIter)
	// INITIALIZATION
	bk, err := trees.newTreePartsBookKeeper(treeID)
	if err != nil {
		return "", err
	}
	outputPartsWriter := newPartsWriter()
	// PROCESS INPUT
	for pushbackInputIter.Next() {
		input := pushbackInputIter.Value()
		// check if this input is higher than the current base part maximum
		if maxKeyCurrentPart < input.Path {
			// flush all updates targeted comming from  current base part
			if basePartIter != nil { // nil  basePartIter indicates this is first iteration
				err = outputPartsWriter.flushIterToPartsWriter(basePartIter)
				if err != nil {
					return "", err
				}
			}
			if outputPartsWriter.hasOpenWriter() {
				// indicates that writing to this file did not close naturally with a splitter
				// may happen if:
				// 1. the splitter was deleted
				// 2. It is the last part of the base
				// 3 the original base part did not terminate with a splitter
				nextPartMaxKey := bk.peekToNextPart() // max key of next part
				if nextPartMaxKey != nil && input.Path > *nextPartMaxKey {
					// next update will go past the next part. so we prefer to force close
					// the current part, and keep the next part/s as is, and not continue writing to same part
					// this way - al least next part will be reused
					outputPartsWriter.forceCloseCurrentPart()
				}
			}
			basePartIter, maxKeyCurrentPart, err = bk.getBasePartForKey(input.Path)
			if err == InfoNoTreeParts {
				pushbackInputIter.pushBack()
				break
				//err = outputPartsWriter.flushIterToPartsWriter(pushbackInputIter)
				//if err != nil {
				//	return "", err
				//}
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
				err = outputPartsWriter.writeEntry(base.Path, base.Entry)
				if err != nil {
					return "", err
				}
			} else { // reached inserion point of input record
				if input.Entry != nil { // not a delete operation
					err = outputPartsWriter.writeEntry(input.Path, input.Entry)
					if err != nil {
						return "", err
					}
				}
				if base.Path != input.Path {
					// base iterator already contains a path bigger than the current input path, it has to be processed in the next cycle
					basePartIter.Pushback()
				}
				break // after handling input raw, exit base reading loop, go back to reading
			}
		}
	}
	if pushbackInputIter.Err() != nil {
		return "", fmt.Errorf(" apply input erroe: %w", pushbackInputIter.Err())
	}
	err = outputPartsWriter.flushIterToPartsWriter(pushbackInputIter)
	if err != nil {
		return "", fmt.Errorf(" input flushing error : %w", err)
	}
	newParts, err := outputPartsWriter.waitCloseTermination()
	if err != nil {
		return "", fmt.Errorf(" closing of apply parts failed : %w", err)
	}
	newTree := append(newParts, bk.getPartsForReuse()...)
	sort.Slice(newTree, func(i, j int) bool { return newTree[i].MaxPath < newTree[j].MaxPath })
	// todo: calculate tree name, write tree to disk
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

func (t *treeScanner) Value() (*rocks.Path, *rocks.Entry) {
	t.evictionWeight += TreeAccessAdditionlWeight
	return t.currentIter.Value()
}

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
