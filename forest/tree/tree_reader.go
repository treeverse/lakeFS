package tree

import (
	"encoding/json"
	"io/ioutil"
	"sort"

	"github.com/treeverse/lakefs/forest/sstable"

	"github.com/treeverse/lakefs/catalog/rocks"
)

var treesRepository TreesRepoType

func InitTreesRepository(manager *sstable.Manager) {
	treesRepository = TreesRepoType{
		TreesMap:   make(map[rocks.TreeID]TreeContainer, 1000),
		PartManger: manager,
	}
}

type treeIterator struct {
	treeID          rocks.TreeID
	TreeParts       *TreeType
	currentIter     rocks.EntryIterator
	currentPart     int
	err             error
	closed          bool
	treesRepository *TreesRepoType
}

func (trees *TreesRepoType) NewScanner(treeID rocks.TreeID, start rocks.Path) (*treeIterator, error) {
	treeSlice, err := trees.loadTreeIfNeeded(treeID)
	if err != nil {
		return nil, err
	}
	partNum := findPartNumForPath(treeSlice, start)
	if partNum >= len(*treeSlice) {
		return nil, ErrPathBiggerThanMaxPath
	}
	partIterator, err := (*treesRepository.PartManger).NewSSTableIterator((*treeSlice)[partNum].PartName, start)
	if err != nil {
		return nil, err
	}
	scanner := &treeIterator{
		treeID:          treeID,
		TreeParts:       treeSlice,
		currentIter:     partIterator,
		currentPart:     partNum,
		treesRepository: trees,
	}
	return scanner, nil
}

func (t *treeIterator) SeekGE(start rocks.Path) bool {
	var err error
	partNum := findPartNumForPath(t.TreeParts, start)
	if partNum != t.currentPart {
		t.currentPart = partNum
		t.currentIter.Close()
		t.currentIter, err = (*t.treesRepository.PartManger).NewSSTableIterator((*t.TreeParts)[partNum].PartName, start)
		if err != nil {
			t.err = err
			return false
		}
		return true
	}
	return t.currentIter.SeekGE(start)
}

func findPartNumForPath(tree *TreeType, path rocks.Path) int {
	n := len(*tree)
	pos := sort.Search(n, func(i int) bool {
		return (*tree)[i].MaxPath >= path
	})
	return pos
}

func (t *treeIterator) Next() bool {
	var err error
	if t.closed {
		return false
	}
	if t.currentIter.Next() {
		return true
	}
	t.err = t.currentIter.Err()
	t.currentIter.Close()
	// assert: if Next returned false and err == nil - reached end of part
	if t.err != nil {
		t.closed = true
		return false
	}
	// assert: the current part end of data. Go to next
	if t.currentPart >= len(*t.TreeParts)-1 {
		t.closed = true
		return false
	}
	t.currentPart++
	t.currentIter, err = (*treesRepository.PartManger).NewSSTableIterator((*t.TreeParts)[t.currentPart].PartName, "")
	if err != nil {
		t.currentIter.Close()
		t.closed = true
		return false
	}
	return t.currentIter.Next()
}

func (t *treeIterator) Err() error {
	if t.currentIter == nil {
		return ErrScannerIsNil
	}
	return t.currentIter.Err()
}

func (t *treeIterator) Value() *rocks.EntryRecord {
	return t.currentIter.Value()
}

func (t *treeIterator) Close() {
	t.currentIter.Close()
}

func (trees TreesRepoType) loadTreeIfNeeded(treeID rocks.TreeID) (TreeType, error) {
	t, exists := trees.TreesMap[treeID]
	if exists {
		return t.TreeParts, nil
	}
	fName := string(treeID) + ".json"
	jsonBytes, err := ioutil.ReadFile(fName)
	if err != nil {
		return nil, err
	}
	treeSlice := make(TreeType, 0)
	err = json.Unmarshal(jsonBytes, &treeSlice)
	if err != nil {
		return nil, err
	}
	t = TreeContainer{
		treeID,
		TreeInitialWeight,
		treeSlice,
	}
	trees.TreesMap[treeID] = t
	return t.TreeParts, nil
}
