package tree

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"sort"

	gr "github.com/treeverse/lakefs/graveler"
)

var treesRepository TreesRepoType

type treeIterator struct {
	//treeID      gr.TreeID
	TreeParts   TreeType
	currentIter gr.ValueIterator
	currentPart int
	err         error
	closed      bool
	trees       *TreesRepoType
}

func (trees *TreesRepoType) NewScannerFromID(treeID gr.TreeID, start gr.Key) (*treeIterator, error) {
	treeSlice, err := trees.GetTree(treeID)
	if err != nil {
		return nil, err
	}
	return trees.newScanner(treeSlice, start)
}

func (trees *TreesRepoType) NewScannerFromTreeParts(treeSlice TreeType, start gr.Key) (*treeIterator, error) {
	return trees.newScanner(treeSlice, start)
}

func (trees *TreesRepoType) newScanner(treeSlice TreeType, start gr.Key) (*treeIterator, error) {
	partNum := findPartNumForPath(treeSlice, start)
	if partNum >= len(treeSlice) {
		return nil, ErrPathBiggerThanMaxPath
	}
	partName := treeSlice[partNum].PartName
	partIterator, err := trees.PartManger.NewSSTableIterator(partName, start)
	if err != nil {
		return nil, err
	}
	scanner := &treeIterator{
		//treeID:      treeID,
		TreeParts:   treeSlice,
		currentIter: partIterator,
		currentPart: partNum,
		trees:       trees,
	}
	return scanner, nil
}

func (t *treeIterator) SeekGE(start gr.Key) bool {
	var err error
	partNum := findPartNumForPath(t.TreeParts, start)
	if partNum != t.currentPart {
		t.currentPart = partNum
		t.currentIter.Close()
		t.currentIter, err = t.trees.PartManger.NewSSTableIterator(t.TreeParts[partNum].PartName, start)
		if err != nil {
			t.err = err
			return false
		}
		return true
	}
	return t.currentIter.SeekGE(start)
}

func findPartNumForPath(tree TreeType, path gr.Key) int {
	n := len(tree)
	pos := sort.Search(n, func(i int) bool {
		return bytes.Compare(tree[i].MaxKey, path) >= 0
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
	if t.currentPart >= len(t.TreeParts)-1 {
		t.closed = true
		return false
	}
	t.currentPart++
	requiredPartName := t.TreeParts[t.currentPart].PartName
	t.currentIter, err = t.trees.PartManger.NewSSTableIterator(requiredPartName, nil)
	if err != nil {
		t.currentIter.Close()
		t.closed = true
		t.err = err
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

func (t *treeIterator) Value() *gr.ValueRecord {
	if t.currentIter == nil || t.closed {
		return nil
	}
	return t.currentIter.Value()
}

func (t *treeIterator) Close() {
	if t.currentIter == nil {
		return
	}
	t.currentIter.Close()
}

func (trees TreesRepoType) GetTree(treeID gr.TreeID) (TreeType, error) {
	t, exists := trees.TreesMap.Get(string(treeID))
	if exists {
		tree := t.(TreeType)
		return tree, nil
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
	trees.TreesMap.Set(string(treeID), treeSlice)
	return treeSlice, nil
}
