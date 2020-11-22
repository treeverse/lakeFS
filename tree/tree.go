package tree

import (
	"encoding/base64"
	"encoding/json"
	"hash/fnv"
	"io/ioutil"

	"github.com/treeverse/lakefs/tree/sstable"
)

const SplitFactor = 200_000 // avarage number of entries in a part file. used to compute modulo on hash of path

type TreePartType struct {
	PartName string `json:"part_name"`
	MaxPath  string `json:"max_path"`
}
type treeSliceType []TreePartType
type TreeType struct {
	TreeID    TreeID
	TreeSlice []TreePartType
}

type TreesRepoType map[TreeID]TreeType

var treeRepository TreesRepoType

type treeScanner struct {
	TreeType
	currentIter sstable.EntryIterator
	currentSst  int
	currentKey  []byte
}

func InitTreeRepository() TreesRepoType {
	treeRepository = make(TreesRepoType, 100)
	return treeRepository
}

func isSplitPath(path []byte) bool {
	fnv := fnv.New32a()
	fnv.Write(path)
	i := fnv.Sum32()
	return (i % SplitFactor) == 0
}

func (trees TreesRepoType) loadTreeIfNeeded(treeID TreeID) (TreeType, error) {
	t, exists := trees[treeID]
	if exists {
		return t, nil
	}
	fName := base64.StdEncoding.EncodeToString(treeID[:]) + ".json"
	jsonBytes, err := ioutil.ReadFile(fName)
	if err != nil {
		return nil, err
	}
	treeSlice := make(treeSliceType, 0)
	err = json.Unmarshal(jsonBytes, &treeSlice)
	if err != nil {
		return nil, err
	}
	t := TreeType{
		treeID,
		treeSlice,
	}
	trees[treeID] = t
	return t, nil
}

func (trees TreesRepoType) NewScanner(tree TreeID, start string) (*treeScanner, error) {
	t, err := trees.loadTreeIfNeeded(tree)
	if err != nil {
		return nil, err
	}
	tr := TreeType{
		TreeID:    tree,
		TreeSlice: t,
	}
	scanner := &treeScanner{
		TreeType:   tr,
		currentKey: []byte(start),
	}
}

/*
type CommittedManager interface {
	// GetEntry returns the provided path, if exists, from the provided TreeID
	GetEntry(TreeID, Path) (*Entry, error)

	// ListEntries takes a given tree and returns an EntryIterator seeked to >= "from" path
	ListEntries(TreeID, from Path) (EntryIterator, error)

	// Diff receives two trees and a 3rd merge base tree used to resolve the change type
	//it tracks changes from left to right, returning an iterator of Diff entries
	Diff(left, right, base TreeID, from Path) (DiffIterator, error)

	// Merge receives two trees and a 3rd merge base tree used to resolve the change type
	// it applies that changes from left to right, resulting in a new tree that
	// is expected to be immediately addressable
	Merge(left, right, base TreeID) (TreeID, error)

	// Apply is the act of taking an existing tree (snapshot) and applying a set of changes to it.
	// A change is either an entity to write/overwrite, or a tombstone to mark a deletion
	// it returns a new treeID that is expected to be immediately addressable
	Apply(TreeID, EntryIterator) (TreeID, error)
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
