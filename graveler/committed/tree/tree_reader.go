package tree

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"sort"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/sstable"
)

type treeRepo struct {
	treesMap   Cache
	partManger sstable.Manager
}

type Cache interface {
	Get(key string) (interface{}, bool)
	Set(key string, val interface{})
}

type treeIterator struct {
	tree        []treePart
	currentIter graveler.ValueIterator
	currentPart int
	err         error
	closed      bool
	trees       *treeRepo
}

const (
	CacheMapSize     = 1000 // number of cached trees
	CacheTrimSize    = 100  // number of trees to evict once cache reaches maximum
	InitialWeight    = 64   // weight a cached tree gets when it is added
	AdditionalWeight = 16   // additional weight gained each time the tree is accessed
	TrimFactor       = 1    //
)

// InitTreeRepository creates the tree cache, and stores part Manager for operations of parts (currently implemented as sstables).
// should be called at process init.
// decisions on who calls it and how to get a treesRepository will be taken later
func InitTreesRepository(cache Cache, manager sstable.Manager) *treeRepo {
	return &treeRepo{
		//treesMap:   cache.NewCacheMap(CacheMapSize, CacheTrimSize, InitialWeight, AdditionalWeight, TrimFactor),
		treesMap:   cache,
		partManger: manager,
	}
}

func (trees *treeRepo) GetValue(treeID graveler.TreeID, key graveler.Key) (*graveler.ValueRecord, error) {
	tree, err := trees.GetTree(treeID)
	if err != nil {
		return nil, err
	}
	partIterator, _, err := trees.getSSTIteratorForKey(tree, key)
	if err != nil {
		return nil, err
	}
	defer partIterator.Close()
	partIterator.SeekGE(key)
	if partIterator.Next() {
		val := partIterator.Value()
		if bytes.Equal(val.Key, key) {
			return val, nil
		} else {
			return nil, ErrNotFound
		}
	}
	return nil, partIterator.Err()
}

//func (t *treeRepo) getPartManger() sstable.Manager {
//	return t.partManger
//}

func (trees *treeRepo) NewIteratorFromTreeID(treeID graveler.TreeID, start graveler.Key) (graveler.ValueIterator, error) {
	tree, err := trees.GetTree(treeID)
	if err != nil {
		return nil, err
	}
	return trees.newIterator(tree, start)
}

func (trees *treeRepo) NewIteratorFromTreeObject(treeSlice Tree, start graveler.Key) (graveler.ValueIterator, error) {
	return trees.newIterator(treeSlice, start)
}
func (trees *treeRepo) getSSTIteratorForKey(tree Tree, key graveler.Key) (graveler.ValueIterator, int, error) {
	treeSlice := tree.treeSlice
	partNum := findPartNumForPath(treeSlice, key)
	if partNum >= len(treeSlice) {
		return nil, 0, ErrPathBiggerThanMaxPath
	}
	partName := treeSlice[partNum].PartName
	partIterator, err := trees.partManger.NewSSTableIterator(partName, key)
	if err != nil {
		return nil, 0, err
	}
	return partIterator, partNum, nil
}

func (trees *treeRepo) newIterator(tree Tree, from graveler.Key) (graveler.ValueIterator, error) {
	partIterator, partNum, err := trees.getSSTIteratorForKey(tree, from)
	if err != nil {
		return nil, err
	}
	scanner := &treeIterator{
		//treeID:      treeID,
		tree:        tree.treeSlice,
		currentIter: partIterator,
		currentPart: partNum,
		trees:       trees,
	}
	return scanner, nil
}

func (trees *treeRepo) GetTree(treeID graveler.TreeID) (Tree, error) {
	t, exists := trees.treesMap.Get(string(treeID))
	if exists {
		tree := t.(Tree)
		return tree, nil
	}
	fName := string(treeID) + ".json"
	jsonBytes, err := ioutil.ReadFile(fName)
	if err != nil {
		return Tree{}, err
	}
	treeSlice := make([]treePart, 0)
	err = json.Unmarshal(jsonBytes, &treeSlice)
	if err != nil {
		return Tree{}, err
	}
	tree := Tree{treeSlice: treeSlice}
	trees.treesMap.Set(string(treeID), tree)
	return tree, nil
}

func (t *treeIterator) SeekGE(start graveler.Key) {
	var err error
	partNum := findPartNumForPath(t.tree, start)
	if partNum != t.currentPart {
		t.currentPart = partNum
		t.currentIter.Close()
		t.currentIter, err = t.trees.partManger.NewSSTableIterator(t.tree[partNum].PartName, start)
		if err != nil {
			t.err = err
			return
		}
	}
	t.currentIter.SeekGE(start)
}

func findPartNumForPath(tree []treePart, path graveler.Key) int {
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
	// assert:  if Next returned false and err == nil - reached end of part
	if t.currentPart >= len(t.tree)-1 {
		t.closed = true
		return false
	}
	t.currentPart++
	requiredPartName := t.tree[t.currentPart].PartName
	t.currentIter, err = t.trees.partManger.NewSSTableIterator(requiredPartName, nil)
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

func (t *treeIterator) Value() *graveler.ValueRecord {
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
	t.closed = true
}
