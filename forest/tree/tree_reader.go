package tree

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"sort"

	cache "github.com/treeverse/lakefs/forest/cache_map"
	"github.com/treeverse/lakefs/forest/sstable"

	gr "github.com/treeverse/lakefs/graveler"
)

var treesRepository treesRepo

type treeIterator struct {
	treeParts   []TreePart
	currentIter gr.ValueIterator
	currentPart int
	err         error
	closed      bool
	trees       *treesRepo
}

const (
	CacheMapSize     = 1000 // number of cached trees
	CacheTrimSize    = 100  // number of trees to evict once cache reaches maximum
	InitialWeight    = 64   // weight a cached tree gets when it is added
	AdditionalWeight = 16   // additional weight gained each time the tree is accessed
	TrimFactor       = 1    //
)

func InitTreesRepository(manager sstable.Manager) TreeRepo {
	treesRepository := &treesRepo{
		treesMap:   cache.NewCacheMap(CacheMapSize, CacheTrimSize, InitialWeight, AdditionalWeight, TrimFactor),
		partManger: manager,
	}
	return treesRepository
}

func (t *treesRepo) GetPartManger() sstable.Manager {
	return t.partManger
}

func (trees *treesRepo) NewIteratorFromTreeID(treeID gr.TreeID, start gr.Key) (gr.ValueIterator, error) {
	treeSlice, err := trees.GetTree(treeID)
	if err != nil {
		return nil, err
	}
	return trees.newIterator(treeSlice, start)
}

func (trees *treesRepo) NewIteratorFromTreeSlice(treeSlice TreeSlice, start gr.Key) (gr.ValueIterator, error) {
	return trees.newIterator(treeSlice, start)
}

func (trees *treesRepo) newIterator(tree TreeSlice, start gr.Key) (gr.ValueIterator, error) {
	treeSlice := tree.treeSlice
	partNum := findPartNumForPath(treeSlice, start)
	if partNum >= len(treeSlice) {
		return nil, ErrPathBiggerThanMaxPath
	}
	partName := treeSlice[partNum].PartName
	partIterator, err := trees.partManger.NewSSTableIterator(partName, start)
	if err != nil {
		return nil, err
	}
	scanner := &treeIterator{
		//treeID:      treeID,
		treeParts:   treeSlice,
		currentIter: partIterator,
		currentPart: partNum,
		trees:       trees,
	}
	return scanner, nil
}

func (trees *treesRepo) GetTree(treeID gr.TreeID) (TreeSlice, error) {
	t, exists := trees.treesMap.Get(string(treeID))
	if exists {
		tree := t.(TreeSlice)
		return tree, nil
	}
	fName := string(treeID) + ".json"
	jsonBytes, err := ioutil.ReadFile(fName)
	if err != nil {
		return TreeSlice{}, err
	}
	treeSlice := make([]TreePart, 0)
	err = json.Unmarshal(jsonBytes, &treeSlice)
	if err != nil {
		return TreeSlice{}, err
	}
	trees.treesMap.Set(string(treeID), treeSlice)
	return TreeSlice{treeSlice: treeSlice}, nil
}

func (t *treeIterator) SeekGE(start gr.Key) {
	var err error
	partNum := findPartNumForPath(t.treeParts, start)
	if partNum != t.currentPart {
		t.currentPart = partNum
		t.currentIter.Close()
		t.currentIter, err = t.trees.partManger.NewSSTableIterator(t.treeParts[partNum].PartName, start)
		if err != nil {
			t.err = err
			return
		}
	}
	t.currentIter.SeekGE(start)
}

func findPartNumForPath(tree []TreePart, path gr.Key) int {
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
	if t.currentPart >= len(t.treeParts)-1 {
		t.closed = true
		return false
	}
	t.currentPart++
	requiredPartName := t.treeParts[t.currentPart].PartName
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
