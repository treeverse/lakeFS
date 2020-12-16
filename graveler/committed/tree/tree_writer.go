package tree

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"sort"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/sstable"
)

const (
	SplitFactor    = 200_000 // avarage number of entries in a part file. used to compute modulo on hash of path
	SplitMaxfactor = 6       // a part will be closed if number of entries written to it exceed splitFactor * splitMaxFactor
	SplitMinFactor = 50      // a part will not be closed in number of rows less than splitFactor / SplitMinFactor
)

type isSplitKeyFunc func(path graveler.Key, rowNum int) bool

type treeWriter struct {
	activeWriter             sstable.Writer
	entriesWrittenToThisPart int
	closeAsync               sstable.BatchWriterCloser
	isSplitKeyFunc           isSplitKeyFunc
	splitFactor              int
	trees                    TreeRepo
	additionalPats           []treePart
}

func (trees *treeRepo) NewTreeWriter(splitFactor int, closeAsync sstable.BatchWriterCloser) TreeWriter {
	return &treeWriter{
		trees:       trees,
		closeAsync:  closeAsync,
		splitFactor: splitFactor,
	}
}

func (trees *treeRepo) GetPartWriter() (sstable.Writer, error) {
	w, err := trees.partManger.GetWriter()
	if err != nil {
		return nil, err
	}
	return w, nil
}
func (trees treeRepo) NewTreeWriterOnBaseTree(splitFactor int, closeAsync sstable.BatchWriterCloser, treeID graveler.TreeID) TreeWriterOnBaseTree {
	panic("NewTreeWriterOnBaseTree not implemented yet")
}

func (trees treeRepo) GetIteratorsForDiff(LeftTree, RightTree graveler.TreeID) (graveler.ValueIterator, graveler.ValueIterator) {
	panic("NewTreeWriterOnBaseTree not implemented yet")
}

func (tw *treeWriter) HasOpenWriter() bool {
	return !(tw.activeWriter == nil)
}

// AddParts is used when creating a tree based on previous tree (e.g. Apply , Merge)
// it enables the tree creator to pass parts from the base tree that can be used "as is"
func (tw *treeWriter) AddParts(parts Tree) error {
	/* todo: add parts should check that there is no overlap between added parts, and
	parts that already exist in the writer.
	This is not possible currently as the name of tree is calculated by SSTable. SSTable Close is
	Asynchronous, so new treeParts ar known only at the end of writing the tree. If there is an overlap
	between new parts and added parts
	can be solved by moving the SHA256 calculation from SSTable to Tree.*/
	tw.additionalPats = append(tw.additionalPats, parts.treeSlice...)
	return nil
}

func (tw *treeWriter) WriteValue(record graveler.ValueRecord) error {
	if tw.activeWriter == nil {
		pw, err := tw.trees.GetPartWriter()
		if err != nil {
			return err
		}
		tw.activeWriter = pw
	}
	err := tw.activeWriter.WriteRecord(record)
	if err != nil {
		return err
	}
	tw.entriesWrittenToThisPart++
	if tw.IsSplitKey(record.Key, tw.entriesWrittenToThisPart) {
		tw.ForceCloseCurrentPart()
	}
	return nil
}

func (tw *treeWriter) ForceCloseCurrentPart() {
	tw.closeAsync.CloseWriterAsync(tw.activeWriter)
	tw.entriesWrittenToThisPart = 0
	tw.activeWriter = nil
}

func (tw *treeWriter) IsSplitKey(key graveler.Key, rowNum int) bool {
	if tw.isSplitKeyFunc != nil {
		return tw.isSplitKeyFunc(key, rowNum)
	}
	if tw.splitFactor == 0 {
		tw.splitFactor = SplitFactor
	}
	if rowNum >= tw.splitFactor*SplitMaxfactor {
		fmt.Printf(" forced split  \n")
		return true
	}
	fnvHash := fnv.New32a()
	fnvHash.Write(key)
	i := fnvHash.Sum32() % uint32(tw.splitFactor)
	minRowNum := int(tw.splitFactor / SplitMinFactor)
	result := i == 0 && rowNum > minRowNum
	if i == 0 && !result {
		fmt.Printf(" found split, number %d,minRownum %d \n", rowNum, minRowNum)
	}
	return result
}

func (tw *treeWriter) FlushIterToTree(iter graveler.ValueIterator) error {
	for iter.Next() {
		err := tw.WriteValue(*iter.Value())
		if err != nil {
			return err
		}
	}
	return iter.Err()
}
func (tw *treeWriter) SaveTree() (graveler.TreeID, error) {
	return tw.SaveTreeWithReusedParts(Tree{})
}
func (tw *treeWriter) SaveTreeWithReusedParts(reuseTree Tree) (graveler.TreeID, error) {
	reuseParts := reuseTree.treeSlice
	var newParts []treePart
	closeResults, err := tw.closeAsync.Wait()
	if err != nil {
		return "", fmt.Errorf(" closing of apply parts failed : %w", err)
	}
	for _, r := range closeResults {
		t := treePart{
			MaxKey:   r.Last,
			PartName: r.SSTableID,
		}
		newParts = append(newParts, t)
	}
	if reuseParts != nil {
		newParts = append(newParts, reuseParts...)
		sort.Slice(newParts, func(i, j int) bool { return bytes.Compare(newParts[i].MaxKey, newParts[j].MaxKey) < 0 })
	}
	return serializeTreeToDisk(newParts)
}

func serializeTreeToDisk(tree []treePart) (graveler.TreeID, error) {
	if len(tree) == 0 {
		return "", ErrEmptyTree
	}
	sha := sha256.New()
	for _, p := range tree {
		sha.Write([]byte(p.PartName))
		sha.Write([]byte(p.MaxKey))
	}
	hash := sha256.Sum256(nil)
	treeBytes, err := json.Marshal(tree)
	if err != nil {
		return "", err
	}
	treeID := graveler.TreeID(hex.EncodeToString(hash[:]))
	err = ioutil.WriteFile("tree_"+string(treeID)+".json", treeBytes, 0666)
	if err != nil {
		return graveler.TreeID(""), err
	}
	return treeID, nil
}
