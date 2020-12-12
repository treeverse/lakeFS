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

	"github.com/treeverse/lakefs/forest/sstable"
	gr "github.com/treeverse/lakefs/graveler"
)

const (
	SplitFactor    = 200_000 // avarage number of entries in a part file. used to compute modulo on hash of path
	SplitMaxfactor = 6       // a part will be closed if number of entries written to it exceed splitFactor * splitMaxFactor
	SplitMinFactor = 50      // a part will not be closed in number of rows less than splitFactor / SplitMinFactor
)

type isSplitKeyFunc func(path gr.Key, rowNum int) bool

type treeWriter struct {
	activeWriter             sstable.Writer
	entriesWrittenToThisPart int
	closeAsync               sstable.BatchWriterCloser
	isSplitKeyFunc           isSplitKeyFunc
	splitFactor              int
	trees                    TreeRepo
}

func (trees *treesRepo) NewTreeWriter(splitFactor int, closeAsync sstable.BatchWriterCloser) TreeWriter {
	return &treeWriter{
		//trees : trees
		closeAsync:  closeAsync,
		splitFactor: splitFactor,
	}
}

func (tw *treeWriter) HasOpenWriter() bool {
	return !(tw.activeWriter == nil)
}

func (tw *treeWriter) WriteEntry(record gr.ValueRecord) error {
	if tw.activeWriter == nil {
		w, err := tw.trees.GetPartManger().GetWriter()
		if err != nil {
			return err
		}
		tw.activeWriter = w
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

func (tw *treeWriter) IsSplitKey(key gr.Key, rowNum int) bool {
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

func (tw *treeWriter) FlushIterToTree(iter gr.ValueIterator) error {
	for iter.Next() {
		err := tw.WriteEntry(*iter.Value())
		if err != nil {
			return err
		}
	}
	return iter.Err()
}

func (tw *treeWriter) SaveTree(reuseTree TreeSlice) (gr.TreeID, error) {
	reuseParts := reuseTree.treeSlice
	var newParts []TreePart
	closeResults, err := tw.closeAsync.Wait()
	if err != nil {
		return "", fmt.Errorf(" closing of apply parts failed : %w", err)
	}
	for _, r := range closeResults {
		t := TreePart{
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

func serializeTreeToDisk(tree []TreePart) (gr.TreeID, error) {
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
	treeID := gr.TreeID(hex.EncodeToString(hash[:]))
	err = ioutil.WriteFile("tree_"+string(treeID)+".json", treeBytes, 0666)
	if err != nil {
		return gr.TreeID(""), err
	}
	return treeID, nil
}
