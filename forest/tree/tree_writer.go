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
	SplitMaxfactor = 3       // a part will be closed if number of entries written to it exceed splitFactor * splitMaxFactor
	SplitMinFactor = 50      // a part will not be closed in number of rows less than splitFactor / SplitMinFactor
)

type IsSplitKeyFunc func(path gr.Key, rowNum int) bool

type TreeWriter struct {
	activeWriter            sstable.Writer
	currentPartNumOfEntries int
	closeAsync              sstable.BatchWriterCloser
	isSplitKeyFunc          IsSplitKeyFunc
	splitFactor             uint32
}

func (tw *TreeWriter) hasOpenWriter() bool {
	return !(tw.activeWriter == nil)
}

func (tw *TreeWriter) writeEntry(record gr.ValueRecord) error {
	if tw.activeWriter == nil {
		w, err := treesRepository.PartManger.GetWriter()
		if err != nil {
			return err
		}
		tw.activeWriter = w
	}
	err := tw.activeWriter.WriteRecord(record)
	if err != nil {
		return err
	}
	tw.currentPartNumOfEntries++
	if tw.isSplitKey(record.Key, tw.currentPartNumOfEntries) {
		tw.forceCloseCurrentPart()
	}
	return nil
}

func (tw *TreeWriter) forceCloseCurrentPart() {
	tw.closeAsync.CloseWriterAsync(tw.activeWriter)
	tw.currentPartNumOfEntries = 0
	tw.activeWriter = nil
}

func (tw *TreeWriter) isSplitKey(key gr.Key, rowNum int) bool {
	if tw.isSplitKeyFunc != nil {
		return tw.isSplitKeyFunc(key, rowNum)
	}
	if tw.splitFactor == 0 {
		tw.splitFactor = SplitFactor
	}
	if uint32(rowNum) >= tw.splitFactor*SplitMaxfactor {
		return true
	}
	fnvHash := fnv.New32a()
	fnvHash.Write(key)
	i := fnvHash.Sum32()
	return (i%tw.splitFactor) == 0 && rowNum > (SplitFactor/SplitMinFactor)
}

func (tw *TreeWriter) flushIterToNewTree(iter gr.ValueIterator) error {
	for iter.Next() {
		err := tw.writeEntry(*iter.Value())
		if err != nil {
			return err
		}
	}
	return iter.Err()
}

func (tw *TreeWriter) finalizeTree(reuseParts *TreeType) (gr.TreeID, error) {
	var newParts TreeType
	closeResults, err := tw.closeAsync.Wait()
	if err != nil {
		return "", fmt.Errorf(" closing of apply parts failed : %w", err)
	}
	for _, r := range closeResults {
		t := TreePartType{
			MaxKey:   r.Last,
			PartName: r.SSTableID,
		}
		newParts = append(newParts, t)
	}
	if reuseParts != nil {
		newParts = append(newParts, *reuseParts...)
		sort.Slice(newParts, func(i, j int) bool { return bytes.Compare(newParts[i].MaxKey, newParts[j].MaxKey) < 0 })
	}
	return serializeTreeToDisk(newParts)
}

func serializeTreeToDisk(tree TreeType) (gr.TreeID, error) {
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
