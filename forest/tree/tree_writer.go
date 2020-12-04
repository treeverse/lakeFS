package tree

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"sort"

	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/forest/sstable"
)

const (
	SplitFactor    = 200_000 // avarage number of entries in a part file. used to compute modulo on hash of path
	SplitMaxfactor = 3       // a part will be closed if number of entries written to it exceed splitFactor * splitMaxFactor
	SplitMinFactor = 50      // a part will not be closed in number of rows less than splitFactor / SplitMinFactor
)

// used as the highest path of the last part in a tree
//var maximalPath = rocks.Path([]byte{255, 255, 255, 255})

//type closeReply struct {
//	err error
//	TreePartType
//}
type IsSplitPathFunc func(path rocks.Path, rowNum int) bool

type TreeWriter struct {
	activeWriter            sstable.Writer
	currentPartNumOfEntries int
	closeAsync              sstable.BatchWriterCloser
	isSplitPathFunc         IsSplitPathFunc
	splitFactor             uint32
}

func (tw *TreeWriter) hasOpenWriter() bool {
	return !(tw.activeWriter == nil)
}

func (tw *TreeWriter) writeEntry(record rocks.EntryRecord) error {
	if tw.activeWriter == nil {
		w, err := treesRepository.PartManger.GetWriter()
		if err != nil {
			return err
		}
		tw.activeWriter = w
	}
	err := tw.activeWriter.WriteEntry(record)
	if err != nil {
		return err
	}
	tw.currentPartNumOfEntries++
	if tw.isSplitPath(record.Path, tw.currentPartNumOfEntries) {
		tw.forceCloseCurrentPart()
	}
	return nil
}

func (tw *TreeWriter) forceCloseCurrentPart() {
	tw.closeAsync.CloseWriterAsync(tw.activeWriter)
	tw.currentPartNumOfEntries = 0
	tw.activeWriter = nil
}

func (tw *TreeWriter) isSplitPath(path rocks.Path, rowNum int) bool {
	if tw.isSplitPathFunc != nil {
		return tw.isSplitPathFunc(path, rowNum)
	}
	if tw.splitFactor == 0 {
		tw.splitFactor = SplitFactor
	}
	if uint32(rowNum) >= tw.splitFactor*SplitMaxfactor {
		return true
	}
	fnvHash := fnv.New32a()
	fnvHash.Write([]byte(path))
	i := fnvHash.Sum32()
	return (i%tw.splitFactor) == 0 && rowNum > (SplitFactor/SplitMinFactor)
}

func (tw *TreeWriter) flushIterToNewTree(iter rocks.EntryIterator) error {
	for iter.Next() {
		err := tw.writeEntry(*iter.Value())
		if err != nil {
			return err
		}
	}
	return iter.Err()
}

func (tw *TreeWriter) finalizeTree(reuseParts *TreeType) (rocks.TreeID, error) {
	var newParts TreeType
	closeResults, err := tw.closeAsync.Wait()
	if err != nil {
		return "", fmt.Errorf(" closing of apply parts failed : %w", err)
	}
	for _, r := range closeResults {
		t := TreePartType{
			MaxPath:  r.Last,
			PartName: r.SSTableID,
		}
		newParts = append(newParts, t)
	}
	if reuseParts != nil {
		newParts = append(newParts, *reuseParts...)
		sort.Slice(newParts, func(i, j int) bool { return newParts[i].MaxPath < newParts[j].MaxPath })
	}
	return serializeTreeToDisk(newParts)
}

func serializeTreeToDisk(tree TreeType) (rocks.TreeID, error) {
	if len(tree) == 0 {
		return "", ErrEmptyTree
	}
	sha := sha256.New()
	for _, p := range tree {
		sha.Write([]byte(p.PartName))
		sha.Write([]byte(p.MaxPath))
	}
	hash := sha256.Sum256(nil)
	treeBytes, err := json.Marshal(tree)
	if err != nil {
		return "", err
	}
	treeID := hex.EncodeToString(hash[:])
	err = ioutil.WriteFile("tree_"+treeID+".json", treeBytes, 0666)
	return rocks.TreeID(treeID), err
}
