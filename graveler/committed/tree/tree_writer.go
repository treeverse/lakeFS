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

// IsSplitKeyFunc is a type for a function that accepts a key and the row number in the current sst.
// It determines if the tree writer will close the sst after this line is written. For more info see
// IsSplitKey function
type IsSplitKeyFunc func(key graveler.Key, rowNum int) bool

// treeWriter manages the writing of a full tree
type treeWriter struct {
	activeWriter             sstable.Writer
	entriesWrittenToThisPart int
	closeAsync               sstable.BatchWriterCloser
	isSplitKeyFunc           IsSplitKeyFunc
	splitFactor              int
	trees                    TreeRepo
	additionalPats           []part
}

func (trees *treeRepo) NewTreeWriter(splitFactor int, closeAsync sstable.BatchWriterCloser) TreeWriter {
	return &treeWriter{
		trees:       trees,
		closeAsync:  closeAsync,
		splitFactor: splitFactor,
	}
}

// NewTreeWriterOnBaseTree - currently in the concept phase
// The idea is to create a tree writer that will  create new parts of a table, while using the non-changed parts from the base tree in the new tree
// basically it is a different way ti implement apply, hopefully is a more reusable way
func (trees treeRepo) NewTreeWriterOnBaseTree(splitFactor int, closeAsync sstable.BatchWriterCloser, treeID graveler.TreeID) TreeWriterOnBaseTree {
	panic("NewTreeWriterOnBaseTree not implemented yet")
}

// getPartWriter returns an sstable Writer object in current implementation
func (trees *treeRepo) getPartWriter() (sstable.Writer, error) {
	w, err := trees.partManger.GetWriter()
	if err != nil {
		return nil, err
	}
	return w, nil
}

// GetIteratorsForDiff accepts two trees that will be diffed. it returns two iterators that do not contain those parts of the trees that are
// identical in both trees.

func (trees treeRepo) GetIteratorsForDiff(LeftTree, RightTree graveler.TreeID) (graveler.ValueIterator, graveler.ValueIterator) {
	panic("NewTreeWriterOnBaseTree not implemented yet")
}

func (tw *treeWriter) HasOpenWriter() bool {
	return !(tw.activeWriter == nil)
}

// AddParts is used when creating a tree based on previous tree (e.g. Apply , Merge)
// it enables the tree creator to pass parts from the base tree that can be used "as is"
func (tw *treeWriter) AddParts(parts Tree) error {
	// todo: add parts should check that there is no overlap between added parts, and
	// parts that already exist in the writer.
	// This is not possible currently as the name of tree is calculated by SSTable. SSTable Close is
	// Asynchronous, so new treeParts ar known only at the end of writing the tree. If there is an overlap
	// between new parts and added parts
	// can be solved by changing sstable to return the file name on close, or moving the SHA256 calculation from SSTable to Tree.
	tw.additionalPats = append(tw.additionalPats, parts.treeSlice...)
	return nil
}

func (tw *treeWriter) WriteValue(record graveler.ValueRecord) error {
	if tw.activeWriter == nil {
		pw, err := tw.trees.getPartWriter()
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
		tw.ClosePart()
	}
	return nil
}

// ClosePart closes the current treeWriter open part.
func (tw *treeWriter) ClosePart() {
	tw.closeAsync.CloseWriterAsync(tw.activeWriter)
	tw.entriesWrittenToThisPart = 0
	tw.activeWriter = nil
}

// IsSplitKey determines if a key should make the writer close the part.
// background: we need to divide the tree into reasonably-sized parts. too big parts will be fully rewritten whenever
// one line changes, and may be slow to copy from s3 to local disk. too small parts will create overhead.
// The exact size of each part is not important. the average part size should be reasonable. All parts should
// not be within a pre-set lower and upper bounds.
// another critical requirement is: if two trees contain an identical long consecutive list of object - the parts that
// contain those objects should be identical. this will save processing time, storage and cache memory.
// if we divide objects to parts randomly - most probably the identical lists will be divided differently in each tree.
// Our solution is to decide on closing a part by the modulo of a hash of the object key. the modulo number is stored
// in treeWriter.splitFactor. It is set on treeRepository initialization. If zero the number is  const SplitFactor.
// IsSplitKey will not return true if the number of rows is too small. It will force split if the number of rows
// is too high.
// in addition, a split function may be injected via IsSplitKeyFunc. May be useful for testing
func (tw *treeWriter) IsSplitKey(key graveler.Key, rowNum int) bool {
	if tw.isSplitKeyFunc != nil {
		return tw.isSplitKeyFunc(key, rowNum)
	}
	if tw.splitFactor == 0 {
		tw.splitFactor = SplitFactor
	}
	if rowNum >= tw.splitFactor*SplitMaxfactor { // set split if part will be too big
		return true
	}
	fnvHash := fnv.New32a()
	fnvHash.Write(key)
	moduluResult := fnvHash.Sum32() % uint32(tw.splitFactor)
	minRowNum := int(tw.splitFactor / SplitMinFactor)
	result := moduluResult == 0 && rowNum > minRowNum // do not create too small parts
	return result
}

// FlushIterToTree - a convenience function that will write the iterator content to the tree
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
	if tw.activeWriter != nil {
		tw.closeAsync.CloseWriterAsync(tw.activeWriter)
	}
	reuseParts := reuseTree.treeSlice
	var newParts []part
	closeResults, err := tw.closeAsync.Wait()
	if err != nil {
		return "", fmt.Errorf(" closing of apply parts failed : %w", err)
	}
	for _, r := range closeResults {
		t := part{
			MaxKey:   r.Last,
			PartName: r.SSTableID,
		}
		newParts = append(newParts, t)
	}
	if reuseParts != nil {
		newParts = append(newParts, reuseParts...)
		sort.Slice(newParts, func(i, j int) bool { return bytes.Compare(newParts[i].MaxKey, newParts[j].MaxKey) < 0 })
	}
	return serializeTreeToDisk("testdata/", newParts)
}

// jsonTreePart enables easy serde of Tree to json and back.
// reason: json serialization converts bytes to base64. makes the json unreadable for humans
type jsonTreePart struct {
	PartName        string `json:"part_name"`
	MaxKey          string `json:"max_key"`
	MinKey          string `json:"min_key"`
	NumberOfRecords int    `json:"number_of_records"`
}

func serializeTreeToDisk(path string, tree []part) (graveler.TreeID, error) {
	if len(tree) == 0 {
		return "", ErrEmptyTree
	}
	sha := sha256.New()
	for _, p := range tree {
		sha.Write([]byte(p.PartName))
		sha.Write([]byte(p.MaxKey))
	}
	hash := sha256.Sum256(nil)
	var jsonTree []jsonTreePart
	for _, part := range tree {
		jsonTree = append(jsonTree, jsonTreePart{
			PartName:        string(part.PartName),
			MaxKey:          string(part.MaxKey),
			MinKey:          string(part.MinKey),
			NumberOfRecords: part.NumberOfRecords,
		})
	}
	treeBytes, err := json.Marshal(jsonTree)
	if err != nil {
		return "", err
	}
	treeID := graveler.TreeID(hex.EncodeToString(hash[:]))
	//todo: currently written always to testdata. fix during integration with tierFS
	treeFileName := "testdata/tree_" + string(treeID) + ".json"
	err = ioutil.WriteFile(treeFileName, treeBytes, 0666)
	if err != nil {
		return graveler.TreeID(""), err
	}
	return treeID, nil
}
