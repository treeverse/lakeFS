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
	MaxStatusChan  = 10_000  // practically unlimitted number of close messages waiting for the apply to terminate
)

// used as the highest path of the last part in a tree
//var maximalPath = rocks.Path([]byte{255, 255, 255, 255})

type closeReply struct {
	err error
	TreePartType
}

type newTreeManager struct {
	statusChan      chan closeReply
	pendingCloseNum int
	activeWriter    *sstable.Writer
	partEntryNum    int
}

func (p *newTreeManager) hasOpenWriter() bool {
	return !(p.activeWriter == nil)
}

func (p *newTreeManager) writeEntry(k rocks.Path, v *rocks.Entry) error {
	if p.activeWriter == nil {
		w, err := (*treesRepository.PartManger).GetWriter()
		if err == nil {
			return err
		}
		p.activeWriter = &w
	}
	err := (*p.activeWriter).WriteEntry(k, *v)
	if err != nil {
		return err
	}
	p.partEntryNum++
	if isSplitPath(k, p.partEntryNum) {
		p.forceCloseCurrentPart()
	}
}

func (p *newTreeManager) forceCloseCurrentPart() {
	go closePart(p.activeWriter, p.statusChan)
	p.pendingCloseNum++
	p.partEntryNum = 0
	p.activeWriter = nil
}

func (p *newTreeManager) waitCloseTermination() (TreeType, error) {
	res := make(TreeType, 0, p.pendingCloseNum)
	for i := 0; i < p.pendingCloseNum; i++ {
		part := <-p.statusChan
		if part.err != nil {
			return nil, part.err
		}
		res = append(res, part.TreePartType)
	}
	return res, nil
}

func closePart(writer *sstable.Writer, statusChan chan closeReply) {
	reply := closeReply{err: ErrCloseCrashed}
	res, err := writer.Close()
	reply.MaxPath = res.lastKey
	reply.err = err
	reply.PartName = res.sstable_ID
	statusChan <- reply
}

func isSplitPath(path []byte, rowNum int) bool {
	if rowNum >= SplitFactor*SplitMaxfactor {
		return true
	}
	fnv := fnv.New32a()
	fnv.Write(path)
	i := fnv.Sum32()
	return (i%SplitFactor) == 0 && rowNum > (SplitFactor/SplitMinFactor)
}

func (pw *newTreeManager) flushIterToNewTree(iter *pushBackEntryIterator) error {
	for iter.Next() {
		err := pw.writeEntry(iter.Value())
		if err != nil {
			return err
		}
	}
	return iter.Err()
}

func (ntm *newTreeManager) finalizeTree(reuseParts TreeType) (rocks.TreeID, error) {
	newParts, err := ntm.waitCloseTermination()
	if err != nil {
		return "", fmt.Errorf(" closing of apply parts failed : %w", err)
	}
	newTree := append(newParts, reuseParts...)
	sort.Slice(newTree, func(i, j int) bool { return newTree[i].MaxPath < newTree[j].MaxPath })
	return serializeTreeToDisk(newTree)
}

func serializeTreeToDisk(tree TreeType) (rocks.TreeID, error) {
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
