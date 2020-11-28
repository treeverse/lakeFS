package tree

import (
	"hash/fnv"

	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/tree/sstable"
)

type closeReply struct {
	err error
	TreePartType
}

type partsWriter struct {
	statusChan      chan closeReply
	pendingCloseNum int
	activeWriter    *sstable.Writer
	partEntryNum    int
	lastKey         rocks.Path
}

func newPartsWriter() *partsWriter {
	ret := new(partsWriter)
	ret.statusChan = make(chan closeReply, MaxStatusChan)
	return ret
}

func (p *partsWriter) hasOpenWriter() bool {
	return !(p.activeWriter == nil)
}

func (p *partsWriter) writeEntry(k rocks.Path, v *rocks.Entry) error {
	var err error
	p.lastKey = k
	if p.activeWriter == nil {
		w, err := treesRepository.PartManger.GetWriter()
		if err == nil {
			return err
		}
		p.activeWriter = &w
	}
	err = *p.activeWriter.WriteEntry(k, v)
	if err != nil {
		return err
	}
	p.partEntryNum++
	if isSplitPath(k, p.partEntryNum) {
		p.forceCloseCurrentPart()
	}
}

func (p *partsWriter) forceCloseCurrentPart() {
	go closePart(p.activeWriter, &p.lastKey, p.statusChan)
	p.pendingCloseNum++
	p.partEntryNum = 0
	p.activeWriter = nil
}

func (p *partsWriter) waitCloseTermination() (TreePartsType, error) {
	res := make(TreePartsType, 0, p.pendingCloseNum)
	for i := 0; i < p.pendingCloseNum; i++ {
		part := <-p.statusChan
		if part.err != nil {
			return nil, part.err
		}
		res = append(res, part.TreePartType)
	}
	return res, nil
}
func (p *partsWriter) closePartsWriter() {
	go closePart(p.activeWriter, &largestByteArray)
	p.pendingCloseNum++
}
func closePart(writer *sstable.Writer, lastKey rocks.Path, statusChan chan closeReply) {
	reply := closeReply{err: ErrCloseCrashed}
	reply.MaxPath = lastKey
	defer func() {
		statusChan <- reply
	}()
	res, err := writer.Close()
	reply.err = err
	reply.PartName = res.sstable_ID
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

func (pw *partsWriter) flushIterToPartsWriter(iter *pushBackEntryIterator) error {
	for iter.Next() {
		err := pw.writeEntry(iter.Value())
		if err != nil {
			return err
		}
	}
	return iter.Err()
}
