package upload

import (
	"crypto/sha256"
	"fmt"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"
	"hash"
	"io"
)

func uuidAsHex() string {
	id := [16]byte(uuid.New())
	return fmt.Sprintf("%x", id)
}

type Blob struct {
	Blocks   []*model.Block
	Checksum string
	Size     int64
}

type sha256Reader struct {
	sha256         hash.Hash
	originalReader io.Reader
	copiedSize     int64
}

func (s *sha256Reader) Read(p []byte) (int, error) {
	len, err := s.originalReader.Read(p)
	if len > 0 {
		s.sha256.Write(p[0:len])
		s.copiedSize += int64(len)
	}
	return len, err
}

func newsha256Reader() (s *sha256Reader) {
	s = new(sha256Reader)
	s.sha256 = sha256.New()
	return
}

func (s *sha256Reader) Seek(offset int64, whence int) (int64, error) {
	panic("Seek was called while reading in upload\n")
}

func WriteBlob(index index.Index, bucketName string, body io.Reader, adapter block.Adapter) (*Blob, error) {
	// handle the upload itself

	shaReader := newsha256Reader()
	shaReader.originalReader = body
	objName := uuidAsHex()
	err := adapter.Put(bucketName, objName, shaReader)
	if err != nil {
		panic("could not copy object to destination\n")
	}
	block := new(model.Block)
	dedupId := shaReader.sha256.Sum(nil)
	objName = index.CreateDedupEntryIfNone(bucketName, dedupId, objName)
	block.Address = objName
	block.Size = shaReader.copiedSize
	blob := new(Blob)
	blob.Blocks = append(blob.Blocks, block)
	blob.Checksum = objName
	blob.Size = block.Size
	return blob, nil

}
