package upload

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
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

type HashingReader struct {
	sha256         hash.Hash
	md5            hash.Hash
	originalReader io.Reader
	copiedSize     int64
}

func (s *HashingReader) Read(p []byte) (int, error) {
	len, err := s.originalReader.Read(p)
	if len > 0 {
		s.sha256.Write(p[0:len])
		s.md5.Write(p[0:len])
		s.copiedSize += int64(len)
	}
	return len, err
}

func newHashingReaderReader(body io.Reader) (s *HashingReader) {
	s = new(HashingReader)
	s.sha256 = sha256.New()
	s.md5 = md5.New()
	s.originalReader = body

	return
}

func (s *HashingReader) Seek(offset int64, whence int) (int64, error) {
	panic("Seek was called while reading in upload\n")
}

func WriteBlob(index index.Index, bucketName string, body io.Reader, adapter block.Adapter) (*Blob, error) {
	// handle the upload itself
	hashReader := newHashingReaderReader(body)
	objName := uuidAsHex()
	err := adapter.Put(bucketName, objName, hashReader)
	if err != nil {
		panic("could not copy object to destination\n")
	}
	Block := new(model.Block)
	Block.Size = hashReader.copiedSize
	dedupId := hex.EncodeToString(hashReader.sha256.Sum(nil))
	checksum := hex.EncodeToString(hashReader.md5.Sum(nil))
	existingName, err := index.CreateDedupEntryIfNone(bucketName, dedupId, objName)
	if existingName != objName { // object already exist
		adapter.Remove(bucketName, objName)
		objName = existingName
	}
	Block.Address = objName
	/*obj,err := index.ReadObjectByKey(bucketName,dedupId)
	if err == nil { // Object already exist under different name
		adapter.Remove(bucketName,objName)
		objName = obj.Checksum
	} else if !xerrors.Is(err, db.ErrNotFound) {
		panic("error reading object ")
		return nil,err
	}*/
	blob := new(Blob)
	blob.Blocks = append(blob.Blocks, Block)
	blob.Checksum = checksum
	blob.Size = Block.Size
	return blob, nil

}
