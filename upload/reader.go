package upload

import (
	"crypto/sha256"
	"fmt"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"hash"
	"io"
)

func uuidAsHex() string {
	id := [16]byte(uuid.New())
	return fmt.Sprintf("%x", id)
}

type Blob struct {
	name     string
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

func ReadBlob(bucketName string, body io.Reader, adapter block.Adapter) (*Blob, error) {
	// handle the upload itself
	shaReader := newsha256Reader()
	shaReader.originalReader = body
	objName := uuidAsHex()
	err := adapter.Put(bucketName, objName, shaReader)
	if err != nil {
		panic("could not copy object to destination\n")
	}

	return &Blob{
		name:     objName,
		Checksum: fmt.Sprintf("%x", shaReader.sha256.Sum(nil)),
		Size:     shaReader.copiedSize,
	}, nil
}
