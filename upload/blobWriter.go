package upload

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"hash"
	"io"
)

// WriteBlob needs only this function from index. created this interface to enable easy testing
type DedupHandler interface {
	CreateDedupEntryIfNone(repoId string, dedupId string, objName string) (string, error)
}

type MockDedup struct {
	DedupIndex map[string]string
}

func NewMockDedup() *MockDedup {
	m := make(map[string]string)
	return &MockDedup{DedupIndex: m}
}

func (d *MockDedup) CreateDedupEntryIfNone(repoId string, dedupId string, objName string) (string, error) {
	existingObj, ok := d.DedupIndex[dedupId]
	if ok {
		return existingObj, nil
	} else {
		return objName, nil
	}
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

func (s *HashingReader) Close() error {
	return nil
}

func newHashingReader(body io.Reader) (s *HashingReader) {
	s = new(HashingReader)
	s.sha256 = sha256.New()
	s.md5 = md5.New()
	s.originalReader = body
	return
}

func WriteBlob(index DedupHandler, repoId, bucketName string, body io.Reader, adapter block.Adapter, contentLength int64) (string, string, int64, error) {
	// handle the upload itself
	hashReader := newHashingReader(body)
	UUIDbytes := ([16]byte(uuid.New()))
	objName := hex.EncodeToString(UUIDbytes[:])
	err := adapter.Put(bucketName, objName, contentLength, hashReader)
	if err != nil {
		return "", "", -1, err
	}
	dedupId := hex.EncodeToString(hashReader.sha256.Sum(nil))
	checksum := hex.EncodeToString(hashReader.md5.Sum(nil))
	existingName, err := index.CreateDedupEntryIfNone(repoId, dedupId, objName)
	if err != nil {
		return "", "", -1, err
	}
	if existingName != objName { // object already exist
		adapter.Remove(bucketName, objName)
		objName = existingName
	}

	return checksum, objName, hashReader.copiedSize, nil

}
