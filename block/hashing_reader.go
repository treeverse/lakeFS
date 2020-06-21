package block

import (
	"crypto/md5" //nolint:gosec
	"crypto/sha256"
	"hash"
	"io"
	"strconv"
)

const (
	HashFunctionMD5 = iota
	HashFunctionSHA256
)

type HashingReader struct {
	Md5            hash.Hash
	Sha256         hash.Hash
	originalReader io.Reader
	CopiedSize     int64
}

func (s *HashingReader) Read(p []byte) (int, error) {
	l, err := s.originalReader.Read(p)
	if l > 0 {
		s.CopiedSize += int64(l)
		if s.Md5 != nil {
			s.Md5.Write(p[0:l])
		}
		if s.Sha256 != nil {
			s.Sha256.Write(p[0:l])
		}
	}
	return l, err
}

func NewHashingReader(body io.Reader, hashTypes ...int) *HashingReader {
	s := new(HashingReader)
	s.originalReader = body
	for hashType := range hashTypes {
		if hashType == HashFunctionMD5 {
			s.Md5 = md5.New() //nolint:gosec
		} else if hashType == HashFunctionSHA256 {
			s.Sha256 = sha256.New()
		} else {
			panic("wrong hash type number " + strconv.Itoa(hashType))
		}
	}
	return s
}
