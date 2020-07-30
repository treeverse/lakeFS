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
	nb, err := s.originalReader.Read(p)
	s.CopiedSize += int64(nb)
	if s.Md5 != nil {
		if _, err2 := s.Md5.Write(p[0:nb]); err2 != nil {
			return nb, err2
		}
	}
	if s.Sha256 != nil {
		if _, err2 := s.Sha256.Write(p[0:nb]); err2 != nil {
			return nb, err2
		}
	}
	return nb, err
}

func NewHashingReader(body io.Reader, hashTypes ...int) *HashingReader {
	s := new(HashingReader)
	s.originalReader = body
	for hashType := range hashTypes {
		switch hashType {
		case HashFunctionMD5:
			if s.Md5 == nil {
				s.Md5 = md5.New() //nolint:gosec
			}
		case HashFunctionSHA256:
			if s.Sha256 == nil {
				s.Sha256 = sha256.New()
			}
		default:
			panic("wrong hash type number " + strconv.Itoa(hashType))
		}
	}
	return s
}
