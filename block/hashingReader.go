package block

import (
	"crypto/md5"
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
	len, err := s.originalReader.Read(p)
	if len > 0 {
		s.CopiedSize += int64(len)
		if s.Md5 != nil {
			s.Md5.Write(p[0:len])
		}
		if s.Sha256 != nil {
			s.Sha256.Write(p[0:len])
		}
	}
	return len, err
}

func NewHashingReader(body io.Reader, hashTypes ...int) *HashingReader {
	s := new(HashingReader)
	s.originalReader = body
	for hashType := range hashTypes {
		if hashType == HashFunctionMD5 {
			s.Md5 = md5.New()
		} else if hashType == HashFunctionSHA256 {
			s.Sha256 = sha256.New()
		} else {
			panic("wrong hash type number " + strconv.Itoa(hashType))
		}
	}
	return s
}
