package block

import (
	"crypto/md5"
	"crypto/sha256"
	"hash"
	"io"
	"strconv"
)

const (
	MD5 = iota
	SHA256
	BOTH
)

type HashingReader struct {
	hashType       int
	Md5            hash.Hash
	Sha256         hash.Hash
	originalReader io.Reader
	CopiedSize     int64
}

func (s *HashingReader) Read(p []byte) (int, error) {
	len, err := s.originalReader.Read(p)
	if len > 0 {
		s.CopiedSize += int64(len)
		if s.hashType == MD5 || s.hashType == BOTH {
			s.Md5.Write(p[0:len])
		}
		if s.hashType == SHA256 || s.hashType == BOTH {
			s.Sha256.Write(p[0:len])
		}
	}
	return len, err
}

func (s *HashingReader) Close() error {
	return nil
}

func NewHashingReader(body io.Reader, hashType int) *HashingReader {
	s := new(HashingReader)
	s.hashType = hashType
	switch hashType {
	case MD5:
		s.Md5 = md5.New()
	case SHA256:
		s.Sha256 = sha256.New()
	case BOTH:
		s.Md5 = md5.New()
		s.Sha256 = sha256.New()
	default:
		panic("wrong hash type number " + strconv.Itoa(hashType))
	}
	s.originalReader = body
	return s
}
