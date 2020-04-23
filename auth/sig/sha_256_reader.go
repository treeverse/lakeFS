package sig

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"

	"github.com/treeverse/lakefs/gateway/errors"
)

type Reader struct {
	src          io.ReadCloser
	expectedHash []byte
	hash         hash.Hash
}

func NewReader(src io.ReadCloser, sha256Hex string) (io.ReadCloser, error) {

	expectedHash, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return nil, err
	}

	var Sha256hash hash.Hash
	if len(expectedHash) != 0 {
		Sha256hash = sha256.New()
	}

	return &Reader{
		expectedHash: expectedHash,
		src:          src,
		hash:         Sha256hash,
	}, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		r.hash.Write(p[:n])
	}
	if err == io.EOF {
		if cerr := r.Verify(); cerr != nil {
			return 0, cerr
		}
	}
	return n, err
}

func (r *Reader) Verify() error {
	if sum := r.hash.Sum(nil); !bytes.Equal(r.expectedHash, sum) {
		return errors.ErrSignatureDoesNotMatch
	}
	return nil
}
func (r *Reader) Close() error {
	return r.src.Close()
}
