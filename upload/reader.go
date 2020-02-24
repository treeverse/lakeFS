package upload

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index/model"
)

const (
	// TODO: should probably be a configurable setting
	ObjectBlockSize = 128 * 1024 * 1024
)

type Blob struct {
	Blocks   []*model.Block
	Checksum string
	Size     int64
}

func ReadBlob(bucketName string, body io.Reader, adapter block.Adapter, objectBlockSize int) (*Blob, error) {
	// handle the upload itself
	blocks := make([]*model.Block, 0)
	cksummer := md5.New()
	var totalSize int64
	var done bool
	for !done {
		buf := make([]byte, objectBlockSize)
		n, err := body.Read(buf)

		// unexpected error
		if err != nil && err != io.EOF {
			return nil, err
		}

		// body is completely drained and we read nothing
		if err == io.EOF && n == 0 {
			break // nothing left to do, we read the whole thing
		}

		// body is completely drained and we read the remainder
		if err == io.EOF {
			done = true
		}

		// write a block
		blockAddr := ident.Bytes(buf[:n]) // content based addressing happens here
		cksummer.Write(buf[:n])
		err = adapter.Put(bucketName, blockAddr, bytes.NewReader(buf[:n]))
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, &model.Block{
			Address: blockAddr,
			Size:    int64(n),
		})
		totalSize += int64(n)

		if done {
			break
		}
	}
	return &Blob{
		Blocks:   blocks,
		Checksum: fmt.Sprintf("%x", cksummer.Sum(nil)),
		Size:     totalSize,
	}, nil
}
