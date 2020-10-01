#code
package upload

import (
	"encoding/hex"
	"io"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
)

type Blob struct {
	PhysicalAddress string
	Checksum        string
	DedupID         string
	Size            int64
}

func WriteBlob(adapter block.Adapter, bucketName string, body io.Reader, contentLength int64, opts block.PutOpts) (*Blob, error) {
	// handle the upload itself
	hashReader := block.NewHashingReader(body, block.HashFunctionMD5, block.HashFunctionSHA256)
	uid := uuid.New()
	address := hex.EncodeToString(uid[:])
	err := adapter.Put(block.ObjectPointer{
		StorageNamespace: bucketName,
		Identifier:       address,
	}, contentLength, hashReader, opts)
	if err != nil {
		return nil, err
	}
	checksum := hex.EncodeToString(hashReader.Md5.Sum(nil))
	dedupID := hex.EncodeToString(hashReader.Sha256.Sum(nil))
	return &Blob{
		PhysicalAddress: address,
		Checksum:        checksum,
		DedupID:         dedupID,
		Size:            hashReader.CopiedSize,
	}, nil
}
