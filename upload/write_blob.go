package upload

import (
	"context"
	"encoding/hex"
	"io"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
)

type Blob struct {
	PhysicalAddress string
	Checksum        string
	Size            int64
}

func WriteBlob(ctx context.Context, adapter block.Adapter, bucketName string, body io.Reader, contentLength int64, opts block.PutOpts) (*Blob, error) {
	// handle the upload itself
	hashReader := block.NewHashingReader(body, block.HashFunctionMD5, block.HashFunctionSHA256)
	uid := uuid.New()
	address := hex.EncodeToString(uid[:])
	err := adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: bucketName,
		Identifier:       address,
	}, contentLength, hashReader, opts)
	if err != nil {
		return nil, err
	}
	checksum := hex.EncodeToString(hashReader.Md5.Sum(nil))
	return &Blob{
		PhysicalAddress: address,
		Checksum:        checksum,
		Size:            hashReader.CopiedSize,
	}, nil
}
