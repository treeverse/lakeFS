package upload

import (
	"context"
	"encoding/hex"
	"io"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
)

type Blob struct {
	PhysicalAddress string
	RelativePath    bool
	Checksum        string
	Size            int64
}

func WriteBlob(ctx context.Context, adapter block.Adapter, bucketName, address string, body io.Reader, contentLength int64, opts block.PutOpts) (*Blob, error) {
	// handle the upload itself
	hashReader := block.NewHashingReader(body, block.HashFunctionMD5, block.HashFunctionSHA256)
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
		RelativePath:    true,
		Checksum:        checksum,
		Size:            hashReader.CopiedSize,
	}, nil
}

// CopyBlob copies file from sourceAddress to a generated UUID in destinationAddress
func CopyBlob(ctx context.Context, adapter block.Adapter, sourceBucketName string, destinationBucketName string, sourceAddress string, checksum string, size int64) (*Blob, error) {
	uid := uuid.New()
	destinationAddress := hex.EncodeToString(uid[:])

	err := adapter.Copy(ctx, block.ObjectPointer{
		StorageNamespace: sourceBucketName,
		Identifier:       sourceAddress,
	}, block.ObjectPointer{
		StorageNamespace: destinationBucketName,
		Identifier:       destinationAddress,
	})
	if err != nil {
		return nil, err
	}
	return &Blob{
		PhysicalAddress: destinationAddress,
		RelativePath:    true,
		Checksum:        checksum,
		Size:            size,
	}, nil
}
