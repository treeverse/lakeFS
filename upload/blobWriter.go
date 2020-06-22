package upload

import (
	"context"
	"encoding/hex"
	"io"

	"github.com/treeverse/lakefs/catalog"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
)

func WriteBlob(ctx context.Context, deduper catalog.Deduper, repository, bucketName string, body io.Reader, adapter block.Adapter, contentLength int64, opts block.PutOpts) (string, string, int64, error) {
	// handle the upload itself
	hashReader := block.NewHashingReader(body, block.HashFunctionMD5, block.HashFunctionSHA256)
	uuidBytes := [16]byte(uuid.New())
	objName := hex.EncodeToString(uuidBytes[:])
	err := adapter.Put(block.ObjectPointer{Repo: bucketName, Identifier: objName}, contentLength, hashReader, opts)
	if err != nil {
		return "", "", -1, err
	}
	dedupID := hex.EncodeToString(hashReader.Sha256.Sum(nil))
	checksum := hex.EncodeToString(hashReader.Md5.Sum(nil))
	existingName, err := deduper.Dedup(ctx, repository, dedupID, objName)
	if err != nil {
		return "", "", -1, err
	}
	if existingName != objName { // object already exists
		adapter.Remove(block.ObjectPointer{Repo: bucketName, Identifier: objName})
		objName = existingName
	}
	return checksum, objName, hashReader.CopiedSize, nil
}
