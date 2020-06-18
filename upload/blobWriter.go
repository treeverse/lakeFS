package upload

import (
	"encoding/hex"
	"io"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
)

// WriteBlob needs only this function from index. created this interface to enable easy testing
type DedupHandler interface {
	CreateDedupEntryIfNone(repoId string, dedupId string, objName string) (string, error)
}

func WriteBlob(index DedupHandler, repoId, bucketName string, body io.Reader, adapter block.Adapter, contentLength int64, opts block.PutOpts) (string, string, int64, error) {
	// handle the upload itself
	hashReader := block.NewHashingReader(body, block.HashFunctionMD5, block.HashFunctionSHA256)
	UUIDbytes := ([16]byte(uuid.New()))
	objName := hex.EncodeToString(UUIDbytes[:])
	err := adapter.Put(block.ObjectPointer{Repo: bucketName, Identifier: objName}, contentLength, hashReader, opts)
	if err != nil {
		return "", "", -1, err
	}
	dedupId := hex.EncodeToString(hashReader.Sha256.Sum(nil))
	checksum := hex.EncodeToString(hashReader.Md5.Sum(nil))
	existingName, err := index.CreateDedupEntryIfNone(repoId, dedupId, objName)
	if err != nil {
		return "", "", -1, err
	}
	if existingName != objName { // object already exists
		adapter.Remove(block.ObjectPointer{Repo: bucketName, Identifier: objName})
		objName = existingName
	}
	return checksum, objName, hashReader.CopiedSize, nil

}
