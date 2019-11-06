package operations

import (
	"io"
	"net/http"
	"time"
	"treeverse-lake/gateway/errors"
	"treeverse-lake/gateway/permissions"
	"treeverse-lake/ident"
	"treeverse-lake/index/model"

	log "github.com/sirupsen/logrus"
)

const (
	// size of physical object to store in the underlying block adapter
	// TODO: should probably be a configuration parameter
	ObjectBlockSize = 128 * 1024 * 1024
)

type PutObject struct{}

func (controller *PutObject) GetArn() string {
	return "arn:treeverse:repos:::{bucket}"
}

func (controller *PutObject) GetPermission() string {
	return permissions.PermissionWriteRepo
}

func (controller *PutObject) Handle(o *PathOperation) {

	// handle the upload itself
	body := o.Request.Body
	blocks := make([]*model.Block, 0)
	var totalSize int64
	var done bool
	for !done {
		buf := make([]byte, ObjectBlockSize)
		n, err := body.Read(buf)

		// unexpected error
		if err != nil && err != io.EOF {
			o.Log().WithError(err).Error("could not read request body")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
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
		err = o.BlockStore.Put(buf[:n], blockAddr)
		if err != nil {
			o.Log().WithError(err).Error("could not write to block store")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
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

	// write metadata
	writeTime := time.Now()
	err := o.Index.WriteObject(o.ClientId, o.Repo, o.Branch, o.Path, &model.Object{
		Blob:      &model.Blob{Blocks: blocks},
		Metadata:  nil, // TODO: Read whatever metadata came from the request headers/params and add here
		Timestamp: writeTime.Unix(),
		Size:      totalSize,
	})
	tookMeta := time.Since(writeTime)

	if err != nil {
		o.Log().WithError(err).Error("could not update metadata")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.Log().WithFields(log.Fields{
		"took":   tookMeta,
		"client": o.ClientId,
		"repo":   o.Repo,
		"branch": o.Branch,
		"path":   o.Path,
	}).Trace("metadata update complete")
	o.ResponseWriter.WriteHeader(http.StatusCreated)
}
