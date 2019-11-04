package operations

import (
	"io/ioutil"
	"net/http"
	"time"
	"versio-index/gateway/errors"
	"versio-index/gateway/permissions"
	"versio-index/ident"
	"versio-index/index/model"

	log "github.com/sirupsen/logrus"
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
	data, err := ioutil.ReadAll(o.Request.Body)
	if err != nil {
		o.Log().WithError(err).Error("could not read request body")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// write to adapter
	blocks := make([]string, 0)
	blockAddr := ident.Bytes(data)
	err = o.BlockStore.Put(data, blockAddr)
	if err != nil {
		o.Log().WithError(err).Error("could not write to block store")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	blocks = append(blocks, blockAddr)

	// write metadata
	before := time.Now()
	err = o.Index.WriteObject(o.ClientId, o.Repo, o.Branch, o.Path, &model.Object{
		Blob: &model.Blob{
			Blocks: blocks,
		},
		Metadata:  nil,
		Timestamp: time.Now().Unix(),
		Size:      int64(len(data)),
	})
	tookMeta := time.Since(before)

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
