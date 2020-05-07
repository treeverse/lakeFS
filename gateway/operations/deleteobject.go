package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/db"
	"golang.org/x/xerrors"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/gateway/errors"
)

type DeleteObject struct {
}

func (controller *DeleteObject) Action(repoId, refId, path string) permissions.Action {
	return permissions.DeleteObject(repoId)
}

func (controller *DeleteObject) HandleAbortMultipartUpload(o *PathOperation) {
	o.Incr("abort_mpu")
	query := o.Request.URL.Query()
	uploadId := query.Get(QueryParamUploadId)
	err := o.BlockStore.AbortMultiPartUpload(o.Repo.StorageNamespace, o.Path, uploadId)
	if err != nil {
		o.Log().WithError(err).Error("could not abort multipart upload")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	// done.
	o.ResponseWriter.WriteHeader(http.StatusNoContent)
}

func (controller *DeleteObject) Handle(o *PathOperation) {
	query := o.Request.URL.Query()

	_, hasUploadId := query[QueryParamUploadId]
	if hasUploadId {
		controller.HandleAbortMultipartUpload(o)
		return
	}

	o.Incr("delete_object")
	err := o.Index.DeleteObject(o.Repo.Id, o.Ref, o.Path)
	if err != nil {
		o.Log().WithError(err).Error("could not delete key")
		if !xerrors.Is(err, db.ErrNotFound) {
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}
	}
	o.ResponseWriter.WriteHeader(http.StatusNoContent)
}
