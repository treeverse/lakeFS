package operations

import (
	"errors"
	"net/http"

	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/permissions"
)

type DeleteObject struct{}

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
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
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
	lg := o.Log().WithField("key", o.Path)
	err := o.Index.DeleteObject(o.Repo.Id, o.Ref, o.Path)
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		lg.WithError(err).Error("could not delete object")
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	} else if errors.Is(err, db.ErrNotFound) {
		lg.WithError(err).Debug("could not delete object, it doesn't exist")
	} else if err == nil {
		lg.Debug("object set for deletion")
	}
	o.ResponseWriter.WriteHeader(http.StatusNoContent)
}
