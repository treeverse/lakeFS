package operations

import (
	"errors"
	"net/http"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/permissions"
)

type DeleteObject struct{}

func (controller *DeleteObject) RequiredPermissions(_ *http.Request, repoID, _, path string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.DeleteObjectAction,
			Resource: permissions.ObjectArn(repoID, path),
		},
	}, nil
}

func (controller *DeleteObject) HandleAbortMultipartUpload(o *PathOperation) {
	o.Incr("abort_mpu")
	query := o.Request.URL.Query()
	uploadID := query.Get(QueryParamUploadID)
	o.AddLogFields(logging.Fields{"upload_id": uploadID})
	err := o.BlockStore.AbortMultiPartUpload(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: o.Path}, uploadID)
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

	_, hasUploadID := query[QueryParamUploadID]
	if hasUploadID {
		controller.HandleAbortMultipartUpload(o)
		return
	}

	o.Incr("delete_object")
	lg := o.Log().WithField("key", o.Path)
	err := o.Cataloger.DeleteEntry(o.Context(), o.Repository.Name, o.Reference, o.Path)
	switch {
	case errors.Is(err, db.ErrNotFound):
		lg.WithError(err).Debug("could not delete object, it doesn't exist")
	case err != nil:
		lg.WithError(err).Error("could not delete object")
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	default:
		lg.Debug("object set for deletion")
	}
	o.ResponseWriter.WriteHeader(http.StatusNoContent)
}
