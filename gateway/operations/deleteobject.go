package operations

import (
	"errors"
	"net/http"

	"github.com/treeverse/lakefs/graveler"
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

func (controller *DeleteObject) HandleAbortMultipartUpload(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("abort_mpu")
	query := req.URL.Query()
	uploadID := query.Get(QueryParamUploadID)
	req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{"upload_id": uploadID}))
	err := o.BlockStore.AbortMultiPartUpload(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: o.Path}, uploadID)
	if err != nil {
		o.Log(req).WithError(err).Error("could not abort multipart upload")
		_ = o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}
	// done.
	w.WriteHeader(http.StatusNoContent)
}

func (controller *DeleteObject) Handle(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	query := req.URL.Query()

	_, hasUploadID := query[QueryParamUploadID]
	if hasUploadID {
		controller.HandleAbortMultipartUpload(w, req, o)
		return
	}

	o.Incr("delete_object")
	lg := o.Log(req).WithField("key", o.Path)
	err := o.Cataloger.DeleteEntry(req.Context(), o.Repository.Name, o.Reference, o.Path)
	switch {
	case errors.Is(err, db.ErrNotFound) || errors.Is(err, graveler.ErrNotFound):
		lg.WithError(err).Debug("could not delete object, it doesn't exist")
	case err != nil:
		lg.WithError(err).Error("could not delete object")
		_ = o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	default:
		lg.Debug("object set for deletion")
	}
	w.WriteHeader(http.StatusNoContent)
}
