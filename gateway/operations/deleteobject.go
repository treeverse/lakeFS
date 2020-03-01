package operations

import (
	"github.com/treeverse/lakefs/gateway/utils"
	"net/http"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/gateway/errors"
)

type DeleteObject struct{}

func (controller *DeleteObject) Action(req *http.Request) permissions.Action {
	return permissions.DeleteObject(utils.GetRepo(req))
}

func (controller *DeleteObject) HandleAbortMultipartUpload(o *PathOperation) {
	query := o.Request.URL.Query()
	uploadId := query.Get(QueryParamUploadId)

	// ensure this

	err := o.MultipartManager.Abort(o.Repo.GetRepoId(), o.Path, uploadId)
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

	err := o.Index.DeleteObject(o.Repo.GetRepoId(), o.Branch, o.Path)
	if err != nil {
		o.Log().WithError(err).Error("could not delete key")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
}
