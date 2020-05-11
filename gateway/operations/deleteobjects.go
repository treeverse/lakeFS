package operations

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/db"

	gerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/permissions"
)

type DeleteObjects struct{}

func (controller *DeleteObjects) Action(repoId, refId, path string) permissions.Action {
	return permissions.DeleteObject(repoId)
}

func (controller *DeleteObjects) Handle(o *RepoOperation) {
	o.Incr("delete_objects")
	req := &serde.Delete{}
	err := o.DecodeXMLBody(req)
	if err != nil {
		o.EncodeError(gerrors.Codes.ToAPIErr(gerrors.ErrBadRequest))
	}
	// delete all the files and collect responses
	errs := make([]serde.DeleteError, 0)
	responses := make([]serde.Deleted, 0)
	for _, obj := range req.Object {
		resolvedPath, err := path.ResolvePath(obj.Key)
		if err != nil {
			errs = append(errs, serde.DeleteError{
				Code:    "ErrDeletingKey",
				Key:     obj.Key,
				Message: fmt.Sprintf("error deleting object: %s", err),
			})
			continue
		}
		err = o.Index.DeleteObject(o.Repo.Id, resolvedPath.Ref, resolvedPath.Path)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			errs = append(errs, serde.DeleteError{
				Code:    "ErrDeletingKey",
				Key:     obj.Key,
				Message: fmt.Sprintf("error deleting object: %s", err),
			})
			continue
		}
		if !req.Quiet {
			responses = append(responses, serde.Deleted{Key: obj.Key})
		}
	}
	// construct response
	resp := serde.DeleteResult{}
	if len(errs) > 0 {
		resp.Error = errs
	}
	if !req.Quiet && len(responses) > 0 {
		resp.Deleted = responses
	}
	o.EncodeResponse(resp, http.StatusOK)
}
