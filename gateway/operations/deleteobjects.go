package operations

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/db"
	gerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/permissions"
)

type DeleteObjects struct{}

func (controller *DeleteObjects) RequiredPermissions(_ *http.Request, _ string) ([]permissions.Permission, error) {
	return nil, nil
}

func (controller *DeleteObjects) Handle(w http.ResponseWriter, req *http.Request, o *RepoOperation) {
	o.Incr("delete_objects")
	decodedXML := &serde.Delete{}
	err := DecodeXMLBody(req.Body, decodedXML)
	if err != nil {
		_ = o.EncodeError(w, req, gerrors.Codes.ToAPIErr(gerrors.ErrBadRequest))
		return
	}
	// delete all the files and collect responses
	errs := make([]serde.DeleteError, 0)
	responses := make([]serde.Deleted, 0)
	for _, obj := range decodedXML.Object {
		resolvedPath, err := path.ResolvePath(obj.Key)
		if err != nil {
			errs = append(errs, serde.DeleteError{
				Code:    "ErrDeletingKey",
				Key:     obj.Key,
				Message: fmt.Sprintf("error deleting object: %s", err),
			})
			continue
		}
		// authorize this object deletion
		authResp, err := o.Auth.Authorize(&auth.AuthorizationRequest{
			Username: o.Principal,
			RequiredPermissions: []permissions.Permission{
				{
					Action:   permissions.DeleteObjectAction,
					Resource: permissions.ObjectArn(o.Repository.Name, resolvedPath.Path),
				},
			},
		})
		if err != nil || !authResp.Allowed {
			errs = append(errs, serde.DeleteError{
				Code:    "AccessDenied",
				Key:     obj.Key,
				Message: "Access Denied",
			})
		}

		lg := o.Log(req).WithField("key", obj.Key)
		err = o.Cataloger.DeleteEntry(req.Context(), o.Repository.Name, resolvedPath.Ref, resolvedPath.Path)
		switch {
		case errors.Is(err, db.ErrNotFound):
			lg.Debug("tried to delete a non-existent object")
		case err != nil:
			lg.WithError(err).Error("failed deleting object")
			errs = append(errs, serde.DeleteError{
				Code:    "ErrDeletingKey",
				Key:     obj.Key,
				Message: fmt.Sprintf("error deleting object: %s", err),
			})
			continue
		default:
			lg.Debug("object set for deletion")
		}
		if !decodedXML.Quiet {
			responses = append(responses, serde.Deleted{Key: obj.Key})
		}
	}
	// construct response
	resp := serde.DeleteResult{}
	if len(errs) > 0 {
		resp.Error = errs
	}
	if !decodedXML.Quiet && len(responses) > 0 {
		resp.Deleted = responses
	}
	o.EncodeResponse(w, req, resp, http.StatusOK)
}
