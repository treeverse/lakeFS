package operations

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/catalog"
	gerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/path"
	"github.com/treeverse/lakefs/pkg/gateway/serde"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/permissions"
)

type DeleteObjects struct{}

func (controller *DeleteObjects) RequiredPermissions(_ *http.Request, _ string) (permissions.Node, error) {
	return permissions.Node{}, nil
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
		authResp, err := o.Auth.Authorize(req.Context(), &auth.AuthorizationRequest{
			Username: o.Principal,
			RequiredPermissions: permissions.Node{
				Permission: permissions.Permission{
					Action:   permissions.DeleteObjectAction,
					Resource: permissions.ObjectArn(o.Repository.Name, resolvedPath.Path),
				}},
		})
		if err != nil || !authResp.Allowed {
			errs = append(errs, serde.DeleteError{
				Code:    "AccessDenied",
				Key:     obj.Key,
				Message: "Access Denied",
			})
		}

		lg := o.Log(req).WithField("key", obj.Key)
		err = o.Catalog.DeleteEntry(req.Context(), o.Repository.Name, resolvedPath.Ref, resolvedPath.Path)
		switch {
		case errors.Is(err, catalog.ErrNotFound),
			errors.Is(err, graveler.ErrNotFound):
			lg.Debug("tried to delete a non-existent object (OK)")
		case errors.Is(err, graveler.ErrWriteToProtectedBranch):
			_ = o.EncodeError(w, req, gerrors.Codes.ToAPIErr(gerrors.ErrWriteToProtectedBranch))
		case errors.Is(err, catalog.ErrPathRequiredValue):
			// issue #1706 - https://github.com/treeverse/lakeFS/issues/1706
			// Spark trying to delete the path "main/", which we map to branch "main" with an empty path.
			// Spark expects it to succeed (not deleting anything is a success), instead of returning an error.
			lg.Debug("tried to delete with an empty branch")
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
