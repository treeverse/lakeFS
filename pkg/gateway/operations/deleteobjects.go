package operations

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/catalog"
	gerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/path"
	"github.com/treeverse/lakefs/pkg/gateway/serde"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// maxDeleteObjects maximum number of objects we can delete in one call.
// base on https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
const maxDeleteObjects = 1000

type DeleteObjects struct{}

func (controller *DeleteObjects) RequiredPermissions(_ *http.Request, _ string) (permissions.Node, error) {
	return permissions.Node{}, nil
}

func (controller *DeleteObjects) Handle(w http.ResponseWriter, req *http.Request, o *RepoOperation) {
	o.Incr("delete_objects", o.Principal, o.Repository.Name, "")
	decodedXML := &serde.Delete{}
	err := DecodeXMLBody(req.Body, decodedXML)
	if err != nil {
		_ = o.EncodeError(w, req, gerrors.Codes.ToAPIErr(gerrors.ErrBadRequest))
		return
	}
	if len(decodedXML.Object) == 0 || len(decodedXML.Object) > maxDeleteObjects {
		_ = o.EncodeError(w, req, gerrors.Codes.ToAPIErr(gerrors.ErrMalformedXML))
		return
	}

	// delete all the files and collect responses
	// arrays of keys/path to delete, left after authorization check
	var (
		keysToDelete  []string
		pathsToDelete []string
		refsToDelete  []string
		errs          []serde.DeleteError
	)
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
				},
			},
		})
		if err != nil || !authResp.Allowed {
			errs = append(errs, serde.DeleteError{
				Code:    "AccessDenied",
				Key:     obj.Key,
				Message: "Access Denied",
			})
			continue
		}

		keysToDelete = append(keysToDelete, obj.Key)
		refsToDelete = append(refsToDelete, resolvedPath.Ref)
		pathsToDelete = append(pathsToDelete, resolvedPath.Path)
	}
	if len(pathsToDelete) == 0 {
		// construct response - probably we failed with all errors
		resp := serde.DeleteResult{Error: errs}
		o.EncodeResponse(w, req, resp, http.StatusOK)
		return
	}

	// batch delete - if all paths to delete are same ref
	canBatch := true
	for i := 1; i < len(refsToDelete); i++ {
		if refsToDelete[0] != refsToDelete[i] {
			canBatch = false
			break
		}
	}

	var resp serde.DeleteResult
	if canBatch {
		// batch - call batch delete for all keys on ref
		resp = controller.batchDelete(req.Context(), o.Log(req), o, decodedXML.Quiet, refsToDelete[0], keysToDelete, pathsToDelete)
	} else {
		// non batch - call delete for each key
		resp = controller.nonBatchDelete(req.Context(), o.Log(req), o, decodedXML.Quiet, keysToDelete, refsToDelete, pathsToDelete)
	}

	// construct response - concat what we had so far with delete results
	if len(errs) > 0 {
		resp.Error = append(errs, resp.Error...)
	}
	o.EncodeResponse(w, req, resp, http.StatusOK)
}

func (controller *DeleteObjects) nonBatchDelete(ctx context.Context, log logging.Logger, o *RepoOperation, quiet bool, keysToDelete []string, refsToDelete []string, pathsToDelete []string) serde.DeleteResult {
	var result serde.DeleteResult
	for i, key := range keysToDelete {
		err := o.Catalog.DeleteEntry(ctx, o.Repository.Name, refsToDelete[i], pathsToDelete[i])
		updateDeleteResult(&result, quiet, log, key, err)
	}
	return result
}

func (controller *DeleteObjects) batchDelete(ctx context.Context, log logging.Logger, o *RepoOperation, quiet bool, ref string, keysToDelete []string, pathsToDelete []string) serde.DeleteResult {
	var result serde.DeleteResult
	batchErr := o.Catalog.DeleteEntries(ctx, o.Repository.Name, ref, pathsToDelete)
	deleteErrs := graveler.NewMapDeleteErrors(batchErr)
	for _, key := range keysToDelete {
		// err will set to the specific error if possible, fallback to the batch delete error
		err := deleteErrs[key]
		if err == nil {
			err = batchErr
		}
		updateDeleteResult(&result, quiet, log, key, err)
	}
	return result
}

// updateDeleteResult check the error and update the 'result' with error or delete response for 'key'
func updateDeleteResult(result *serde.DeleteResult, quiet bool, log logging.Logger, key string, err error) {
	deleteError := checkForDeleteError(log, key, err)
	if deleteError != nil {
		result.Error = append(result.Error, *deleteError)
	} else if !quiet {
		result.Deleted = append(result.Deleted, serde.Deleted{Key: key})
	}
}

func checkForDeleteError(log logging.Logger, key string, err error) *serde.DeleteError {
	switch {
	case errors.Is(err, graveler.ErrNotFound):
		log.Debug("tried to delete a non-existent object (OK)")
	case errors.Is(err, graveler.ErrWriteToProtectedBranch):
		apiErr := gerrors.Codes.ToAPIErr(gerrors.ErrWriteToProtectedBranch)
		return &serde.DeleteError{
			Code:    apiErr.Code,
			Key:     key,
			Message: fmt.Sprintf("error deleting object: %s", apiErr.Description),
		}
	case errors.Is(err, catalog.ErrPathRequiredValue):
		// issue #1706 - https://github.com/treeverse/lakeFS/issues/1706
		// Spark trying to delete the path "main/", which we map to branch "main" with an empty path.
		// Spark expects it to succeed (not deleting anything is a success), instead of returning an error.
		log.Debug("tried to delete with an empty path")
	case err != nil:
		log.WithField("key", key).WithError(err).Error("failed deleting object")
		return &serde.DeleteError{
			Code:    "ErrDeletingKey",
			Key:     key,
			Message: fmt.Sprintf("error deleting object: %s", err),
		}
	default:
		log.WithField("key", key).Debug("object set for deletion")
	}
	return nil
}
