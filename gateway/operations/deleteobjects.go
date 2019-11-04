package operations

import (
	"fmt"
	"net/http"
	"versio-index/gateway/errors"
	"versio-index/gateway/permissions"
	"versio-index/gateway/serde"
	"versio-index/index/path"
)

type DeleteObjects struct{}

func (controller *DeleteObjects) GetArn() string {
	return "arn:treeverse:repos:::{bucket}"
}

func (controller *DeleteObjects) GetPermission() string {
	return permissions.PermissionWriteRepo
}

func (controller *DeleteObjects) Handle(o *RepoOperation) {
	req := &serde.Delete{}
	err := o.DecodeXMLBody(req)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
	}
	// delete all the files and collect responses
	errs := make([]serde.DeleteError, 0)
	resps := make([]serde.Deleted, 0)
	for _, obj := range req.Object {
		prefixPath := path.New(obj.Key)
		parts := prefixPath.SplitParts()
		branch := parts[0]

		err := o.Index.DeleteObject(o.ClientId, o.Repo, branch, path.Join(parts[1:]))
		if err != nil {
			errs = append(errs, serde.DeleteError{
				Code:    "ErrDeletingKey",
				Key:     obj.Key,
				Message: fmt.Sprintf("error deleting object: %s", err),
			})
		}
		if err != nil && !req.Quiet {
			resps = append(resps, serde.Deleted{
				Key: obj.Key,
			})
		}
	}
	// construct response
	resp := serde.DeleteObjectsOutput{}
	if len(errs) > 0 {
		resp.Error = errs
	}
	if !req.Quiet && len(resps) > 0 {
		resp.Deleted = resps
	}
	o.EncodeResponse(resp, http.StatusOK)
}
