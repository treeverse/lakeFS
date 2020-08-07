package operations

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/permissions"
)

type HeadObject struct{}

func (controller *HeadObject) RequiredPermissions(request *http.Request, repoID, branchID, path string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repoID, path),
		},
	}, nil
}

func (controller *HeadObject) Handle(o *PathOperation) {
	o.Incr("stat_object")
	entry, err := o.Cataloger.GetEntry(o.Context(), o.Repository.Name, o.Reference, o.Path, catalog.GetEntryParams{ReturnExpired: true})
	if errors.Is(err, db.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		o.Log().Debug("path not found")
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchKey))
		return
	}
	if err != nil {
		o.Log().WithError(err).Error("failed querying path")
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}
	o.SetHeader("Accept-Ranges", "bytes")
	o.SetHeader("Last-Modified", httputil.HeaderTimestamp(entry.CreationDate))
	o.SetHeader("ETag", httputil.ETag(entry.Checksum))
	o.SetHeader("Content-Length", fmt.Sprintf("%d", entry.Size))
	if entry.Expired {
		o.Log().WithError(err).Info("querying expired object")
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchVersion))
	}
}
