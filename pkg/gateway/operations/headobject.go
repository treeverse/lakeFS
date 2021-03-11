package operations

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/catalog"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/permissions"
)

type HeadObject struct{}

func (controller *HeadObject) RequiredPermissions(_ *http.Request, repoID, _, path string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repoID, path),
		},
	}, nil
}

func (controller *HeadObject) Handle(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("stat_object")
	entry, err := o.Catalog.GetEntry(req.Context(), o.Repository.Name, o.Reference, o.Path, catalog.GetEntryParams{ReturnExpired: true})
	if errors.Is(err, catalog.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		o.Log(req).Debug("path not found")
		_ = o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchKey))
		return
	}
	if err != nil {
		o.Log(req).WithError(err).Error("failed querying path")
		_ = o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}
	if entry.Expired {
		o.Log(req).WithError(err).Info("querying expired object")
		_ = o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchVersion))
		return
	}

	o.SetHeader(w, "Accept-Ranges", "bytes")
	o.SetHeader(w, "Last-Modified", httputil.HeaderTimestamp(entry.CreationDate))
	o.SetHeader(w, "ETag", httputil.ETag(entry.Checksum))
	o.SetHeader(w, "Content-Length", fmt.Sprintf("%d", entry.Size))

	// Delete the default content-type header so http.Server will detect it from contents
	// TODO(ariels): After/if we add content-type support to adapter, use *that*.
	o.DeleteHeader(w, "Content-Type")
}
