package operations

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/permissions"
)

type HeadObject struct{}

func (controller *HeadObject) Action(repoId, refId, path string) permissions.Action {
	return permissions.GetObject(repoId)
}

func (controller *HeadObject) Handle(o *PathOperation) {
	o.Incr("stat_object")
	entry, err := o.Index.ReadEntryObject(o.Repo.Id, o.Ref, o.Path)
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
	if entry.GetType() != model.EntryTypeObject {
		// only objects should return a successful response
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchKey))
		return
	}
	o.SetHeader("Accept-Ranges", "bytes")
	o.SetHeader("Last-Modified", httputil.HeaderTimestamp(entry.CreationDate))
	o.SetHeader("ETag", httputil.ETag(entry.Checksum))
	o.SetHeader("Content-Length", fmt.Sprintf("%d", entry.Size))
}
