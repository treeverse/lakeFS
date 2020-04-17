package operations

import (
	"fmt"

	"github.com/treeverse/lakefs/httputil"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/permissions"

	"golang.org/x/xerrors"
)

type HeadObject struct{}

func (controller *HeadObject) Action(repoId, refId, path string) permissions.Action {
	return permissions.GetObject(repoId)
}

func (controller *HeadObject) Handle(o *PathOperation) {
	entry, err := o.Index.ReadEntryObject(o.Repo.Id, o.Ref, o.Path)
	if xerrors.Is(err, db.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		o.Log().Warn("path not found")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchKey))
		return
	}
	if err != nil {
		o.Log().WithError(err).Error("failed querying path")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	if entry.GetType() != model.EntryTypeObject {
		// only objects should return a successful response
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchKey))
		return
	}
	o.SetHeader("Accept-Ranges", "bytes")
	o.SetHeader("Last-Modified", httputil.HeaderTimestamp(entry.CreationDate))
	o.SetHeader("ETag", httputil.ETag(entry.Checksum))
	o.SetHeader("Content-Length", fmt.Sprintf("%d", entry.Size))
}
