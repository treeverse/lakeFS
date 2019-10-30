package operations

import (
	"strconv"
	"versio-index/db"
	"versio-index/gateway/errors"
	"versio-index/gateway/permissions"
	"versio-index/gateway/serde"
	"versio-index/ident"

	"golang.org/x/xerrors"
)

type HeadObject struct{}

func (controller *HeadObject) GetArn() string {
	return "arn:versio:repos:::{bucket}"
}

func (controller *HeadObject) GetPermission() string {
	return permissions.PermissionReadRepo
}

func (controller *HeadObject) Handle(o *PathOperation) {
	obj, err := o.Index.ReadObject(o.ClientId, o.Repo, o.Branch, o.Path)
	if xerrors.Is(err, db.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchKey))
		return
	}
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	res := o.ResponseWriter
	res.Header().Set("Content-Length", strconv.FormatInt(obj.GetSize(), 10))
	res.Header().Set("Last-Modified", serde.Timestamp(obj.GetTimestamp()))
	res.Header().Set("Etag", ident.Hash(obj))
}
