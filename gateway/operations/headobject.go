package operations

import (
	"strconv"
	authmodel "versio-index/auth/model"
	"versio-index/db"
	"versio-index/gateway/errors"
	"versio-index/gateway/serde"
	"versio-index/ident"

	"golang.org/x/xerrors"
)

type HeadObject struct{}

func (controller *HeadObject) GetArn() string {
	return "versio:repos:::{bucket}"
}

func (controller *HeadObject) GetIntent() authmodel.Permission_Intent {
	return authmodel.Permission_REPO_READ
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
