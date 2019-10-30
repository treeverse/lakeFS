package operations

import (
	"bytes"
	"io"
	"time"
	"versio-index/db"
	"versio-index/gateway/errors"
	"versio-index/gateway/permissions"
	"versio-index/gateway/serde"
	"versio-index/ident"

	"golang.org/x/xerrors"
)

type GetObject struct{}

func (controller *GetObject) GetArn() string {
	return "arn:versio:repos:::{bucket}"
}

func (controller *GetObject) GetPermission() string {
	return permissions.PermissionReadRepo
}

func (controller *GetObject) Handle(o *PathOperation) {
	beforeMeta := time.Now()
	obj, err := o.Index.ReadObject(o.ClientId, o.Repo, o.Branch, o.Path)
	metaTook := time.Since(beforeMeta)
	o.Log().WithField("took", metaTook).Info("metadata operation to retrieve object done")

	if xerrors.Is(err, db.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchKey))
		return
	}
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	blocks := obj.GetBlob().GetBlocks()
	buf := bytes.NewBuffer(nil)
	for _, block := range blocks {
		data, err := o.BlockStore.Get(block)
		if err != nil {
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}
		buf.Write(data)
	}

	o.ResponseWriter.Header().Set("Last-Modified", serde.Timestamp(obj.GetTimestamp()))
	o.ResponseWriter.Header().Set("Etag", ident.Hash(obj))
	// TODO: the rest of https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/API_GetObject.html

	_, err = io.Copy(o.ResponseWriter, buf)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
}
