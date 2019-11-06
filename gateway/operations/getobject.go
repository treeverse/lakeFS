package operations

import (
	"strconv"
	"strings"
	"time"
	"treeverse-lake/block"
	"treeverse-lake/db"
	"treeverse-lake/gateway/errors"
	"treeverse-lake/gateway/permissions"
	"treeverse-lake/gateway/serde"
	"treeverse-lake/ident"
	"treeverse-lake/index/model"

	"golang.org/x/xerrors"
)

var (
	ErrBadRange = xerrors.Errorf("unsatisfiable range")
)

type GetObject struct{}

func (controller *GetObject) GetArn() string {
	return "arn:treeverse:repos:::{bucket}"
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

	o.ResponseWriter.Header().Set("Last-Modified", serde.Timestamp(obj.GetTimestamp()))
	o.ResponseWriter.Header().Set("Etag", ident.Hash(obj))
	o.ResponseWriter.Header().Set("Accept-Ranges", "bytes")
	// TODO: the rest of https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/API_GetObject.html

	// assemble a response body
	blocks := obj.GetBlob().GetBlocks()
	for _, block := range blocks {
		data, err := o.BlockStore.Get(block.GetAddress())
		if err != nil {
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}
		_, err = o.ResponseWriter.Write(data)
		if err != nil {
			o.Log().Error("could not write response body for object")
		}
	}
}

type HttpRange struct {
	StartOffset int64
	EndOffset   int64
}

func ParseHTTPRange(spec string, length int64) (HttpRange, error) {
	// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
	var r HttpRange
	if !strings.HasPrefix(spec, "bytes=") {
		return r, ErrBadRange
	}
	spec = strings.TrimSuffix(spec, "bytes=")
	parts := strings.Split(spec, "-")
	if len(parts) != 2 {
		return r, ErrBadRange
	}

	fromString := parts[0]
	toString := parts[1]
	if len(fromString) == 0 && len(toString) == 0 {
		return r, ErrBadRange
	}
	// negative only
	if len(fromString) == 0 {
		endOffset, err := strconv.ParseInt(toString, 10, 64)
		if err != nil || endOffset > length {
			return r, ErrBadRange
		}
		r.StartOffset = length - endOffset
		r.EndOffset = length - 1
		return r, nil
	}
	// positive only
	if len(toString) == 0 {
		beginOffset, err := strconv.ParseInt(fromString, 10, 64)
		if err != nil || beginOffset > length-1 {
			return r, ErrBadRange
		}
		r.StartOffset = beginOffset
		r.EndOffset = length - 1
		return r, nil
	}
	// both set
	beginOffset, err := strconv.ParseInt(fromString, 10, 64)
	if err != nil {
		return r, ErrBadRange
	}
	endOffset, err := strconv.ParseInt(toString, 10, 64)
	if err != nil {
		return r, ErrBadRange
	}
	if beginOffset > length-1 || endOffset > length {
		return r, ErrBadRange
	}
	r.StartOffset = beginOffset
	r.EndOffset = endOffset
	return r, nil
}

// Utility tool to help with range requests
type ObjectRanger struct {
	rang    HttpRange
	obj     *model.Object
	adapter block.Adapter

	offset int64
}

func NewObjectRanger(spec string, obj *model.Object, adapter block.Adapter) (*ObjectRanger, error) {
	// let's start by deciding which blocks we actually need
	// TODO: we can later pass on the range queries to S3 itself
	rang, err := ParseHTTPRange(spec, obj.GetSize())
	if err != nil {
		return nil, err
	}
	return &ObjectRanger{
		rang:    rang,
		obj:     obj,
		adapter: adapter,
	}, nil

}

// implement io.Reader
func (r *ObjectRanger) Read(p []byte) (n int, err error) {
	begin := r.rang.StartOffset
	end := r.rang.EndOffset
	var scanned int64
	bufSize := len(p)
	for _, block := range r.obj.GetBlob().GetBlocks() {
		// see what range we need from this block
		bytes, err := r.adapter.GetOffset(block.GetAddress(), ?, ?)
		if err != nil {
			return
		}
		scanned += blockSize
	}
	return
}
