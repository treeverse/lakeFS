package operations

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/httputil"
	ipath "github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/upload"
)

const (
	CopySourceHeader     = "x-amz-copy-source"
	QueryParamUploadId   = "uploadId"
	QueryParamPartNumber = "partNumber"
)

type PutObject struct{}

func (controller *PutObject) RequiredPermissions(request *http.Request, repoId, branchId, path string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repoId, path),
		},
	}, nil
}

func (controller *PutObject) HandleCopy(o *PathOperation, copySource string) {
	o.Incr("copy_object")
	// resolve source branch and source path
	copySourceDecoded, err := url.QueryUnescape(copySource)
	if err != nil {
		copySourceDecoded = copySource
	}
	p, err := path.ResolveAbsolutePath(copySourceDecoded)
	if err != nil {
		o.Log().WithError(err).Error("could not parse copy source path")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// validate src and dst are in the same repo
	if !strings.EqualFold(o.Repo.Id, p.Repo) {
		o.Log().WithError(err).Error("cannot copy objects across repos")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// update metadata to refer to the source hash in the destination workspace
	src, err := o.Index.ReadEntryObject(o.Repo.Id, p.Ref, p.Path, true)
	if err != nil {
		o.Log().WithError(err).Error("could not read copy source")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}
	// write this object to workspace
	// TODO: move this logic into the Index impl.
	src.CreationDate = time.Now()
	src.Name = ipath.New(o.Path, src.EntryType).BaseName()
	err = o.Index.WriteEntry(o.Repo.Id, o.Ref, o.Path, src)
	if err != nil {
		o.Log().WithError(err).Error("could not write copy destination")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopyDest))
		return
	}

	o.EncodeResponse(&serde.CopyObjectResult{
		LastModified: serde.Timestamp(src.CreationDate),
		ETag:         fmt.Sprintf("\"%s\"", src.Checksum),
	}, http.StatusOK)
}

func (controller *PutObject) HandleUploadPart(o *PathOperation) {
	o.Incr("put_mpu_part")
	query := o.Request.URL.Query()
	uploadId := query.Get(QueryParamUploadId)
	partNumberStr := query.Get(QueryParamPartNumber)

	partNumber, err := strconv.ParseInt(partNumberStr, 10, 64)
	if err != nil {
		o.Log().WithError(err).Error("invalid part number")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidPartNumberMarker))
		return
	}
	// handle the upload itself
	multiPart, err := o.Index.ReadMultiPartUpload(o.Repo.Id, uploadId)
	if err != nil {
		o.Log().WithError(err).Error("could not read  multipart record")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	byteSize := o.Request.ContentLength
	ETag, err := o.BlockStore.UploadPart(o.Repo.StorageNamespace, multiPart.PhysicalAddress, byteSize, o.Request.Body, uploadId, partNumber)
	if err != nil {
		o.Log().WithError(err).Error("part " + partNumberStr + " upload failed")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.SetHeader("ETag", ETag)
	o.ResponseWriter.WriteHeader(http.StatusOK)
}
func (controller *PutObject) Handle(o *PathOperation) {
	// check if this is a copy operation (i.e.https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
	// A copy operation is identified by the existence of an "x-amz-copy-source" header

	//validate branch
	_, err := o.Index.GetBranch(o.Repo.Id, o.Ref)
	if err != nil {
		o.Log().WithError(err).Debug("trying to write to invalid branch")
		o.ResponseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	copySource := o.Request.Header.Get(CopySourceHeader)
	if len(copySource) > 0 {
		controller.HandleCopy(o, copySource)
		return
	}

	query := o.Request.URL.Query()

	// check if this is a multipart upload creation call
	_, hasUploadId := query[QueryParamUploadId]
	if hasUploadId {
		controller.HandleUploadPart(o)
		return
	}

	o.Incr("put_object")
	// handle the upload itself
	checksum, physicalAddress, size, err := upload.WriteBlob(o.Index, o.Repo.Id, o.Repo.StorageNamespace, o.Request.Body, o.BlockStore, o.Request.ContentLength)
	if err != nil {
		o.Log().WithError(err).Error("could not write request body to block adapter")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// write metadata
	err = o.finishUpload(checksum, physicalAddress, size)
	if err == nil {
		o.SetHeader("ETag", httputil.ETag(checksum))
		o.ResponseWriter.WriteHeader(http.StatusOK)
	} else {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
	}
}
