package operations

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/upload"
)

const (
	CopySourceHeader     = "x-amz-copy-source"
	QueryParamUploadID   = "uploadId"
	QueryParamPartNumber = "partNumber"
)

type PutObject struct{}

func (controller *PutObject) RequiredPermissions(_ *http.Request, repoID, _, path string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repoID, path),
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

	// validate src and dst are in the same repository
	if !strings.EqualFold(o.Repository.Name, p.Repo) {
		o.Log().WithError(err).Error("cannot copy objects across repos")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// update metadata to refer to the source hash in the destination workspace
	ent, err := o.Cataloger.GetEntry(o.Context(), o.Repository.Name, p.Reference, p.Path, catalog.GetEntryParams{})
	if err != nil {
		o.Log().WithError(err).Error("could not read copy source")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}
	// write this object to workspace
	// TODO: move this logic into the Index impl.
	ent.CreationDate = time.Now()
	ent.Path = o.Path
	err = o.Cataloger.CreateEntry(o.Context(), o.Repository.Name, o.Reference, *ent, catalog.CreateEntryParams{})
	if err != nil {
		o.Log().WithError(err).Error("could not write copy destination")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopyDest))
		return
	}

	o.EncodeResponse(&serde.CopyObjectResult{
		LastModified: serde.Timestamp(ent.CreationDate),
		ETag:         fmt.Sprintf("\"%s\"", ent.Checksum),
	}, http.StatusOK)
}

func (controller *PutObject) HandleUploadPart(o *PathOperation) {
	o.Incr("put_mpu_part")
	query := o.Request.URL.Query()
	uploadID := query.Get(QueryParamUploadID)
	partNumberStr := query.Get(QueryParamPartNumber)

	partNumber, err := strconv.ParseInt(partNumberStr, 10, 64)
	if err != nil {
		o.Log().WithError(err).Error("invalid part number")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidPartNumberMarker))
		return
	}

	o.AddLogFields(logging.Fields{
		"part_number": partNumber,
		"upload_id":   uploadID,
	})

	// handle the upload itself
	multiPart, err := o.Cataloger.GetMultipartUpload(o.Context(), o.Repository.Name, uploadID)
	if err != nil {
		o.Log().WithError(err).Error("could not read  multipart record")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	byteSize := o.Request.ContentLength
	etag, err := o.BlockStore.UploadPart(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: multiPart.PhysicalAddress},
		byteSize, o.Request.Body, uploadID, partNumber)
	if err != nil {
		o.Log().WithError(err).Error("part " + partNumberStr + " upload failed")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.SetHeader("ETag", etag)
	o.ResponseWriter.WriteHeader(http.StatusOK)
}

func (controller *PutObject) Handle(o *PathOperation) {
	// verify branch before we upload data - fail early
	branchExists, err := o.Cataloger.BranchExists(o.Context(), o.Repository.Name, o.Reference)
	if err != nil {
		o.Log().WithError(err).Error("could not check if branch exists")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	if !branchExists {
		o.Log().Debug("branch not found")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchBucket))
		return
	}

	// check if this is a copy operation (i.e. https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
	// A copy operation is identified by the existence of an "x-amz-copy-source" header
	storageClass := StorageClassFromHeader(o.Request.Header)
	opts := block.PutOpts{StorageClass: storageClass}

	copySource := o.Request.Header.Get(CopySourceHeader)
	if len(copySource) > 0 {
		// The *first* PUT operation sets PutOpts such as
		// storage class, subsequent PUT operations of the
		// same file continue to use that storage class.

		// TODO(ariels): Add a counter for how often a copy has different options
		controller.HandleCopy(o, copySource)
		return
	}

	query := o.Request.URL.Query()

	// check if this is a multipart upload creation call
	_, hasUploadID := query[QueryParamUploadID]
	if hasUploadID {
		controller.HandleUploadPart(o)
		return
	}

	o.Incr("put_object")
	// handle the upload itself
	blob, err := upload.WriteBlob(o.BlockStore, o.Repository.StorageNamespace, o.Request.Body, o.Request.ContentLength, opts)
	if err != nil {
		o.Log().WithError(err).Error("could not write request body to block adapter")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// write metadata
	err = o.finishUpload(o.Repository.StorageNamespace, blob.Checksum, blob.PhysicalAddress, blob.Size)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.SetHeader("ETag", httputil.ETag(blob.Checksum))
	o.ResponseWriter.WriteHeader(http.StatusOK)
}
