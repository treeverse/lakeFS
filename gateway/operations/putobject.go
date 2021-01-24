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

func (controller *PutObject) HandleCopy(w http.ResponseWriter, req *http.Request, o *PathOperation, copySource string) {
	o.Incr("copy_object")
	// resolve source branch and source path
	copySourceDecoded, err := url.QueryUnescape(copySource)
	if err != nil {
		copySourceDecoded = copySource
	}
	p, err := path.ResolveAbsolutePath(copySourceDecoded)
	if err != nil {
		o.Log(req).WithError(err).Error("could not parse copy source path")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// validate src and dst are in the same repository
	if !strings.EqualFold(o.Repository.Name, p.Repo) {
		o.Log(req).WithError(err).Error("cannot copy objects across repos")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// update metadata to refer to the source hash in the destination workspace
	ent, err := o.Cataloger.GetEntry(req.Context(), o.Repository.Name, p.Reference, p.Path, catalog.GetEntryParams{})
	if err != nil {
		o.Log(req).WithError(err).Error("could not read copy source")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}
	// write this object to workspace
	// TODO: move this logic into the Index impl.
	ent.CreationDate = time.Now()
	ent.Path = o.Path
	err = o.Cataloger.CreateEntry(req.Context(), o.Repository.Name, o.Reference, *ent, catalog.CreateEntryParams{})
	if err != nil {
		o.Log(req).WithError(err).Error("could not write copy destination")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInvalidCopyDest))
		return
	}

	o.EncodeResponse(w, req, &serde.CopyObjectResult{
		LastModified: serde.Timestamp(ent.CreationDate),
		ETag:         fmt.Sprintf("\"%s\"", ent.Checksum),
	}, http.StatusOK)
}

func (controller *PutObject) HandleUploadPart(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("put_mpu_part")
	query := req.URL.Query()
	uploadID := query.Get(QueryParamUploadID)
	partNumberStr := query.Get(QueryParamPartNumber)

	partNumber, err := strconv.ParseInt(partNumberStr, 10, 64)
	if err != nil {
		o.Log(req).WithError(err).Error("invalid part number")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInvalidPartNumberMarker))
		return
	}

	req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{
		"part_number": partNumber,
		"upload_id":   uploadID,
	}))

	// handle the upload itself
	multiPart, err := o.MultipartsTracker.Get(req.Context(), uploadID)
	if err != nil {
		o.Log(req).WithError(err).Error("could not read  multipart record")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	byteSize := req.ContentLength
	etag, err := o.BlockStore.UploadPart(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: multiPart.PhysicalAddress},
		byteSize, req.Body, uploadID, partNumber)
	if err != nil {
		o.Log(req).WithError(err).Error("part " + partNumberStr + " upload failed")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.SetHeader(w, "ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (controller *PutObject) Handle(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	// verify branch before we upload data - fail early
	branchExists, err := o.Cataloger.BranchExists(req.Context(), o.Repository.Name, o.Reference)
	if err != nil {
		o.Log(req).WithError(err).Error("could not check if branch exists")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	if !branchExists {
		o.Log(req).Debug("branch not found")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrNoSuchBucket))
		return
	}

	// check if this is a copy operation (i.e. https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
	// A copy operation is identified by the existence of an "x-amz-copy-source" header
	storageClass := StorageClassFromHeader(req.Header)
	opts := block.PutOpts{StorageClass: storageClass}

	copySource := req.Header.Get(CopySourceHeader)
	if len(copySource) > 0 {
		// The *first* PUT operation sets PutOpts such as
		// storage class, subsequent PUT operations of the
		// same file continue to use that storage class.

		// TODO(ariels): Add a counter for how often a copy has different options
		controller.HandleCopy(w, req, o, copySource)
		return
	}

	query := req.URL.Query()

	// check if this is a multipart upload creation call
	_, hasUploadID := query[QueryParamUploadID]
	if hasUploadID {
		controller.HandleUploadPart(w, req, o)
		return
	}

	o.Incr("put_object")
	// handle the upload itself
	blob, err := upload.WriteBlob(o.BlockStore, o.Repository.StorageNamespace, req.Body, req.ContentLength, opts)
	if err != nil {
		o.Log(req).WithError(err).Error("could not write request body to block adapter")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// write metadata
	err = o.finishUpload(req, o.Repository.StorageNamespace, blob.Checksum, blob.PhysicalAddress, blob.Size)
	if err != nil {
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.SetHeader(w, "ETag", httputil.ETag(blob.Checksum))
	w.WriteHeader(http.StatusOK)
}
