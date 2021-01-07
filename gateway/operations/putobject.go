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

func (controller *PutObject) HandleCopy(w http.ResponseWriter, r *http.Request, o *PathOperation, copySource string) {
	o.Incr("copy_object")
	// resolve source branch and source path
	copySourceDecoded, err := url.QueryUnescape(copySource)
	if err != nil {
		copySourceDecoded = copySource
	}
	p, err := path.ResolveAbsolutePath(copySourceDecoded)
	if err != nil {
		o.Log(r).WithError(err).Error("could not parse copy source path")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// validate src and dst are in the same repository
	if !strings.EqualFold(o.Repository.Name, p.Repo) {
		o.Log(r).WithError(err).Error("cannot copy objects across repos")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// update metadata to refer to the source hash in the destination workspace
	ent, err := o.Cataloger.GetEntry(o.Context(r), o.Repository.Name, p.Reference, p.Path, catalog.GetEntryParams{})
	if err != nil {
		o.Log(r).WithError(err).Error("could not read copy source")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}
	// write this object to workspace
	// TODO: move this logic into the Index impl.
	ent.CreationDate = time.Now()
	ent.Path = o.Path
	err = o.Cataloger.CreateEntry(o.Context(r), o.Repository.Name, o.Reference, *ent, catalog.CreateEntryParams{})
	if err != nil {
		o.Log(r).WithError(err).Error("could not write copy destination")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInvalidCopyDest))
		return
	}

	o.EncodeResponse(w, r, &serde.CopyObjectResult{
		LastModified: serde.Timestamp(ent.CreationDate),
		ETag:         fmt.Sprintf("\"%s\"", ent.Checksum),
	}, http.StatusOK)
}

func (controller *PutObject) HandleUploadPart(w http.ResponseWriter, r *http.Request, o *PathOperation) {
	o.Incr("put_mpu_part")
	query := r.URL.Query()
	uploadID := query.Get(QueryParamUploadID)
	partNumberStr := query.Get(QueryParamPartNumber)

	partNumber, err := strconv.ParseInt(partNumberStr, 10, 64)
	if err != nil {
		o.Log(r).WithError(err).Error("invalid part number")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInvalidPartNumberMarker))
		return
	}

	o.AddLogFields(r, logging.Fields{
		"part_number": partNumber,
		"upload_id":   uploadID,
	})

	// handle the upload itself
	multiPart, err := o.MultipartsTracker.Get(o.Context(r), uploadID)
	if err != nil {
		o.Log(r).WithError(err).Error("could not read  multipart record")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	byteSize := r.ContentLength
	etag, err := o.BlockStore.UploadPart(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: multiPart.PhysicalAddress},
		byteSize, r.Body, uploadID, partNumber)
	if err != nil {
		o.Log(r).WithError(err).Error("part " + partNumberStr + " upload failed")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.SetHeader(w, "ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (controller *PutObject) Handle(w http.ResponseWriter, r *http.Request, o *PathOperation) {
	// verify branch before we upload data - fail early
	branchExists, err := o.Cataloger.BranchExists(o.Context(r), o.Repository.Name, o.Reference)
	if err != nil {
		o.Log(r).WithError(err).Error("could not check if branch exists")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	if !branchExists {
		o.Log(r).Debug("branch not found")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrNoSuchBucket))
		return
	}

	// check if this is a copy operation (i.e. https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
	// A copy operation is identified by the existence of an "x-amz-copy-source" header
	storageClass := StorageClassFromHeader(r.Header)
	opts := block.PutOpts{StorageClass: storageClass}

	copySource := r.Header.Get(CopySourceHeader)
	if len(copySource) > 0 {
		// The *first* PUT operation sets PutOpts such as
		// storage class, subsequent PUT operations of the
		// same file continue to use that storage class.

		// TODO(ariels): Add a counter for how often a copy has different options
		controller.HandleCopy(w, r, o, copySource)
		return
	}

	query := r.URL.Query()

	// check if this is a multipart upload creation call
	_, hasUploadID := query[QueryParamUploadID]
	if hasUploadID {
		controller.HandleUploadPart(w, r, o)
		return
	}

	o.Incr("put_object")
	// handle the upload itself
	blob, err := upload.WriteBlob(o.BlockStore, o.Repository.StorageNamespace, r.Body, r.ContentLength, opts)
	if err != nil {
		o.Log(r).WithError(err).Error("could not write request body to block adapter")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// write metadata
	err = o.finishUpload(r, o.Repository.StorageNamespace, blob.Checksum, blob.PhysicalAddress, blob.Size)
	if err != nil {
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.SetHeader(w, "ETag", httputil.ETag(blob.Checksum))
	w.WriteHeader(http.StatusOK)
}
