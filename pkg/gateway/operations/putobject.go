package operations

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	gatewayErrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/path"
	"github.com/treeverse/lakefs/pkg/gateway/serde"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	CopySourceHeader      = "x-amz-copy-source"
	CopySourceRangeHeader = "x-amz-copy-source-range"
	QueryParamUploadID    = "uploadId"
	QueryParamPartNumber  = "partNumber"
)

type PutObject struct{}

func (controller *PutObject) RequiredPermissions(req *http.Request, repoID, _, destPath string) (permissions.Node, error) {
	copySource := req.Header.Get(CopySourceHeader)

	if len(copySource) == 0 {
		return permissions.Node{
			Permission: permissions.Permission{
				Action:   permissions.WriteObjectAction,
				Resource: permissions.ObjectArn(repoID, destPath),
			},
		}, nil
	}
	// this is a copy operation
	p, err := getPathFromSource(copySource)
	if err != nil {
		logging.Default().WithError(err).Error("could not parse copy source path")
		return permissions.Node{}, gatewayErrors.ErrInvalidCopySource
	}

	return permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.WriteObjectAction,
					Resource: permissions.ObjectArn(repoID, destPath),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadObjectAction,
					Resource: permissions.ObjectArn(p.Repo, p.Path),
				},
			},
		},
	}, nil
}

// extractEntryFromCopyReq: get metadata from source file
func extractEntryFromCopyReq(w http.ResponseWriter, req *http.Request, o *PathOperation, copySource string) *catalog.DBEntry {
	p, err := getPathFromSource(copySource)
	if err != nil {
		o.Log(req).WithError(err).Error("could not parse copy source path")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidCopySource))
		return nil
	}
	ent, err := o.Catalog.GetEntry(req.Context(), o.Repository.Name, p.Reference, p.Path, catalog.GetEntryParams{})
	if err != nil {
		o.Log(req).WithError(err).Error("could not read copy source")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidCopySource))
		return nil
	}
	return ent
}

func getPathFromSource(copySource string) (path.ResolvedAbsolutePath, error) {
	copySourceDecoded, err := url.QueryUnescape(copySource)
	if err != nil {
		copySourceDecoded = copySource
	}
	p, err := path.ResolveAbsolutePath(copySourceDecoded)
	if err != nil {
		return path.ResolvedAbsolutePath{}, gatewayErrors.ErrInvalidCopySource
	}
	return p, nil
}

func handleCopy(w http.ResponseWriter, req *http.Request, o *PathOperation, copySource string) {
	repository := o.Repository.Name
	branch := o.Reference
	o.Incr("copy_object", o.Principal, repository, branch)
	srcPath, err := getPathFromSource(copySource)
	if err != nil {
		o.Log(req).WithError(err).Error("could not parse copy source path")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidCopySource))
		return
	}
	ctx := req.Context()

	// TODO(barak): verify that o.Reference must be a branch

	// check if src and dst are in the same repository and branch
	var (
		entry    *catalog.DBEntry // set to the entry we created (shallow or copy)
		srcEntry *catalog.DBEntry // in case we load entry from staging we can reuse on full-copy
	)

	if repository == srcPath.Repo && branch == srcPath.Reference {
		// try getting source entry from staging - if not found we continue to fall to full copy
		srcEntry, err = o.Catalog.GetEntry(ctx, repository, branch, srcPath.Path, catalog.GetEntryParams{
			StageOnly: true,
		})
		if err != nil && !errors.Is(err, graveler.ErrNotFound) {
			o.Log(req).WithError(err).Error("could not read copy source")
			_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidCopySource))
			return
		}
		if srcEntry != nil {
			entry, err = copyObjectShallow(ctx, o.Catalog, repository, branch, srcEntry, o.Path)
			// for ErrTooManyTries we like to continue falling to full copy
			if err != nil && !errors.Is(err, graveler.ErrTooManyTries) {
				o.Log(req).WithError(err).Error("could create a shallow copy source")
				_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidCopyDest))
				return
			}
		}
	}

	if entry == nil {
		entry, err = copyObjectFull(ctx, o, srcPath, srcEntry)
		if err != nil {
			o.Log(req).WithError(err).Error("could create a full copy source")
			_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidCopyDest))
			return
		}
	}

	o.EncodeResponse(w, req, &serde.CopyObjectResult{
		LastModified: serde.Timestamp(entry.CreationDate),
		ETag:         httputil.ETag(entry.Checksum),
	}, http.StatusOK)
}

func copyObjectShallow(ctx context.Context, c catalog.Interface, repository string, branch string, srcEntry *catalog.DBEntry, destPath string) (*catalog.DBEntry, error) {
	// track physical address (copy-table)
	err := c.TrackPhysicalAddress(ctx, repository, srcEntry.PhysicalAddress)
	if err != nil {
		return nil, err
	}

	// create entry (copy) - try only once
	dstEntry := *srcEntry
	dstEntry.CreationDate = time.Now().UTC()
	dstEntry.Path = destPath
	err = c.CreateEntry(ctx, repository, branch, dstEntry, graveler.WithMaxTries(1))
	if err != nil {
		return nil, err
	}
	return &dstEntry, nil
}

func copyObjectFull(ctx context.Context, o *PathOperation, srcPath path.ResolvedAbsolutePath, srcEntry *catalog.DBEntry) (*catalog.DBEntry, error) {
	var err error
	// fetch src entry if needed - optimization in case we already have the entry
	if srcEntry == nil {
		srcEntry, err = o.Catalog.GetEntry(ctx, srcPath.Repo, srcPath.Reference, srcPath.Path, catalog.GetEntryParams{ReturnExpired: true})
		if err != nil {
			return nil, err
		}
	}

	srcRepo, err := o.Catalog.GetRepository(ctx, srcPath.Repo)
	if err != nil {
		return nil, err
	}

	// copy data to a new physical address
	dstEntry := *srcEntry
	dstEntry.CreationDate = time.Now().UTC()
	dstEntry.Path = o.Path
	dstEntry.AddressType = catalog.AddressTypeRelative
	dstEntry.PhysicalAddress = o.PathProvider.NewPath()
	srcObject := block.ObjectPointer{StorageNamespace: srcRepo.StorageNamespace, Identifier: srcEntry.PhysicalAddress}
	destObj := block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: dstEntry.PhysicalAddress}
	err = o.BlockStore.Copy(ctx, srcObject, destObj)
	if err != nil {
		return nil, err
	}

	// create entry for the final copy
	err = o.Catalog.CreateEntry(ctx, o.Repository.Name, o.Reference, dstEntry)
	if err != nil {
		return nil, err
	}
	return &dstEntry, nil
}

func handleUploadPart(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("put_mpu_part", o.Principal, o.Repository.Name, o.Reference)
	query := req.URL.Query()
	uploadID := query.Get(QueryParamUploadID)
	partNumberStr := query.Get(QueryParamPartNumber)

	var partNumber int
	if n, err := strconv.ParseInt(partNumberStr, 10, 32); err != nil { //nolint: gomnd
		o.Log(req).WithError(err).Error("invalid part number")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidPartNumberMarker))
		return
	} else {
		partNumber = int(n)
	}

	req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{
		logging.PartNumberFieldKey: partNumber,
		logging.UploadIDFieldKey:   uploadID,
	}))

	// handle the upload/copy itself
	multiPart, err := o.MultipartTracker.Get(req.Context(), uploadID)
	if err != nil {
		o.Log(req).WithError(err).Error("could not read  multipart record")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}

	// see if this is an upload part with a request body, or is it a copy of another object
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html#API_UploadPartCopy_RequestSyntax
	if copySource := req.Header.Get(CopySourceHeader); copySource != "" {
		// see if there's a range passed as well
		ent := extractEntryFromCopyReq(w, req, o, copySource)
		if ent == nil {
			return // operation already failed
		}

		src := block.ObjectPointer{
			StorageNamespace: o.Repository.StorageNamespace,
			Identifier:       ent.PhysicalAddress,
		}

		dst := block.ObjectPointer{
			StorageNamespace: o.Repository.StorageNamespace,
			Identifier:       multiPart.PhysicalAddress,
		}

		var resp *block.UploadPartResponse
		if rang := req.Header.Get(CopySourceRangeHeader); rang != "" {
			// if this is a copy part with a byte range:
			parsedRange, parseErr := httputil.ParseRange(rang, ent.Size)
			if parseErr != nil {
				// invalid range will silently fall back to copying the entire object. ¯\_(ツ)_/¯
				resp, err = o.BlockStore.UploadCopyPart(req.Context(), src, dst, uploadID, partNumber)
			} else {
				resp, err = o.BlockStore.UploadCopyPartRange(req.Context(), src, dst, uploadID, partNumber, parsedRange.StartOffset, parsedRange.EndOffset)
			}
		} else {
			// normal copy part that accepts another object and no byte range:
			resp, err = o.BlockStore.UploadCopyPart(req.Context(), src, dst, uploadID, partNumber)
		}

		if err != nil {
			o.Log(req).WithError(err).WithField("copy_source", ent.Path).Error("copy part " + partNumberStr + " upload failed")
			_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
			return
		}

		o.EncodeResponse(w, req, &serde.CopyObjectResult{
			LastModified: serde.Timestamp(time.Now()),
			ETag:         httputil.ETag(resp.ETag),
		}, http.StatusOK)
		return
	}

	byteSize := req.ContentLength
	resp, err := o.BlockStore.UploadPart(req.Context(), block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: multiPart.PhysicalAddress},
		byteSize, req.Body, uploadID, partNumber)
	if err != nil {
		o.Log(req).WithError(err).Error("part " + partNumberStr + " upload failed")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	o.SetHeaders(w, resp.ServerSideHeader)
	o.SetHeader(w, "ETag", httputil.ETag(resp.ETag))
	w.WriteHeader(http.StatusOK)
}

func (controller *PutObject) Handle(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	// verify branch before we upload data - fail early
	branchExists, err := o.Catalog.BranchExists(req.Context(), o.Repository.Name, o.Reference)
	if err != nil {
		o.Log(req).WithError(err).Error("could not check if branch exists")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	if !branchExists {
		o.Log(req).Debug("branch not found")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrNoSuchBucket))
		return
	}

	query := req.URL.Query()

	// check if this is a multipart upload creation call
	_, hasUploadID := query[QueryParamUploadID]
	if hasUploadID {
		handleUploadPart(w, req, o)
		return
	}

	// check if this is a copy operation (i.e. https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
	// A copy operation is identified by the existence of an "x-amz-copy-source" header
	copySource := req.Header.Get(CopySourceHeader)
	if len(copySource) > 0 {
		// The *first* PUT operation sets PutOpts such as
		// storage class, subsequent PUT operations of the
		// same file continue to use that storage class.

		// TODO(ariels): Add a counter for how often a copy has different options
		handleCopy(w, req, o, copySource)
		return
	}

	// handle the upload itself
	handlePut(w, req, o)
}

func handlePut(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("put_object", o.Principal, o.Repository.Name, o.Reference)
	storageClass := StorageClassFromHeader(req.Header)
	opts := block.PutOpts{StorageClass: storageClass}
	address := o.PathProvider.NewPath()
	blob, err := upload.WriteBlob(req.Context(), o.BlockStore, o.Repository.StorageNamespace, address, req.Body, req.ContentLength, opts)
	if err != nil {
		o.Log(req).WithError(err).Error("could not write request body to block adapter")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}

	// write metadata
	metadata := amzMetaAsMetadata(req)
	contentType := req.Header.Get("Content-Type")
	err = o.finishUpload(req, blob.Checksum, blob.PhysicalAddress, blob.Size, true, metadata, contentType)
	if errors.Is(err, graveler.ErrWriteToProtectedBranch) {
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrWriteToProtectedBranch))
		return
	}
	if err != nil {
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	o.SetHeader(w, "ETag", httputil.ETag(blob.Checksum))
	w.WriteHeader(http.StatusOK)
}
