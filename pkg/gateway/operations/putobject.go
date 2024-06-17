package operations

import (
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
		logging.FromContext(req.Context()).WithError(err).Error("could not parse copy source path")
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
func extractEntryFromCopyReq(w http.ResponseWriter, req *http.Request, o *PathOperation, copySource path.ResolvedAbsolutePath) *catalog.DBEntry {
	ent, err := o.Catalog.GetEntry(req.Context(), copySource.Repo, copySource.Reference, copySource.Path, catalog.GetEntryParams{})
	if err != nil {
		o.Log(req).WithError(err).Error("could not read copy source")
		_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidCopySource))
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
		// This is a solution to avoid misleading error messages in gateway. This is a pinpoint fix for the copy object
		// API, since we decided not to change the entire gateway error handling in order to avoid breaking changes.
		// See: https://github.com/treeverse/lakeFS/issues/7452
		apiErr := gatewayErrors.Codes.ToAPIErrWithInternalError(gatewayErrors.ErrInvalidCopySource, err)
		_ = o.EncodeError(w, req, err, apiErr)
		return
	}

	ctx := req.Context()
	entry, err := o.Catalog.CopyEntry(ctx, srcPath.Repo, srcPath.Reference, srcPath.Path, repository, branch, o.Path)
	if err != nil {
		o.Log(req).WithError(err).Error("could create a copy")
		apiErr := gatewayErrors.Codes.ToAPIErrWithInternalError(gatewayErrors.ErrInvalidCopyDest, err)
		_ = o.EncodeError(w, req, err, apiErr)
		return
	}

	o.EncodeResponse(w, req, &serde.CopyObjectResult{
		LastModified: serde.Timestamp(entry.CreationDate),
		ETag:         httputil.ETag(entry.Checksum),
	}, http.StatusOK)
}

func handleUploadPart(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("put_mpu_part", o.Principal, o.Repository.Name, o.Reference)
	query := req.URL.Query()
	uploadID := query.Get(QueryParamUploadID)
	partNumberStr := query.Get(QueryParamPartNumber)

	var partNumber int
	if n, err := strconv.ParseInt(partNumberStr, 10, 32); err != nil { //nolint: mnd
		o.Log(req).WithError(err).Error("invalid part number")
		_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidPartNumberMarker))
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
		_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}

	// see if this is an upload part with a request body, or is it a copy of another object
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html#API_UploadPartCopy_RequestSyntax
	if copySource := req.Header.Get(CopySourceHeader); copySource != "" {
		// see if there's a range passed as well
		resolvedCopySource, err := getPathFromSource(copySource)
		if err != nil {
			o.Log(req).WithField("copy_source", copySource).WithError(err).Error("could not parse copy source path")
			_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidCopySource))
			return
		}
		ent := extractEntryFromCopyReq(w, req, o, resolvedCopySource)
		if ent == nil {
			return // operation already failed
		}
		srcRepo := o.Repository
		if resolvedCopySource.Repo != o.Repository.Name {
			srcRepo, err = o.Catalog.GetRepository(req.Context(), resolvedCopySource.Repo)
			if err != nil {
				o.Log(req).
					WithField("copy_source", copySource).
					WithError(err).
					Error("Failed to get repository")
				_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInvalidCopySource))
				return
			}
		}

		src := block.ObjectPointer{
			StorageNamespace: srcRepo.StorageNamespace,
			IdentifierType:   ent.AddressType.ToIdentifierType(),
			Identifier:       ent.PhysicalAddress,
		}

		dst := block.ObjectPointer{
			StorageNamespace: o.Repository.StorageNamespace,
			IdentifierType:   block.IdentifierTypeRelative,
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
			o.Log(req).
				WithFields(logging.Fields{
					"copy_source": ent.Path,
					"part":        partNumberStr,
				}).
				WithError(err).
				Error("copy part: upload failed")
			_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
			return
		}

		o.EncodeResponse(w, req, &serde.CopyObjectResult{
			LastModified: serde.Timestamp(time.Now()),
			ETag:         httputil.ETag(resp.ETag),
		}, http.StatusOK)
		return
	}

	byteSize := req.ContentLength
	resp, err := o.BlockStore.UploadPart(req.Context(), block.ObjectPointer{
		StorageNamespace: o.Repository.StorageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       multiPart.PhysicalAddress,
	},
		byteSize, req.Body, uploadID, partNumber)
	if err != nil {
		o.Log(req).WithField("part", partNumberStr).
			WithError(err).Error("part upload failed")
		_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	o.SetHeaders(w, resp.ServerSideHeader)
	o.SetHeader(w, "ETag", httputil.ETag(resp.ETag))
	w.WriteHeader(http.StatusOK)
}

func (controller *PutObject) Handle(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	if o.HandleUnsupported(w, req, "torrent", "acl") {
		return
	}
	if o.Repository.ReadOnly {
		_ = o.EncodeError(w, req, nil, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrReadOnlyRepository))
		return
	}

	// verify branch before we upload data - fail early
	branchExists, err := o.Catalog.BranchExists(req.Context(), o.Repository.Name, o.Reference)
	if err != nil {
		o.Log(req).WithError(err).Error("could not check if branch exists")
		_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	if !branchExists {
		o.Log(req).Debug("branch not found")
		_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrNoSuchBucket))
		return
	}

	query := req.URL.Query()

	// check if this is a multipart upload creation call
	if query.Has(QueryParamUploadID) {
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

	if query.Has("tagging") {
		o.Log(req).Debug("put-object-tagging isn't supported yet")
		o.EncodeError(w, req, nil, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ERRLakeFSNotSupported))
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
		_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}

	// write metadata
	metadata := amzMetaAsMetadata(req)
	contentType := req.Header.Get("Content-Type")
	err = o.finishUpload(req, blob.Checksum, blob.PhysicalAddress, blob.Size, true, metadata, contentType)
	if errors.Is(err, graveler.ErrWriteToProtectedBranch) {
		_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrWriteToProtectedBranch))
		return
	}
	if errors.Is(err, graveler.ErrReadOnlyRepository) {
		_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrReadOnlyRepository))
		return
	}
	if err != nil {
		_ = o.EncodeError(w, req, err, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	o.SetHeader(w, "ETag", httputil.ETag(blob.Checksum))
	w.WriteHeader(http.StatusOK)
}
