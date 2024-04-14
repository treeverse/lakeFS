package operations

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/path"
	"github.com/treeverse/lakefs/pkg/gateway/serde"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	QueryParamMaxParts = "max-parts"
	// QueryParamPartNumberMarker Specifies the part after which listing should begin. Only parts with higher part numbers will be listed.
	QueryParamPartNumberMarker       = "part-number-marker"
	s3RedirectionSupportUserAgentTag = "s3RedirectionSupport"
)

type GetObject struct{}

func (controller *GetObject) RequiredPermissions(_ *http.Request, repoID, _, path string) (permissions.Node, error) {
	return permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repoID, path),
		},
	}, nil
}

func (controller *GetObject) Handle(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	if o.HandleUnsupported(w, req, "torrent", "acl", "retention", "legal-hold", "lambdaArn") {
		return
	}
	userAgent := req.Header.Get("User-Agent")
	redirect := strings.Contains(userAgent, s3RedirectionSupportUserAgentTag)
	if redirect {
		req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{"S3_redirect": true}))
	}
	o.Incr("get_object", o.Principal, o.Repository.Name, o.Reference)
	ctx := req.Context()
	query := req.URL.Query()
	if _, exists := query["versioning"]; exists {
		o.EncodeResponse(w, req, serde.VersioningConfiguration{}, http.StatusOK)
		return
	}

	if _, exists := query["tagging"]; exists {
		o.EncodeResponse(w, req, serde.Tagging{}, http.StatusOK)
		return
	}

	// check if this is a list parts call
	if query.Has(QueryParamUploadID) {
		handleListParts(w, req, o)
		return
	}

	beforeMeta := time.Now()
	entry, err := o.Catalog.GetEntry(ctx, o.Repository.Name, o.Reference, o.Path, catalog.GetEntryParams{})
	metaTook := time.Since(beforeMeta)
	o.Log(req).
		WithField("took", metaTook).
		WithError(err).
		Debug("metadata operation to retrieve object done")

	if errors.Is(err, graveler.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchKey))
		return
	}
	if errors.Is(err, catalog.ErrExpired) {
		_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchVersion))
		return
	}
	if err != nil {
		_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}

	// TODO: the rest of https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/API_GetObject.html
	// range query
	var data io.ReadCloser
	var rng httputil.Range
	// range query
	rangeSpec := req.Header.Get("Range")
	if len(rangeSpec) > 0 {
		rng, err = httputil.ParseRange(rangeSpec, entry.Size)
		if err != nil {
			o.Log(req).WithError(err).WithField("range", rangeSpec).Debug("invalid range spec")
			if errors.Is(err, httputil.ErrUnsatisfiableRange) {
				_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInvalidRange))
				return
			}
		}
		// by here, we have a range we can use.
	}

	statusCode := http.StatusOK
	contentLength := entry.Size
	contentRange := ""
	objectPointer := block.ObjectPointer{
		StorageNamespace: o.Repository.StorageNamespace,
		IdentifierType:   entry.AddressType.ToIdentifierType(),
		Identifier:       entry.PhysicalAddress,
	}

	if redirect {
		preSignedURL, _, err := o.BlockStore.GetPreSignedURL(ctx, objectPointer, block.PreSignModeRead)
		if err != nil {
			code := gatewayerrors.ErrInternalError
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(code))
			return
		}

		o.SetHeader(w, "Location", preSignedURL)
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}

	if rangeSpec == "" || err != nil {
		// assemble a response body (range-less query)
		data, err = o.BlockStore.Get(ctx, objectPointer, entry.Size)
	} else {
		contentLength = rng.Size()
		contentRange = fmt.Sprintf("bytes %d-%d/%d", rng.StartOffset, rng.EndOffset, entry.Size)
		statusCode = http.StatusPartialContent
		data, err = o.BlockStore.GetRange(ctx, objectPointer, rng.StartOffset, rng.EndOffset)
	}
	if err != nil {
		code := gatewayerrors.ErrInternalError
		if errors.Is(err, block.ErrDataNotFound) {
			code = gatewayerrors.ErrNoSuchVersion
		}
		_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(code))
		return
	}

	o.SetHeader(w, "Last-Modified", httputil.HeaderTimestamp(entry.CreationDate))
	o.SetHeader(w, "ETag", httputil.ETag(entry.Checksum))
	o.SetHeader(w, "Content-Type", entry.ContentType)
	o.SetHeader(w, "Accept-Ranges", "bytes")
	if contentRange != "" {
		o.SetHeader(w, "Content-Range", contentRange)
	}
	o.SetHeader(w, "Content-Length", fmt.Sprintf("%d", contentLength))
	o.SetHeader(w, "X-Content-Type-Options", "nosniff")
	o.SetHeader(w, "X-Frame-Options", "SAMEORIGIN")
	o.SetHeader(w, "Content-Security-Policy", "default-src 'none'")
	amzMetaWriteHeaders(w, entry.Metadata)
	w.WriteHeader(statusCode)

	defer func() {
		_ = data.Close()
	}()
	_, err = io.Copy(w, data)
	if err != nil {
		o.Log(req).WithError(err).Error("could not write response body for object")
	}
}

func handleListParts(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("list_mpu_parts", o.Principal, o.Repository.Name, o.Reference)
	query := req.URL.Query()
	uploadID := query.Get(QueryParamUploadID)
	maxPartsStr := query.Get(QueryParamMaxParts)
	partNumberMarker := query.Get(QueryParamPartNumberMarker)
	resp := &serde.ListPartsOutput{
		Bucket: o.Repository.Name,
		Key:    path.WithRef(o.Path, o.Reference),
	}
	opts := block.ListPartsOpts{}
	if maxPartsStr != "" {
		maxParts, err := strconv.ParseInt(maxPartsStr, 10, 32)
		if err != nil {
			o.Log(req).WithField("uploadId", uploadID).
				WithField("MaxParts", maxPartsStr).
				WithError(err).Error("malformed query parameter 'MaxParts'")
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
			return
		}
		resp.MaxParts = int32(maxParts)
		opts.MaxParts = &resp.MaxParts
	}
	if partNumberMarker != "" {
		opts.PartNumberMarker = &partNumberMarker
	}

	req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{
		logging.UploadIDFieldKey: uploadID,
	}))

	multiPart, err := o.MultipartTracker.Get(req.Context(), uploadID)
	if err != nil {
		o.Log(req).WithError(err).Error("could not read multipart record")
		_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}

	partsResp, err := o.BlockStore.ListParts(req.Context(), block.ObjectPointer{
		StorageNamespace: o.Repository.StorageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       multiPart.PhysicalAddress,
	}, uploadID, opts)
	if err != nil {
		o.Log(req).WithField("uploadId", uploadID).
			WithError(err).Error("list parts failed")
		_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}
	parts := make([]serde.MultipartUploadPart, len(partsResp.Parts))
	for i, part := range partsResp.Parts {
		parts[i] = serde.MultipartUploadPart{
			PartNumber:   int32(part.PartNumber),
			ETag:         part.ETag,
			LastModified: serde.Timestamp(part.LastModified),
			Size:         part.Size,
		}
	}
	resp.IsTruncated = partsResp.IsTruncated
	resp.Parts = parts
	if partsResp.NextPartNumberMarker != nil {
		marker, err := strconv.ParseInt(*partsResp.NextPartNumberMarker, 10, 32)
		if err != nil {
			o.Log(req).WithField("uploadId", uploadID).
				WithField("NextPartNumberMarker", partsResp.NextPartNumberMarker).
				WithError(err).Error("invalid response 'NextPartNumberMarker'")
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
			return
		}
		resp.NextPartNumberMarker = int32(marker)
	}

	o.EncodeResponse(w, req, resp, http.StatusOK)
}
