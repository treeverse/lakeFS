package operations

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/path"
	"github.com/treeverse/lakefs/pkg/gateway/serde"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	ListObjectMaxKeys = 1000
	maxUploadsListMPU = 1000

	// defaultBucketLocation used to identify if we need to specify the location constraint
	defaultBucketLocation    = "us-east-1"
	QueryParamMaxUploads     = "max-uploads"
	QueryParamUploadIDMarker = "upload-id-marker"
	QueryParamKeyMarker      = "key-marker"
	// missing implementation - will return error
	QueryParamPrefix       = "prefix"
	QueryParamEncodingType = "encoding-type"
	QueryParamDelimiter    = "delimiter"
)

type ListObjects struct{}

func (controller *ListObjects) RequiredPermissions(req *http.Request, repoID string) (permissions.Node, error) {
	// check if we're listing files in a branch, or listing branches
	params := req.URL.Query()
	delimiter := params.Get("delimiter")
	prefix := params.Get("prefix")
	if delimiter == "/" && !strings.Contains(prefix, "/") {
		return permissions.Node{
			Permission: permissions.Permission{
				Action:   permissions.ListBranchesAction,
				Resource: permissions.RepoArn(repoID),
			},
		}, nil
	}

	// otherwise, we're listing objects within a branch
	return permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListObjectsAction,
			Resource: permissions.RepoArn(repoID),
		},
	}, nil
}

func (controller *ListObjects) getMaxKeys(req *http.Request, _ *RepoOperation) int {
	params := req.URL.Query()
	maxKeys := ListObjectMaxKeys
	maxKeysParam := params.Get("max-keys")
	if len(maxKeysParam) > 0 {
		parsedKeys, err := strconv.Atoi(maxKeysParam)
		if err == nil {
			maxKeys = parsedKeys
		}
	}
	return maxKeys
}

func (controller *ListObjects) serializeEntries(ref string, entries []*catalog.DBEntry) ([]serde.CommonPrefixes, []serde.Contents, string) {
	dirs := make([]serde.CommonPrefixes, 0)
	files := make([]serde.Contents, 0)
	var lastKey string
	for _, entry := range entries {
		lastKey = entry.Path
		if entry.CommonLevel {
			dirs = append(dirs, serde.CommonPrefixes{Prefix: path.WithRef(entry.Path, ref)})
		} else {
			files = append(files, serde.Contents{
				Key:          path.WithRef(entry.Path, ref),
				LastModified: serde.Timestamp(entry.CreationDate),
				ETag:         httputil.ETag(entry.Checksum),
				Size:         entry.Size,
				StorageClass: "STANDARD",
			})
		}
	}
	return dirs, files, lastKey
}

func (controller *ListObjects) serializeBranches(branches []*catalog.Branch) ([]serde.CommonPrefixes, string) {
	dirs := make([]serde.CommonPrefixes, 0)
	var lastKey string
	for _, branch := range branches {
		lastKey = branch.Name
		dirs = append(dirs, serde.CommonPrefixes{Prefix: path.WithRef("", branch.Name)})
	}
	return dirs, lastKey
}

func (controller *ListObjects) ListV2(w http.ResponseWriter, req *http.Request, o *RepoOperation) {
	req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{
		logging.ListTypeFieldKey: "v2",
	}))
	params := req.URL.Query()
	delimiter := params.Get("delimiter")
	startAfter := params.Get("start-after")
	continuationToken := params.Get("continuation-token")

	// resolve "from"
	var fromStr string
	if len(startAfter) > 0 {
		fromStr = startAfter
	}
	if len(continuationToken) > 0 {
		// take this instead
		fromStr = continuationToken
	}

	maxKeys := controller.getMaxKeys(req, o)

	var results []*catalog.DBEntry
	var hasMore bool
	var ref string
	// should we list branches?
	prefix, err := path.ResolvePath(params.Get("prefix"))
	if err != nil {
		o.Log(req).
			WithError(err).
			WithField("path", params.Get("prefix")).
			Error("could not resolve path for prefix")
		_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
		return
	}

	var from path.ResolvedPath
	if !prefix.WithPath {
		// list branches then.
		branchPrefix := prefix.Ref // TODO: same prefix logic also in V1!!!!!
		o.Log(req).WithField("prefix", branchPrefix).Debug("listing branches with prefix")
		branches, hasMore, err := o.Catalog.ListBranches(req.Context(), o.Repository.Name, branchPrefix, maxKeys, fromStr)
		if err != nil {
			o.Log(req).WithError(err).Error("could not list branches")
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
			return
		}
		// return branch response
		dirs, lastKey := controller.serializeBranches(branches)
		resp := serde.ListObjectsV2Output{
			Name:           o.Repository.Name,
			Prefix:         params.Get("prefix"),
			Delimiter:      delimiter,
			KeyCount:       len(dirs),
			MaxKeys:        maxKeys,
			CommonPrefixes: dirs,
			Contents:       make([]serde.Contents, 0),
		}

		if len(continuationToken) > 0 && strings.EqualFold(continuationToken, fromStr) {
			resp.ContinuationToken = continuationToken
		}

		if hasMore {
			resp.IsTruncated = true
			resp.NextContinuationToken = lastKey
		}

		o.EncodeResponse(w, req, resp, http.StatusOK)
		return
	} else {
		// list objects then.
		ref = prefix.Ref
		if len(fromStr) > 0 {
			from, err = path.ResolvePath(fromStr)
			if err != nil || !strings.EqualFold(from.Ref, prefix.Ref) {
				o.Log(req).WithError(err).WithFields(logging.Fields{
					"branch": prefix.Ref,
					"path":   prefix.Path,
					"from":   fromStr,
				}).Error("invalid marker - doesnt start with branch name")
				_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
				return
			}
		}

		results, hasMore, err = o.Catalog.ListEntries(
			req.Context(),
			o.Repository.Name,
			prefix.Ref,
			prefix.Path,
			from.Path,
			delimiter,
			maxKeys,
		)
		log := o.Log(req).WithError(err).WithFields(logging.Fields{
			"ref":  prefix.Ref,
			"path": prefix.Path,
		})
		if errors.Is(err, graveler.ErrBranchNotFound) {
			log.Debug("could not list objects in path")
		} else if err != nil {
			log.Error("could not list objects in path")
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
			return
		}
	}

	dirs, files, lastKey := controller.serializeEntries(ref, results)
	resp := serde.ListObjectsV2Output{
		Name:           o.Repository.Name,
		Prefix:         params.Get("prefix"),
		Delimiter:      delimiter,
		KeyCount:       len(results),
		MaxKeys:        maxKeys,
		CommonPrefixes: dirs,
		Contents:       files,
	}

	if len(continuationToken) > 0 && strings.EqualFold(continuationToken, fromStr) {
		resp.ContinuationToken = continuationToken
	}

	if hasMore {
		resp.IsTruncated = true
		resp.NextContinuationToken = path.WithRef(lastKey, ref)
	}

	o.EncodeResponse(w, req, resp, http.StatusOK)
}

func (controller *ListObjects) ListV1(w http.ResponseWriter, req *http.Request, o *RepoOperation) {
	req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{
		logging.ListTypeFieldKey: "v1",
	}))
	// handle ListObjects (v1)
	params := req.URL.Query()
	delimiter := params.Get("delimiter")
	descend := len(delimiter) < 1
	maxKeys := controller.getMaxKeys(req, o)

	var results []*catalog.DBEntry
	hasMore := false

	var ref string
	// should we list branches?
	prefix, err := path.ResolvePath(params.Get("prefix"))
	if err != nil {
		o.Log(req).
			WithError(err).
			WithField("path", params.Get("prefix")).
			Error("could not resolve path for prefix")
		_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
		return
	}

	if !prefix.WithPath {
		// list branches then.
		branches, hasMore, err := o.Catalog.ListBranches(req.Context(), o.Repository.Name, prefix.Ref, maxKeys, params.Get("marker"))
		if err != nil {
			// TODO incorrect error type
			o.Log(req).WithError(err).Error("could not list branches")
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
			return
		}
		// return branch response
		dirs, lastKey := controller.serializeBranches(branches)
		resp := serde.ListBucketResult{
			Name:           o.Repository.Name,
			Prefix:         params.Get("prefix"),
			Delimiter:      delimiter,
			Marker:         params.Get("marker"),
			KeyCount:       len(results),
			MaxKeys:        maxKeys,
			CommonPrefixes: dirs,
			Contents:       make([]serde.Contents, 0),
		}

		if hasMore {
			resp.IsTruncated = true
			if !descend {
				// NextMarker is only set if a delimiter exists
				resp.NextMarker = lastKey
			}
		}

		o.EncodeResponse(w, req, resp, http.StatusOK)
		return
	} else {
		prefix, err := path.ResolvePath(params.Get("prefix"))
		if err != nil {
			o.Log(req).WithError(err).Error("could not list branches")
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
			return
		}
		ref = prefix.Ref
		// see if we have a continuation token in the request to pick up from
		var marker path.ResolvedPath
		// strip the branch from the marker
		if len(params.Get("marker")) > 0 {
			marker, err = path.ResolvePath(params.Get("marker"))
			if err != nil || !strings.EqualFold(marker.Ref, prefix.Ref) {
				o.Log(req).WithError(err).WithFields(logging.Fields{
					"branch": prefix.Ref,
					"path":   prefix.Path,
					"marker": marker,
				}).Error("invalid marker - doesnt start with branch name")
				_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
				return
			}
		}
		results, hasMore, err = o.Catalog.ListEntries(
			req.Context(),
			o.Repository.Name,
			prefix.Ref,
			prefix.Path,
			marker.Path,
			delimiter,
			maxKeys,
		)
		if errors.Is(err, graveler.ErrNotFound) {
			results = make([]*catalog.DBEntry, 0) // no results found
		} else if err != nil {
			o.Log(req).WithError(err).WithFields(logging.Fields{
				"branch": prefix.Ref,
				"path":   prefix.Path,
			}).Error("could not list objects in path")
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
			return
		}
	}

	// build a response
	dirs, files, lastKey := controller.serializeEntries(ref, results)
	resp := serde.ListBucketResult{
		Name:           o.Repository.Name,
		Prefix:         params.Get("prefix"),
		Delimiter:      delimiter,
		Marker:         params.Get("marker"),
		KeyCount:       len(results),
		MaxKeys:        maxKeys,
		CommonPrefixes: dirs,
		Contents:       files,
	}

	if hasMore {
		resp.IsTruncated = true
		if !descend {
			// NextMarker is only set if a delimiter exists
			resp.NextMarker = path.WithRef(lastKey, ref)
		}
	}

	o.EncodeResponse(w, req, resp, http.StatusOK)
}

func (controller *ListObjects) Handle(w http.ResponseWriter, req *http.Request, o *RepoOperation) {
	if o.HandleUnsupported(w, req, "inventory", "metrics", "publicAccessBlock", "ownershipControls",
		"intelligent-tiering", "analytics", "policy", "lifecycle", "encryption", "object-lock", "replication",
		"notification", "events", "acl", "cors", "website", "accelerate",
		"requestPayment", "logging", "tagging", "versions", "policyStatus") {
		return
	}
	query := req.URL.Query()

	// getbucketlocation support
	if query.Has("location") {
		o.Incr("get_bucket_location", o.Principal, o.Repository.Name, "")
		response := serde.LocationResponse{}
		if o.Region != "" && o.Region != defaultBucketLocation {
			response.Location = o.Region
		}
		o.EncodeResponse(w, req, response, http.StatusOK)
		return
	}
	// check if request is list-multipart-uploads
	if query.Has("uploads") {
		handleListMultipartUploads(w, req, o)
		return
	}
	// getbucketversioing support
	if query.Has("versioning") {
		o.EncodeXMLBytes(w, req, []byte(serde.VersioningResponse), http.StatusOK)
		return
	}
	o.Incr("list_objects", o.Principal, o.Repository.Name, "")

	// parse request parameters
	// GET /example?list-type=2&prefix=main%2F&delimiter=%2F&encoding-type=url HTTP/1.1

	// handle ListObjects versions
	listType := query.Get("list-type")
	switch listType {
	case "", "1":
		controller.ListV1(w, req, o)
	case "2":
		controller.ListV2(w, req, o)
	default:
		o.Log(req).WithField("list-type", listType).Error("listObjects version not supported")
		_ = o.EncodeError(w, req, nil, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
	}
}

func handleListMultipartUploads(w http.ResponseWriter, req *http.Request, o *RepoOperation) {
	o.Incr("list_multipart_uploads", o.Principal, o.Repository.Name, "")
	query := req.URL.Query()
	maxUploadsStr := query.Get(QueryParamMaxUploads)
	uploadIDMarker := query.Get(QueryParamUploadIDMarker)
	keyMarker := query.Get(QueryParamKeyMarker)
	prefix := query.Get(QueryParamPrefix)
	delimiter := query.Get(QueryParamDelimiter)
	encodingType := query.Get(QueryParamEncodingType)
	if prefix != "" || delimiter != "" || encodingType != "" {
		_ = o.EncodeError(w, req, gatewayerrors.ErrNotImplemented, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNotImplemented))
		return
	}
	opts := block.ListMultipartUploadsOpts{}
	if maxUploadsStr != "" {
		maxUploads, err := strconv.ParseInt(maxUploadsStr, 10, 32)
		maxUploads32 := int32(maxUploads)
		if err != nil || maxUploads < 0 {
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInvalidMaxUploads))
			return
		}
		if maxUploads > maxUploadsListMPU {
			maxUploads32 = maxUploadsListMPU
		}
		opts.MaxUploads = &maxUploads32
	}
	if uploadIDMarker != "" {
		opts.UploadIDMarker = &uploadIDMarker
	}
	if keyMarker != "" {
		opts.KeyMarker = &keyMarker
	}
	mpuResp, err := o.BlockStore.ListMultipartUploads(req.Context(), block.ObjectPointer{
		StorageID:        o.Repository.StorageID,
		StorageNamespace: o.Repository.StorageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
	}, opts)
	if err != nil {
		o.Log(req).WithError(err).Error("list multipart uploads failed")
		_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNotImplemented))
		return
	}

	uploads := make([]serde.Upload, 0, len(mpuResp.Uploads))
	for _, upload := range mpuResp.Uploads {
		if upload.UploadId == nil {
			continue
		}
		mpu, err := o.MultipartTracker.Get(req.Context(), *upload.UploadId)
		if err != nil {
			if errors.Is(err, kv.ErrNotFound) {
				continue
			}
			o.Log(req).WithError(err).Error("could not read multipart record %s", *upload.UploadId)
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
			return
		}
		uploads = append(uploads, serde.Upload{
			Key:      mpu.Path,
			UploadID: *upload.UploadId,
		})
	}
	resp := &serde.ListMultipartUploadsOutput{
		Bucket:             o.Repository.Name,
		Uploads:            uploads,
		NextKeyMarker:      aws.ToString(mpuResp.NextKeyMarker),
		NextUploadIDMarker: aws.ToString(mpuResp.NextUploadIDMarker),
		IsTruncated:        mpuResp.IsTruncated,
		MaxUploads:         aws.ToInt32(mpuResp.MaxUploads),
	}
	o.EncodeResponse(w, req, resp, http.StatusOK)
}
