package operations

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
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

// errUnhandledBucketGetKind backs the tripwires in RequiredPermissions and Handle. It is a sentinel
// so the tripwire is assertable with errors.Is, and deliberately not path.ErrPathMalformed:
// RepoOperationHandler maps that to 400, and an unhandled kind must fail closed with 403 instead.
var errUnhandledBucketGetKind = errors.New("unhandled bucket GET kind")

type ListObjects struct{}

// bucketGetKind is the operation a GET on a bucket resolves to. Every decision about such a
// request (the permission it requires and the handler it reaches) must derive from the same
// classifyBucketGet result, so the two cannot disagree.
type bucketGetKind int

const (
	// bucketGetInvalid is the zero value. classifyBucketGet returns it only alongside a non-nil
	// error, so a caller that acts on the kind without checking the error cannot mistake it for a
	// real operation.
	bucketGetInvalid bucketGetKind = iota
	bucketGetLocation
	bucketGetMultipartUploads
	bucketGetVersioning
	bucketGetBranches
	bucketGetObjects
)

// classifyBucketGet is a pure function of the request, so it is safe to evaluate more than once.
// The resolved prefix is meaningful only for the listing kinds; the bucket sub-resources do not
// read it.
//
// The unsupported sub-resources handled by HandleUnsupported are deliberately not classified here:
// that check is gated on gateways.s3.verify_unsupported, and when it is disabled those requests fall
// through to ListV1 and genuinely are listings.
func classifyBucketGet(req *http.Request) (bucketGetKind, path.ResolvedPath, error) {
	query := req.URL.Query()
	switch {
	case query.Has("location"):
		return bucketGetLocation, path.ResolvedPath{}, nil
	case query.Has("uploads"):
		return bucketGetMultipartUploads, path.ResolvedPath{}, nil
	case query.Has("versioning"):
		return bucketGetVersioning, path.ResolvedPath{}, nil
	}

	prefix, err := path.ResolvePath(query.Get(QueryParamPrefix))
	switch {
	case err != nil:
		return bucketGetInvalid, path.ResolvedPath{}, err
	case prefix.WithPath:
		return bucketGetObjects, prefix, nil
	default:
		return bucketGetBranches, prefix, nil
	}
}

func (controller *ListObjects) RequiredPermissions(req *http.Request, repoID string) (permissions.Node, error) {
	kind, _, err := classifyBucketGet(req)
	if err != nil {
		// The operation being requested cannot be determined, so neither can the permission it
		// requires. Fail closed rather than assuming an object listing.
		logging.FromContext(req.Context()).WithError(err).
			WithField("path", req.URL.Query().Get(QueryParamPrefix)).
			Error("could not resolve path for prefix")
		return permissions.Node{}, err
	}

	var action string
	switch kind {
	case bucketGetBranches:
		action = permissions.ListBranchesAction
	case bucketGetLocation, bucketGetMultipartUploads, bucketGetVersioning, bucketGetObjects:
		// GetBucketLocation, ListMultipartUploads and GetBucketVersioning read bucket-level state
		// rather than branches and return no branch names, so they require fs:ListObjects, as an
		// object listing does.
		action = permissions.ListObjectsAction
	default:
		// Tripwire, mirroring the one in Handle. A kind added to classifyBucketGet without deciding
		// its permission here must not silently inherit fs:ListObjects: if its handler reads
		// anything branch-scoped, that is this advisory again. Fail closed instead.
		return permissions.Node{}, fmt.Errorf("%w: %d", errUnhandledBucketGetKind, kind)
	}
	return permissions.Node{
		Permission: permissions.Permission{
			Action:   action,
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

type referenceLister interface {
	ListBranches(context.Context, string, string, int, string, ...graveler.ListOptionsFunc) ([]*catalog.Branch, bool, error)
	ListTags(context.Context, string, string, int, string) ([]*catalog.Tag, bool, error)
}

// listReferences merges bounded metadata pages, without enumerating objects.
// The cursor is the last emitted ref name, shared by both ordered iterators.
func (controller *ListObjects) listReferences(ctx context.Context, c referenceLister, repo, prefix string, limit int, after string) ([]serde.CommonPrefixes, string, bool, error) {
	if limit < 0 || limit > ListObjectMaxKeys {
		limit = ListObjectMaxKeys
	}
	branches, moreBranches, err := c.ListBranches(ctx, repo, prefix, limit, after)
	if err != nil {
		return nil, "", false, err
	}
	tags, moreTags, err := c.ListTags(ctx, repo, prefix, limit, after)
	if err != nil {
		return nil, "", false, err
	}
	dirs, _ := controller.serializeBranches(branches)
	for _, tag := range tags {
		dirs = append(dirs, serde.CommonPrefixes{Prefix: path.WithRef("", tag.ID)})
	}
	// Match the catalog's ref-name ordering, excluding the directory slash.
	slices.SortFunc(dirs, func(a, b serde.CommonPrefixes) int {
		return strings.Compare(strings.TrimSuffix(a.Prefix, "/"), strings.TrimSuffix(b.Prefix, "/"))
	})
	dirs = slices.CompactFunc(dirs, func(a, b serde.CommonPrefixes) bool { return a.Prefix == b.Prefix })
	more := moreBranches || moreTags || len(dirs) > limit
	dirs = dirs[:min(len(dirs), limit)]
	last := ""
	if len(dirs) > 0 {
		last = strings.TrimSuffix(dirs[len(dirs)-1].Prefix, "/")
	}
	return dirs, last, more, nil
}

func (controller *ListObjects) ListV2(w http.ResponseWriter, req *http.Request, o *RepoOperation, prefix path.ResolvedPath) {
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

	var from path.ResolvedPath
	if !prefix.WithPath {
		// List branches and tags.
		branchPrefix := prefix.Ref // TODO: same prefix logic also in V1!!!!!
		o.Log(req).WithField("prefix", branchPrefix).Debug("listing references with prefix")
		dirs, lastKey, hasMore, err := controller.listReferences(req.Context(), o.Catalog, o.Repository.Name, branchPrefix, maxKeys, fromStr)
		if err != nil {
			o.Log(req).WithError(err).Error("could not list references")
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
			return
		}
		// Return the reference listing response.
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
		var err error
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

func (controller *ListObjects) ListV1(w http.ResponseWriter, req *http.Request, o *RepoOperation, prefix path.ResolvedPath) {
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
	if !prefix.WithPath {
		// List branches and tags.
		dirs, lastKey, hasMore, err := controller.listReferences(req.Context(), o.Catalog, o.Repository.Name, prefix.Ref, maxKeys, params.Get("marker"))
		if err != nil {
			// TODO incorrect error type
			o.Log(req).WithError(err).Error("could not list references")
			_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
			return
		}
		// Return the reference listing response.
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
		var err error
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
	kind, prefix, err := classifyBucketGet(req)
	if err != nil {
		o.Log(req).
			WithError(err).
			WithField("path", req.URL.Query().Get(QueryParamPrefix)).
			Error("could not resolve path for prefix")
		_ = o.EncodeError(w, req, err, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
		return
	}

	switch kind {
	case bucketGetLocation:
		o.Incr("get_bucket_location", o.Principal, o.Repository.Name, "")
		response := serde.LocationResponse{}
		if o.Region != "" && o.Region != defaultBucketLocation {
			response.Location = o.Region
		}
		o.EncodeResponse(w, req, response, http.StatusOK)
		return
	case bucketGetMultipartUploads:
		handleListMultipartUploads(w, req, o)
		return
	case bucketGetVersioning:
		o.EncodeXMLBytes(w, req, []byte(serde.VersioningResponse), http.StatusOK)
		return
	case bucketGetBranches, bucketGetObjects:
		// handled below
	default:
		// Tripwire. A kind added to classifyBucketGet but not handled here would fall through to
		// the listing below and list branches, while RequiredPermissions demanded only
		// fs:ListObjects -- reintroducing this advisory by omission. Fail instead.
		o.Log(req).WithField("kind", kind).Error("unhandled bucket GET kind")
		_ = o.EncodeError(w, req, nil, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}
	o.Incr("list_objects", o.Principal, o.Repository.Name, "")

	// parse request parameters
	// GET /example?list-type=2&prefix=main%2F&delimiter=%2F&encoding-type=url HTTP/1.1

	// handle ListObjects versions
	listType := req.URL.Query().Get("list-type")
	switch listType {
	case "", "1":
		controller.ListV1(w, req, o, prefix)
	case "2":
		controller.ListV2(w, req, o, prefix)
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
			o.Log(req).WithError(err).Errorf("could not read multipart record %s", *upload.UploadId)
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
