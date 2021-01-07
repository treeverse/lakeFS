package operations

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
)

const (
	ListObjectMaxKeys = 1000
)

type ListObjects struct{}

func (controller *ListObjects) RequiredPermissions(req *http.Request, repoID string) ([]permissions.Permission, error) {
	// check if we're listing files in a branch, or listing branches
	params := req.URL.Query()
	delimiter := params.Get("delimiter")
	prefix := params.Get("prefix")
	if delimiter == "/" && !strings.Contains(prefix, "/") {
		return []permissions.Permission{
			{
				Action:   permissions.ListBranchesAction,
				Resource: permissions.RepoArn(repoID),
			},
		}, nil
	}

	// otherwise, we're listing objects within a branch
	return []permissions.Permission{
		{
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

func (controller *ListObjects) serializeEntries(ref string, entries []*catalog.Entry) ([]serde.CommonPrefixes, []serde.Contents, string) {
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
	o.AddLogFields(req, logging.Fields{
		"list_type": "v2",
	})
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

	// see if this is a recursive call`
	if len(delimiter) >= 1 {
		if delimiter != path.Separator {
			// we only support "/" as a delimiter
			o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
			return
		}
	}

	var results []*catalog.Entry
	var hasMore bool
	var ref string
	// should we list branches?
	prefix, err := path.ResolvePath(params.Get("prefix"))
	if err != nil {
		o.Log(req).
			WithError(err).
			WithField("path", params.Get("prefix")).
			Error("could not resolve path for prefix")
		o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
		return
	}

	var from path.ResolvedPath
	if !prefix.WithPath {
		// list branches then.
		branchPrefix := prefix.Ref // TODO: same prefix logic also in V1!!!!!
		o.Log(req).WithField("prefix", branchPrefix).Debug("listing branches with prefix")
		branches, hasMore, err := o.Cataloger.ListBranches(o.Context(req), o.Repository.Name, branchPrefix, maxKeys, fromStr)
		if err != nil {
			o.Log(req).WithError(err).Error("could not list branches")
			o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
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
		// list branches then.
		ref = prefix.Ref
		if len(fromStr) > 0 {
			from, err = path.ResolvePath(fromStr)
			if err != nil || !strings.EqualFold(from.Ref, prefix.Ref) {
				o.Log(req).WithError(err).WithFields(logging.Fields{
					"branch": prefix.Ref,
					"path":   prefix.Path,
					"from":   fromStr,
				}).Error("invalid marker - doesnt start with branch name")
				o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
				return
			}
		}

		results, hasMore, err = o.Cataloger.ListEntries(
			o.Context(req),
			o.Repository.Name,
			prefix.Ref,
			prefix.Path,
			from.Path,
			delimiter,
			maxKeys,
		)
		if errors.Is(err, catalog.ErrBranchNotFound) {
			o.Log(req).WithError(err).WithFields(logging.Fields{
				"ref":  prefix.Ref,
				"path": prefix.Path,
			}).Debug("could not list objects in path")
		} else if err != nil {
			o.Log(req).WithError(err).WithFields(logging.Fields{
				"ref":  prefix.Ref,
				"path": prefix.Path,
			}).Error("could not list objects in path")
			o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
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
	o.AddLogFields(req, logging.Fields{
		"list_type": "v1",
	})

	// handle ListObjects (v1)
	params := req.URL.Query()
	delimiter := params.Get("delimiter")
	descend := true
	if len(delimiter) >= 1 {
		if delimiter != path.Separator {
			// we only support "/" as a delimiter
			o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
			return
		}
		descend = false
	}

	maxKeys := controller.getMaxKeys(req, o)

	var results []*catalog.Entry
	hasMore := false

	var ref string
	// should we list branches?
	prefix, err := path.ResolvePath(params.Get("prefix"))
	if err != nil {
		o.Log(req).
			WithError(err).
			WithField("path", params.Get("prefix")).
			Error("could not resolve path for prefix")
		o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
		return
	}

	if !prefix.WithPath {
		// list branches then.
		branches, hasMore, err := o.Cataloger.ListBranches(o.Context(req), o.Repository.Name, prefix.Ref, maxKeys, params.Get("marker"))
		if err != nil {
			// TODO incorrect error type
			o.Log(req).WithError(err).Error("could not list branches")
			o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
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
			o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
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
				o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
				return
			}
		}
		results, hasMore, err = o.Cataloger.ListEntries(
			o.Context(req),
			o.Repository.Name,
			prefix.Ref,
			prefix.Path,
			marker.Path,
			delimiter,
			maxKeys,
		)
		if errors.Is(err, db.ErrNotFound) {
			results = make([]*catalog.Entry, 0) // no results found
		} else if err != nil {
			o.Log(req).WithError(err).WithFields(logging.Fields{
				"branch": prefix.Ref,
				"path":   prefix.Path,
			}).Error("could not list objects in path")
			o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
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
	o.Incr("list_objects")
	// parse request parameters
	// GET /example?list-type=2&prefix=master%2F&delimiter=%2F&encoding-type=url HTTP/1.1

	// handle GET /?versioning
	query := req.URL.Query()
	if _, found := query["versioning"]; found {
		o.EncodeXMLBytes(w, req, []byte(serde.VersioningResponse), http.StatusOK)
		return
	}

	// handle ListObjects versions
	listType := query.Get("list-type")
	switch listType {
	case "", "1":
		controller.ListV1(w, req, o)
	case "2":
		controller.ListV2(w, req, o)
	default:
		o.Log(req).WithField("list-type", listType).Error("listObjects version not supported")
		o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrBadRequest))
	}
}
