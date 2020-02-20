package operations

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/treeverse/lakefs/gateway/utils"
	"github.com/treeverse/lakefs/httputil"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/gateway/serde"

	"github.com/treeverse/lakefs/index/model"

	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

const (
	ListObjectMaxKeys = 1000
)

type ListObjects struct{}

func (controller *ListObjects) Action(req *http.Request) permissions.Action {
	return permissions.ListObjects(utils.GetRepo(req))
}

func (controller *ListObjects) getMaxKeys(o *RepoOperation) int {
	params := o.Request.URL.Query()
	maxKeys := ListObjectMaxKeys
	if len(params.Get("max-keys")) > 0 {
		parsedKeys, err := strconv.ParseInt(params.Get("max-keys"), 10, 64)
		if err == nil {
			maxKeys = int(parsedKeys)
		}
	}
	return maxKeys
}

func (controller *ListObjects) serializeEntries(refspec string, entries []*model.Entry) ([]serde.CommonPrefixes, []serde.Contents, string) {
	dirs := make([]serde.CommonPrefixes, 0)
	files := make([]serde.Contents, 0)
	var lastKey string
	for _, entry := range entries {
		lastKey = entry.GetName()
		switch entry.GetType() {
		case model.Entry_TREE:
			dirs = append(dirs, serde.CommonPrefixes{Prefix: path.WithRefspec(entry.GetName(), refspec)})
		case model.Entry_OBJECT:
			files = append(files, serde.Contents{
				Key:          path.WithRefspec(entry.GetName(), refspec),
				LastModified: serde.Timestamp(entry.GetTimestamp()),
				ETag:         httputil.ETag(entry.GetChecksum()),
				Size:         entry.GetSize(),
				StorageClass: "STANDARD",
			})
		}
	}
	return dirs, files, lastKey
}

func (controller *ListObjects) serializeBranches(branches []*model.Branch) ([]serde.CommonPrefixes, string) {
	dirs := make([]serde.CommonPrefixes, 0)
	var lastKey string
	for _, branch := range branches {
		lastKey = branch.GetName()
		dirs = append(dirs, serde.CommonPrefixes{Prefix: path.WithRefspec("", branch.GetName())})
	}
	return dirs, lastKey
}

func (controller *ListObjects) ListV2(o *RepoOperation) {
	params := o.Request.URL.Query()
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

	var from path.ResolvedPath

	maxKeys := controller.getMaxKeys(o)

	// see if this is a recursive call`
	descend := true
	if len(delimiter) >= 1 {
		if delimiter != path.Separator {
			// we only support "/" as a delimiter
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
		descend = false
	}

	var results []*model.Entry
	hasMore := false

	var refspec string

	// should we list branches?
	prefix, err := path.ResolvePath(params.Get("prefix"))
	if err != nil {
		o.Log().
			WithError(err).
			WithField("path", params.Get("prefix")).
			Error("could not resolve path for prefix")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		return
	}

	if !prefix.WithPath {
		// list branches then.
		branchPrefix := prefix.Refspec // TODO: same prefix logic also in V1!!!!!
		o.Log().WithField("prefix", branchPrefix).Debug("listing branches with prefix")
		branches, hasMore, err := o.Index.ListBranchesByPrefix(o.Repo.GetRepoId(), branchPrefix, maxKeys, fromStr)
		if err != nil {
			o.Log().WithError(err).Error("could not list branches")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}
		// return branch response
		dirs, lastKey := controller.serializeBranches(branches)
		resp := serde.ListObjectsV2Output{
			Name:           o.Repo.GetRepoId(),
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

		o.EncodeResponse(resp, http.StatusOK)
		return

	} else {
		refspec = prefix.Refspec
		if len(fromStr) > 0 {
			from, err = path.ResolvePath(fromStr)
			if err != nil || !strings.EqualFold(from.Refspec, prefix.Refspec) {
				o.Log().WithError(err).WithFields(log.Fields{
					"branch": prefix.Refspec,
					"path":   prefix.Path,
					"from":   fromStr,
				}).Error("invalid marker - doesnt start with branch name")
				o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
				return
			}
		}

		results, hasMore, err = o.Index.ListObjectsByPrefix(
			o.Repo.GetRepoId(),
			prefix.Refspec,
			prefix.Path,
			from.Path,
			maxKeys,
			descend)
		if xerrors.Is(err, db.ErrNotFound) {
			results = make([]*model.Entry, 0) // no results found
		} else if err != nil {
			o.Log().WithError(err).WithFields(log.Fields{
				"refspec": prefix.Refspec,
				"path":    prefix.Path,
			}).Error("could not list objects in path")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
	}

	dirs, files, lastKey := controller.serializeEntries(refspec, results)

	resp := serde.ListObjectsV2Output{
		Name:           o.Repo.GetRepoId(),
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
		resp.NextContinuationToken = path.WithRefspec(lastKey, refspec)
	}

	o.EncodeResponse(resp, http.StatusOK)
}

func (controller *ListObjects) ListV1(o *RepoOperation) {
	// handle ListObjects (v1)
	params := o.Request.URL.Query()
	delimiter := params.Get("delimiter")
	descend := true

	if len(delimiter) >= 1 {
		if delimiter != path.Separator {
			// we only support "/" as a delimiter
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
		descend = false
	}

	maxKeys := controller.getMaxKeys(o)

	var results []*model.Entry
	hasMore := false

	var refspec string
	// should we list branches?
	prefix, err := path.ResolvePath(params.Get("prefix"))
	if err != nil {
		o.Log().
			WithError(err).
			WithField("path", params.Get("prefix")).
			Error("could not resolve path for prefix")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		return
	}

	if !prefix.WithPath {
		// list branches then.
		branches, hasMore, err := o.Index.ListBranchesByPrefix(o.Repo.GetRepoId(), prefix.Refspec, maxKeys, params.Get("marker"))
		if err != nil {
			// TODO incorrect error type
			o.Log().WithError(err).Error("could not list branches")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
		// return branch response
		dirs, lastKey := controller.serializeBranches(branches)
		resp := serde.ListBucketResult{
			Name:           o.Repo.GetRepoId(),
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

		o.EncodeResponse(resp, http.StatusOK)
		return
	} else {
		prefix, err := path.ResolvePath(params.Get("prefix"))
		if err != nil {
			o.Log().WithError(err).Error("could not list branches")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
		refspec = prefix.Refspec
		// see if we have a continuation token in the request to pick up from
		var marker path.ResolvedPath
		// strip the branch from the marker
		if len(params.Get("marker")) > 0 {
			marker, err = path.ResolvePath(params.Get("marker"))
			if err != nil || !strings.EqualFold(marker.Refspec, prefix.Refspec) {
				o.Log().WithError(err).WithFields(log.Fields{
					"branch": prefix.Refspec,
					"path":   prefix.Path,
					"marker": marker,
				}).Error("invalid marker - doesnt start with branch name")
				o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
				return
			}
		}

		results, hasMore, err = o.Index.ListObjectsByPrefix(
			o.Repo.GetRepoId(),
			prefix.Refspec,
			prefix.Path,
			marker.Path,
			maxKeys,
			descend,
		)
		if xerrors.Is(err, db.ErrNotFound) {
			results = make([]*model.Entry, 0) // no results found
		} else if err != nil {
			o.Log().WithError(err).WithFields(log.Fields{
				"branch": prefix.Refspec,
				"path":   prefix.Path,
			}).Error("could not list objects in path")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
	}

	// build a response
	dirs, files, lastKey := controller.serializeEntries(refspec, results)
	resp := serde.ListBucketResult{
		Name:           o.Repo.GetRepoId(),
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
			resp.NextMarker = path.WithRefspec(lastKey, refspec)
		}
	}

	o.EncodeResponse(resp, http.StatusOK)
}

func (controller *ListObjects) Handle(o *RepoOperation) {
	// parse request parameters
	// GET /example?list-type=2&prefix=master%2F&delimiter=%2F&encoding-type=url HTTP/1.1

	// handle GET /?versioning
	keys := o.Request.URL.Query()
	for k := range keys {
		if strings.EqualFold(k, "versioning") {
			// this is a versioning request
			o.EncodeXMLBytes([]byte(serde.VersioningResponse), http.StatusOK)
			return
		}
	}

	// handle ListObjects versions
	listType := o.Request.URL.Query().Get("list-type")
	if strings.EqualFold(listType, "2") {
		controller.ListV2(o)
	} else if strings.EqualFold(listType, "1") {
		controller.ListV1(o)
	} else if len(listType) > 0 {
		o.Log().WithField("list-type", listType).Error("listObjects version not supported")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		return
	} else {
		// otherwise, handle ListObjectsV1
		controller.ListV1(o)
	}

}
