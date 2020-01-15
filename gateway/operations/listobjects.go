package operations

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"treeverse-lake/db"
	"treeverse-lake/gateway/errors"
	upath "treeverse-lake/gateway/path"
	"treeverse-lake/gateway/permissions"
	"treeverse-lake/gateway/serde"
	"treeverse-lake/index/model"
	"treeverse-lake/index/path"

	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

const (
	ListObjectMaxKeys  = 1000
	VersioningResponse = `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`
)

type ListObjects struct{}

func (controller *ListObjects) GetArn() string {
	return "arn:treeverse:repos:::{repo}"
}

func (controller *ListObjects) GetPermission() string {
	return permissions.PermissionReadRepo
}

func (controller *ListObjects) ListV2(o *RepoOperation) {
	params := o.Request.URL.Query()
	prefix := params.Get("prefix")
	delimiter := params.Get("delimiter")

	// support non-delimited list requests, but no other delimiter besides "/" is supported
	if len(delimiter) > 0 && !strings.EqualFold(delimiter, string(path.Separator)) {
		o.Log().WithField("delimiter", delimiter).Error("got unexpected delimiter")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		return
	}

	if len(delimiter) == 0 {
		p, err := upath.ResolvePath(prefix)
		if err != nil {
			// we don't have a path that properly consists of a branch
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
		// no delimiter specified
		entries, more, err := o.Index.ListObjectsByPrefix(o.Repo.GetRepoId(), p.Refspec, p.Path, "", 100000)
		if err != nil {
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			o.Log().WithError(err).WithField("prefix", prefix).Error("bad happened when ranging")
			return
		}

		files := make([]serde.Contents, 0)
		for _, f := range entries {
			files = append(files, serde.Contents{
				Key:          path.Join([]string{p.Refspec, f.GetName()}),
				LastModified: serde.Timestamp(f.GetTimestamp()),
				ETag:         serde.ETag(f.GetChecksum()),
				Size:         f.GetSize(),
				StorageClass: "STANDARD",
			})
		}

		resp := serde.ListObjectsV2Output{
			Name:      o.Repo.GetRepoId(),
			Prefix:    prefix,
			Delimiter: delimiter,
			KeyCount:  len(files),
			MaxKeys:   ListObjectMaxKeys,
			Contents:  files,
		}

		if more {
			resp.IsTruncated = true
			resp.NextContinuationToken = "do more"
		}
		o.EncodeResponse(resp, http.StatusOK)
		return
	}

	// see if we have a continuation token in the request to pick up from
	continuationToken := params.Get("continuation-token")

	prefixPath := path.New(prefix)
	prefixParts := prefixPath.SplitParts()
	var results []*model.Entry
	hasMore := false
	var err error
	if len(prefixParts) == 0 {
		// list branches then.
		results, err = o.Index.ListBranches(o.Repo.GetRepoId(), -1)
		if err != nil {
			o.Log().WithError(err).Error("could not list branches")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}
	} else {
		branch := prefixParts[0]
		parsedPath := path.Join(prefixParts[1:])
		// TODO: continuation token
		if len(continuationToken) > 0 {
			continuationTokenStr, err := base64.StdEncoding.DecodeString(continuationToken)
			if err != nil {
				// TODO incorrect error type
				o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
				return
			}
			continuationToken = string(continuationTokenStr)
		}
		results, hasMore, err = o.Index.ListObjects(o.Repo.GetRepoId(), branch, parsedPath, continuationToken, ListObjectMaxKeys)
		if xerrors.Is(err, db.ErrNotFound) {
			results = make([]*model.Entry, 0) // no results found
		} else if err != nil {
			o.Log().WithError(err).WithFields(log.Fields{
				"branch": branch,
				"path":   parsedPath,
			}).Error("could not list objects in path")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
	}

	dirs := make([]serde.CommonPrefixes, 0)
	files := make([]serde.Contents, 0)
	var lastKey string
	for _, res := range results {
		lastKey = res.GetName()
		switch res.GetType() {
		case model.Entry_TREE:
			dirs = append(dirs, serde.CommonPrefixes{Prefix: fmt.Sprintf("%s/", res.GetName())})
		case model.Entry_OBJECT:
			files = append(files, serde.Contents{
				Key:          res.GetName(),
				LastModified: serde.Timestamp(res.GetTimestamp()),
				ETag:         serde.ETag(res.GetChecksum()),
				Size:         res.GetSize(),
				StorageClass: "STANDARD",
			})
		}
	}

	resp := serde.ListObjectsV2Output{
		Name:           o.Repo.GetRepoId(),
		Prefix:         prefix,
		Delimiter:      delimiter,
		KeyCount:       len(results),
		MaxKeys:        ListObjectMaxKeys,
		CommonPrefixes: dirs,
		Contents:       files,
	}

	if hasMore {
		resp.IsTruncated = true
		resp.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(lastKey))
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
			o.EncodeXMLBytes([]byte(VersioningResponse), http.StatusOK)
			return
		}
	}

	// handle ListObjectsV2
	if strings.EqualFold(o.Request.URL.Query().Get("list-type"), "2") {
		controller.ListV2(o)
		return
	}

	// handle ListObjects (v1)
	params := o.Request.URL.Query()
	prefix := params.Get("prefix")
	delimiter := params.Get("delimiter")

	if len(delimiter) != 1 || delimiter[0] != path.Separator {
		// we only support "/" as a delimiter
		delimiter = "/"
		//o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		//return
	}

	// see if we have a continuation token in the request to pick up from
	marker := params.Get("marker")

	prefixPath := path.New(prefix)
	prefixParts := prefixPath.SplitParts()
	var results []*model.Entry
	hasMore := false
	var err error
	if len(prefixParts) == 0 {
		// list branches then.
		results, err = o.Index.ListBranches(o.Repo.GetRepoId(), -1)
		if err != nil {
			// TODO incorrect error type
			o.Log().WithError(err).Error("could not list branches")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
	} else {
		branch := prefixParts[0]
		parsedPath := path.Join(prefixParts[1:])
		results, hasMore, err = o.Index.ListObjects(o.Repo.GetRepoId(), branch, parsedPath, marker, ListObjectMaxKeys)
		if xerrors.Is(err, db.ErrNotFound) {
			results = make([]*model.Entry, 0) // no results found
		} else if err != nil {
			o.Log().WithError(err).WithFields(log.Fields{
				"branch": branch,
				"path":   parsedPath,
			}).Error("could not list objects in path")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
	}

	dirs := make([]serde.CommonPrefixes, 0)
	files := make([]serde.Contents, 0)
	var lastKey string
	for _, res := range results {
		lastKey = res.GetName()
		switch res.GetType() {
		case model.Entry_TREE:
			dirs = append(dirs, serde.CommonPrefixes{Prefix: fmt.Sprintf("%s/", res.GetName())})
		case model.Entry_OBJECT:
			files = append(files, serde.Contents{
				Key:          res.GetName(),
				LastModified: serde.Timestamp(res.GetTimestamp()),
				ETag:         fmt.Sprintf("\"%s\"", res.GetChecksum()),
				Size:         res.GetSize(),
				StorageClass: "STANDARD",
			})
		}
	}

	resp := serde.ListBucketResult{
		Name:           o.Repo.GetRepoId(),
		Prefix:         prefix,
		Delimiter:      delimiter,
		Marker:         marker,
		KeyCount:       len(results),
		MaxKeys:        ListObjectMaxKeys,
		CommonPrefixes: dirs,
		Contents:       files,
	}

	if hasMore {
		resp.IsTruncated = true
		resp.NextMarker = lastKey
	}

	o.EncodeResponse(resp, http.StatusOK)
}
