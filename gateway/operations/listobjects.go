package operations

import (
	"fmt"
	"net/http"
	"strings"
	"versio-index/db"
	"versio-index/gateway/errors"
	"versio-index/gateway/permissions"
	"versio-index/gateway/serde"
	"versio-index/index/model"
	"versio-index/index/path"

	"golang.org/x/xerrors"
)

const VersioningResponse = `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`

type ListObjects struct{}

func (controller *ListObjects) GetArn() string {
	return "arn:versio:repos:::{bucket}"
}

func (controller *ListObjects) GetPermission() string {
	return permissions.PermissionReadRepo
}

func (controller *ListObjects) Handle(o *RepoOperation) {
	// parse request parameters
	// GET /example?list-type=2&prefix=master%2F&delimiter=%2F&encoding-type=url HTTP/1.1

	// handle GET /?versioning
	keys := o.Request.URL.Query()
	for k, _ := range keys {
		if strings.EqualFold(k, "versioning") {
			// this is a versioning request
			o.EncodeXMLBytes([]byte(VersioningResponse), http.StatusOK)
			return
		}
	}

	params := o.Request.URL.Query()
	prefix := params.Get("prefix")
	delimiter := params.Get("delimiter")

	// parse delimiter (everything other than "/" is illegal)
	prefixPath := path.New(prefix)
	prefixParts := prefixPath.SplitParts()
	var results []*model.Entry
	var err error
	if len(prefixParts) == 0 {
		// list branches then.
		results, err = o.Index.ListBranches(o.ClientId, o.Repo, -1)
		if err != nil {
			// TODO incorrect error type
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
	} else {
		branch := prefixParts[0]
		parsedPath := path.Join(prefixParts[1:])
		// TODO: continuation token
		results, err = o.Index.ListObjects(o.ClientId, o.Repo, branch, parsedPath, "", -1)
		if xerrors.Is(err, db.ErrNotFound) {
			results = make([]*model.Entry, 0) // no results found
		} else if err != nil {
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
			return
		}
	}

	dirs := make([]serde.CommonPrefixes, 0)
	files := make([]serde.Contents, 0)
	for _, res := range results {
		switch res.GetType() {
		case model.Entry_TREE:
			dirs = append(dirs, serde.CommonPrefixes{Prefix: fmt.Sprintf("%s/", res.GetName())})
		case model.Entry_OBJECT:
			files = append(files, serde.Contents{
				Key:          res.GetName(),
				LastModified: serde.Timestamp(res.GetTimestamp()),
				ETag:         fmt.Sprintf("\"%s\"", res.GetAddress()),
				Size:         res.GetSize(),
				StorageClass: "STANDARD",
			})
		}
	}

	o.EncodeResponse(serde.ListObjectsV2Output{
		Name:           o.Repo,
		IsTruncated:    false,
		Prefix:         prefix,
		Delimiter:      delimiter,
		KeyCount:       len(results),
		MaxKeys:        1000,
		CommonPrefixes: dirs,
		Contents:       files,
	}, http.StatusOK)
}
