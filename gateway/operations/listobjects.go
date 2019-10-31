package operations

import (
	"fmt"
	"net/http"
	"strings"
	"time"
	"versio-index/gateway/errors"
	"versio-index/gateway/permissions"
	"versio-index/gateway/serde"
	"versio-index/index/model"
	"versio-index/index/path"
)

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
	params := o.Request.URL.Query()
	prefix := params.Get("prefix")
	delimiter := params.Get("delimiter")
	if !strings.EqualFold(delimiter, "/") {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		return
	}

	// parse delimiter (everything other than "/" is illegal)
	prefixPath := path.New(prefix)
	prefixParts := prefixPath.SplitParts()
	if len(prefixParts) == 0 {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		return
	}
	branch := prefixParts[0]
	parsedPath := path.Join(prefixParts[1:])

	// TODO: continuation token
	results, err := o.Index.ListObjects(o.ClientId, o.Repo, branch, parsedPath, "", 1000)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
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
				LastModified: serde.Timestamp(time.Now().Unix()),
				ETag:         fmt.Sprintf("\"%s\"", res.GetAddress()),
				Size:         1000,
				StorageClass: "STANDARD",
			})
		}
	}

	// build response
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
