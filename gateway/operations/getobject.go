package operations

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/httputil"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	ghttp "github.com/treeverse/lakefs/gateway/http"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/permissions"

	"golang.org/x/xerrors"
)

type GetObject struct{}

func (controller *GetObject) RequiredPermissions(request *http.Request, repoId, branchId, path string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repoId, path),
		},
	}, nil
}

func (controller *GetObject) Handle(o *PathOperation) {
	o.Incr("get_object")
	query := o.Request.URL.Query()
	if _, exists := query["versioning"]; exists {
		o.EncodeResponse(serde.VersioningConfiguration{}, http.StatusOK)
		return
	}

	if _, exists := query["tagging"]; exists {
		o.EncodeResponse(serde.Tagging{}, http.StatusOK)
		return
	}

	beforeMeta := time.Now()
	entry, err := o.Index.ReadEntryObject(o.Repo.Id, o.Ref, o.Path, true)
	metaTook := time.Since(beforeMeta)
	o.Log().
		WithField("took", metaTook).
		WithError(err).
		Debug("metadata operation to retrieve object done")

	if xerrors.Is(err, db.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchKey))
		return
	}
	if err != nil {
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}

	o.SetHeader("Last-Modified", httputil.HeaderTimestamp(entry.CreationDate))
	o.SetHeader("ETag", httputil.ETag(entry.Checksum))
	o.SetHeader("Accept-Ranges", "bytes")
	// TODO: the rest of https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/API_GetObject.html

	// now we might need the object itself
	obj, err := o.Index.ReadObject(o.Repo.Id, o.Ref, o.Path, true)
	if err != nil {
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}

	// range query
	var expected int64
	var data io.ReadCloser
	var rng ghttp.HttpRange
	rng.StartOffset = -1
	// range query
	rangeSpec := o.Request.Header.Get("Range")
	if len(rangeSpec) > 0 {
		rng, err = ghttp.ParseHTTPRange(rangeSpec, obj.Size)
		if err != nil {
			o.Log().WithError(err).WithField("range", rangeSpec).Debug("invalid range spec")
		}
	}
	if rangeSpec == "" || err != nil {
		// assemble a response body (range-less query)
		expected = obj.Size
		data, err = o.BlockStore.Get(block.ObjectPointer{Repo: o.Repo.StorageNamespace, Identifier: obj.PhysicalAddress})
	} else {
		expected = rng.EndOffset - rng.StartOffset + 1 // both range ends are inclusive
		data, err = o.BlockStore.GetRange(block.ObjectPointer{Repo: o.Repo.StorageNamespace, Identifier: obj.PhysicalAddress}, rng.StartOffset, rng.EndOffset)
	}
	if err != nil {
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}
	defer func() {
		_ = data.Close()
	}()
	o.SetHeader("Content-Length", fmt.Sprintf("%d", expected))
	if rng.StartOffset != -1 {
		o.SetHeader("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.StartOffset, rng.EndOffset, obj.Size))
	}
	_, err = io.Copy(o.ResponseWriter, data)
	if err != nil {
		o.Log().WithError(err).Error("could not write response body for object")
	}
}
