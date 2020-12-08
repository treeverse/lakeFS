package operations

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	ghttp "github.com/treeverse/lakefs/gateway/http"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/permissions"
)

type GetObject struct{}

func (controller *GetObject) RequiredPermissions(_ *http.Request, repoID, _, path string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn(repoID, path),
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
	entry, err := o.Cataloger.GetEntry(o.Context(), o.Repository.Name, o.Reference, o.Path, catalog.GetEntryParams{})
	metaTook := time.Since(beforeMeta)
	o.Log().
		WithField("took", metaTook).
		WithError(err).
		Debug("metadata operation to retrieve object done")

	if errors.Is(err, db.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchKey))
		return
	}
	if errors.Is(err, catalog.ErrExpired) {
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchVersion))
	}
	if err != nil {
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}

	o.SetHeader("Last-Modified", httputil.HeaderTimestamp(entry.CreationDate))
	o.SetHeader("ETag", httputil.ETag(entry.Checksum))
	o.SetHeader("Accept-Ranges", "bytes")
	// TODO: the rest of https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/API_GetObject.html

	// range query
	var expected int64
	var data io.ReadCloser
	var rng ghttp.Range
	rng.StartOffset = -1
	// range query
	rangeSpec := o.Request.Header.Get("Range")
	if len(rangeSpec) > 0 {
		rng, err = ghttp.ParseRange(rangeSpec, entry.Size)
		if err != nil {
			o.Log().WithError(err).WithField("range", rangeSpec).Debug("invalid range spec")
		}
	}
	if rangeSpec == "" || err != nil {
		// assemble a response body (range-less query)
		expected = entry.Size
		data, err = o.BlockStore.Get(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: entry.PhysicalAddress}, entry.Size)
	} else {
		expected = rng.EndOffset - rng.StartOffset + 1 // both range ends are inclusive
		data, err = o.BlockStore.GetRange(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: entry.PhysicalAddress}, rng.StartOffset, rng.EndOffset)
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
		o.SetHeader("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.StartOffset, rng.EndOffset, entry.Size))
	}
	// Delete the default content-type header so http.Server will detect it from contents
	// TODO(ariels): After/if we add content-type support to adapter, use *that*.
	o.DeleteHeader("Content-Type")
	_, err = io.Copy(o.ResponseWriter, data)
	if err != nil {
		o.Log().WithError(err).Error("could not write response body for object")
	}
}
