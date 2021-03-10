package operations

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/db"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	ghttp "github.com/treeverse/lakefs/pkg/gateway/http"
	"github.com/treeverse/lakefs/pkg/gateway/serde"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/permissions"
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

func (controller *GetObject) Handle(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("get_object")
	query := req.URL.Query()
	if _, exists := query["versioning"]; exists {
		o.EncodeResponse(w, req, serde.VersioningConfiguration{}, http.StatusOK)
		return
	}

	if _, exists := query["tagging"]; exists {
		o.EncodeResponse(w, req, serde.Tagging{}, http.StatusOK)
		return
	}

	beforeMeta := time.Now()
	entry, err := o.Catalog.GetEntry(req.Context(), o.Repository.Name, o.Reference, o.Path, catalog.GetEntryParams{})
	metaTook := time.Since(beforeMeta)
	o.Log(req).
		WithField("took", metaTook).
		WithError(err).
		Debug("metadata operation to retrieve object done")

	if errors.Is(err, db.ErrNotFound) {
		// TODO: create distinction between missing repo & missing key
		_ = o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchKey))
		return
	}
	if errors.Is(err, catalog.ErrExpired) {
		_ = o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchVersion))
	}
	if err != nil {
		_ = o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}

	o.SetHeader(w, "Last-Modified", httputil.HeaderTimestamp(entry.CreationDate))
	o.SetHeader(w, "ETag", httputil.ETag(entry.Checksum))
	o.SetHeader(w, "Accept-Ranges", "bytes")
	// TODO: the rest of https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/API_GetObject.html

	// range query
	var expected int64
	var data io.ReadCloser
	var rng ghttp.Range
	// range query
	rangeSpec := req.Header.Get("Range")
	if len(rangeSpec) > 0 {
		rng, err = ghttp.ParseRange(rangeSpec, entry.Size)
		if err != nil {
			o.Log(req).WithError(err).WithField("range", rangeSpec).Debug("invalid range spec")
		}
	}
	if rangeSpec == "" || err != nil {
		// assemble a response body (range-less query)
		expected = entry.Size
		data, err = o.BlockStore.Get(req.Context(), block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: entry.PhysicalAddress}, entry.Size)
	} else {
		expected = rng.EndOffset - rng.StartOffset + 1 // both range ends are inclusive
		data, err = o.BlockStore.GetRange(req.Context(), block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: entry.PhysicalAddress}, rng.StartOffset, rng.EndOffset)
		o.SetHeader(w, "Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.StartOffset, rng.EndOffset, entry.Size))
	}
	if err != nil {
		_ = o.EncodeError(w, req, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return
	}
	defer func() {
		_ = data.Close()
	}()
	o.SetHeader(w, "Content-Length", fmt.Sprintf("%d", expected))
	// Delete the default content-type header so http.Server will detect it from contents
	// TODO(ariels): After/if we add content-type support to adapter, use *that*.
	o.DeleteHeader(w, "Content-Type")
	_, err = io.Copy(w, data)
	if err != nil {
		o.Log(req).WithError(err).Error("could not write response body for object")
	}
}
