package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/pkg/catalog"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/httputil"
)

// applyGetPreconditions writes a 304 or 412 response when GetObject/HeadObject
// conditional headers fail. It returns true when the caller should stop.
func (o *PathOperation) applyGetPreconditions(w http.ResponseWriter, req *http.Request, entry *catalog.DBEntry) bool {
	etag := httputil.ETag(entry.Checksum)
	switch httputil.CheckGetPreconditions(req.Header, etag, entry.CreationDate) {
	case httputil.PreconditionOK:
		return false
	case httputil.PreconditionNotModified:
		o.SetHeader(w, "ETag", etag)
		o.SetHeader(w, "Last-Modified", httputil.HeaderTimestamp(entry.CreationDate))
		w.WriteHeader(http.StatusNotModified)
		return true
	case httputil.PreconditionFailed:
		_ = o.EncodeError(w, req, nil, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrPreconditionFailed))
		return true
	default:
		_ = o.EncodeError(w, req, nil, gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return true
	}
}
