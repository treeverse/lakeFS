package auth

import (
	"context"
	"net/http"

	"github.com/treeverse/lakefs/pkg/httputil"
)

// AddRequestID returns a RequestEditorFn that puts the RequestID from the
// context logging field on every client request.
func AddRequestID(headerName string) RequestEditorFn {
	return func(ctx context.Context, req *http.Request) error {
		reqIDField := ctx.Value(httputil.RequestIDContextKey)
		if reqIDField == nil {
			return nil
		}
		reqID := reqIDField.(string)
		req.Header.Add(headerName, reqID)
		return nil
	}
}
