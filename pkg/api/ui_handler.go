package api

import (
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"

	gomime "github.com/cubewise-code/go-mime"
	"github.com/go-chi/chi/v5/middleware"
	gwerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/operations"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/webui"
)

// NotFoundResponseWriter handle page not found to redirect later the response
// source: https://stackoverflow.com/questions/47285119/how-to-custom-handle-a-file-not-being-found-when-using-go-static-file-server
type NotFoundResponseWriter struct {
	http.ResponseWriter
	Status int
}

func (w *NotFoundResponseWriter) WriteHeader(status int) {
	w.Status = status
	if status != http.StatusNotFound {
		w.ResponseWriter.WriteHeader(status)
	}
}

func (w *NotFoundResponseWriter) Write(p []byte) (int, error) {
	if w.Status != http.StatusNotFound {
		return w.ResponseWriter.Write(p)
	}
	// Lie that we have successfully written it
	return len(p), nil
}

func NewUIHandler(gatewayDomains []string) http.Handler {
	content, err := fs.Sub(webui.Content, "dist")
	if err != nil {
		// embedded UI content is missing
		panic(err)
	}
	nocacheContent := middleware.NoCache(
		http.StripPrefix("/",
			http.FileServer(
				http.FS(content))))
	return NewHandlerWithDefault(nocacheContent, gatewayDomains)
}

func NewHandlerWithDefault(handler http.Handler, gatewayDomains []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isGatewayRequest(r) {
			// s3 signed request reaching the ui handler, return an error response instead of the default path
			err := gwerrors.Codes[gwerrors.ERRLakeFSWrongEndpoint]
			err.Description = fmt.Sprintf("%s (%v)", err.Description, gatewayDomains)
			o := operations.Operation{}
			o.EncodeError(w, r, err)
			return
		}

		// consistent content-type
		contentType := gomime.TypeByExtension(filepath.Ext(r.URL.Path))
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}

		// handle request, capture page not found for redirect later
		notFoundWriter := &NotFoundResponseWriter{ResponseWriter: w}
		handler.ServeHTTP(notFoundWriter, r)

		// redirect to root on page not found
		if notFoundWriter.Status == http.StatusNotFound {
			http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		}
	})
}

func isGatewayRequest(r *http.Request) bool {
	// v4 and v2 header key are equal
	vals := r.Header.Values(sig.V4authHeaderName)
	for _, v := range vals {
		if strings.HasPrefix(v, sig.V4authHeaderPrefix) {
			return true
		}
		if sig.V2AuthHeaderRegexp.MatchString(v) {
			return true
		}
	}
	return false
}
