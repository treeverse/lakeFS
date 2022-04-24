package api

import (
	"errors"
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

func NewUIHandler(gatewayDomains []string) http.Handler {
	content, err := fs.Sub(webui.Content, "dist")
	if err != nil {
		// embedded UI content is missing
		panic(err)
	}
	fileSystem := http.FS(content)
	nocacheContent := middleware.NoCache(http.StripPrefix("/", http.FileServer(fileSystem)))
	return NewHandlerWithDefault(fileSystem, nocacheContent, gatewayDomains)
}

func NewHandlerWithDefault(fileSystem http.FileSystem, handler http.Handler, gatewayDomains []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isGatewayRequest(r) {
			// s3 signed request reaching the ui handler, return an error response instead of the default path
			err := gwerrors.Codes[gwerrors.ERRLakeFSWrongEndpoint]
			err.Description = fmt.Sprintf("%s (%v)", err.Description, gatewayDomains)
			o := operations.Operation{}
			o.EncodeError(w, r, err)
			return
		}

		// serve root content in case of file not found
		// the client side react browser router handles the rendering
		_, err := fileSystem.Open(r.URL.Path)
		if errors.Is(err, fs.ErrNotExist) {
			r.URL.Path = "/"
		}

		// consistent content-type
		contentType := gomime.TypeByExtension(filepath.Ext(r.URL.Path))
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}

		// handle request, capture page not found for redirect later
		handler.ServeHTTP(w, r)
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
