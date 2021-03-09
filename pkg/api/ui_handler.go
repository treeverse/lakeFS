package api

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/rakyll/statik/fs"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/operations"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/pkg/webui"
)

func NewUIHandler(authService auth.Service, gatewayDomain string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/auth/login", NewLoginHandler(authService))
	mux.Handle("/auth/logout", NewLogoutHandler())
	staticFiles, _ := fs.NewWithNamespace(webui.Webui)
	mux.Handle("/", NewHandlerWithDefault(staticFiles, http.FileServer(staticFiles), "/", gatewayDomain))
	return mux
}

func NewHandlerWithDefault(root http.FileSystem, handler http.Handler, defaultPath, gatewayDomain string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isGatewayRequest(r) {
			// s3 signed request reaching the ui handler, return an error response instead of the default path
			o := operations.Operation{}
			err := errors.Codes[errors.ERRLakeFSWrongEndpoint]
			err.Description = fmt.Sprintf("%s (%s)", err.Description, gatewayDomain)
			o.EncodeError(w, r, err)
			return
		}
		urlPath := r.URL.Path
		if !strings.HasPrefix(urlPath, "/") {
			urlPath = "/" + urlPath
			r.URL.Path = urlPath
		}
		_, err := root.Open(path.Clean(urlPath))
		if err != nil && os.IsNotExist(err) {
			r.URL.Path = defaultPath
		}
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
