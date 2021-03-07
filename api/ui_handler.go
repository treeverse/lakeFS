package api

import (
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/rakyll/statik/fs"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/sig"
	"github.com/treeverse/lakefs/statik"
)

func NewUIHandler(authService auth.Service) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/auth/login", NewLoginHandler(authService))
	mux.Handle("/auth/logout", NewLogoutHandler())
	staticFiles, _ := fs.NewWithNamespace(statik.Webui)
	mux.Handle("/", NewHandlerWithDefault(staticFiles, http.FileServer(staticFiles), "/"))
	return mux
}

func NewHandlerWithDefault(root http.FileSystem, handler http.Handler, defaultPath string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s3auth := r.Header.Get(sig.V4authHeaderName); strings.HasPrefix(s3auth, sig.V4authHeaderPrefix) {
			// s3 signed request reaching the ui handler, return an error response instead of the default path
			o := operations.Operation{}
			o.EncodeError(w, r, errors.ERRLakeFSWrongEndpoint.ToAPIErr())
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
