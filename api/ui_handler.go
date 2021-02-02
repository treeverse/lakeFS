package api

import (
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/rakyll/statik/fs"
	"github.com/treeverse/lakefs/auth"
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
