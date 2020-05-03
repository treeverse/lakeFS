package api

import (
	"net/http"
	"os"
	"path"
	"strings"
)

type NonAuthenticatingResponseHeader http.Header

type NonAuthenticatingResponseWriter struct {
	w http.ResponseWriter
}

func (n NonAuthenticatingResponseWriter) Header() http.Header {
	return n.w.Header()
}

func (n NonAuthenticatingResponseWriter) Write(b []byte) (int, error) {
	return n.w.Write(b)
}

func (n NonAuthenticatingResponseWriter) WriteHeader(statusCode int) {
	n.w.Header().Del("WWW-Authenticate")
	n.w.WriteHeader(statusCode)
}

func HandlerWithDefault(root http.FileSystem, handler http.Handler, defaultPath string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upath := r.URL.Path
		if !strings.HasPrefix(upath, "/") {
			upath = "/" + upath
			r.URL.Path = upath
		}
		_, err := root.Open(path.Clean(upath))
		if err != nil && os.IsNotExist(err) {
			r.URL.Path = defaultPath
		}
		handler.ServeHTTP(w, r)
	})
}
