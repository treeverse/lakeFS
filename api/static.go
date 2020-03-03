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

func HandlerWithUI(api http.Handler, ui http.Handler) http.Handler {
	mux := http.NewServeMux()

	// /api/ and /swagger.json go to the api handler
	mux.Handle("/api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("User-Agent"), "Mozilla") {
			// this is a browser, pass a response writer that ignores www-authenticate
			w = NonAuthenticatingResponseWriter{w}
		}
		api.ServeHTTP(w, r)

	}))
	mux.Handle("/swagger.json", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.ServeHTTP(w, r)
	}))

	// otherwise, serve  UI
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ui.ServeHTTP(w, r)
	}))
	return mux
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
