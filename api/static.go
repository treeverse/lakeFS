package api

import (
	"net/http"
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
