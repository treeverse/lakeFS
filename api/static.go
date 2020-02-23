package api

import (
	"net/http"
)

func HandlerWithUI(api http.Handler, ui http.Handler) http.Handler {
	mux := http.NewServeMux()

	// /api/ and /swagger.json go to the api handler
	mux.Handle("/api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
