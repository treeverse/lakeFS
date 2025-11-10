package httputil

import "net/http"

// IsSuccessStatusCode returns true for status code 2xx
func IsSuccessStatusCode(response *http.Response) bool {
	return response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices
}

// KeepPrivate sets some headers on response to keep it private: no caching, no sniff,
// sameorigin, etc.  It should be used on any non-API response (HTML, text, object contents,
// etc.)
func KeepPrivate(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store, must-revalidate")
	w.Header().Set("Expires", "0")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")
	w.Header().Set("Content-Security-Policy", "default-src 'none'")
}
