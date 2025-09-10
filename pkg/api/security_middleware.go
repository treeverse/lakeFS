package api

import (
	"net/http"
)

// SecurityMiddleware returns a new security middleware handler.
// It adds security headers to all HTTP responses:
// - X-Frame-Options: SAMEORIGIN - prevents clickjacking attacks
// - X-Content-Type-Options: nosniff - prevents MIME type sniffing
func SecurityMiddleware(next http.Handler) http.Handler {
	return &securityHandler{
		next: next,
	}
}

type securityHandler struct {
	next http.Handler
}

func (s *securityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// Call the next handler in the chain
	s.next.ServeHTTP(w, r)
}
