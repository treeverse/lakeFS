package gateway

import "net/http"

// NoOpMiddleware returns a middleware that passes requests through unchanged.
func NoOpMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return next
	}
}
