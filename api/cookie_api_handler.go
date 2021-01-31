package api

import "net/http"

const JWTAuthorizationHeaderName = "X-JWT-Authorization"

// CookieAPIHandler set JWT header with JWT cookie value
func CookieAPIHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// read cookie (no need to validate, this will be done in the API
		cookie, err := r.Cookie(JWTCookieName)
		if err == nil {
			r.Header.Set(JWTAuthorizationHeaderName, cookie.Value)
		}
		next.ServeHTTP(w, r)
	})
}
