package api

import "net/http"

const JWTAuthorizationHeaderName = "X-JWT-Authorization"

// NewCookieAPIHandler set JWT header with JWT cookie value
func NewCookieAPIHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// api key header is not empty set it by cookie
		headerValue := r.Header.Get(JWTAuthorizationHeaderName)
		if len(headerValue) == 0 {
			// read cookie (no need to validate, this will be done in the API
			cookie, err := r.Cookie(JWTCookieName)
			if err == nil {
				r.Header.Set(JWTAuthorizationHeaderName, cookie.Value)
			}
		}
		next.ServeHTTP(w, r)
	})
}
