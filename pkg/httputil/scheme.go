package httputil

import "net/http"

const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

func RequestScheme(r *http.Request) string {
	switch {
	case r.URL.Scheme == schemeHTTPS:
		return schemeHTTPS
	case r.Header.Get("X-Forwarded-Proto") == schemeHTTPS:
		return schemeHTTPS
	case r.Header.Get("X-Forwarded-Ssl") == "on":
		return schemeHTTPS
	default:
		return schemeHTTP
	}
}
