package httputil

import (
	"net/http"
	"strings"
	"time"
)

// PreconditionResult is the outcome of S3 GetObject/HeadObject conditional headers.
type PreconditionResult int

const (
	PreconditionOK PreconditionResult = iota
	PreconditionNotModified
	PreconditionFailed
)

// CheckGetPreconditions evaluates If-Match, If-Unmodified-Since, If-None-Match and
// If-Modified-Since using S3/RFC 7232 precedence:
// If-Match, then If-Unmodified-Since (skipped when If-Match is present and matches),
// then If-None-Match, then If-Modified-Since (skipped when If-None-Match is present).
func CheckGetPreconditions(h http.Header, etag string, lastModified time.Time) PreconditionResult {
	ifMatch := h.Get("If-Match")
	if ifMatch != "" {
		if !etagAnyMatch(ifMatch, etag) {
			return PreconditionFailed
		}
	} else if unmod := h.Get("If-Unmodified-Since"); unmod != "" {
		if t, ok := parseHTTPDate(unmod); ok && modifiedSince(lastModified, t) {
			return PreconditionFailed
		}
	}

	ifNone := h.Get("If-None-Match")
	if ifNone != "" {
		if etagAnyMatch(ifNone, etag) {
			return PreconditionNotModified
		}
		return PreconditionOK
	}
	if mod := h.Get("If-Modified-Since"); mod != "" {
		if t, ok := parseHTTPDate(mod); ok && !modifiedSince(lastModified, t) {
			return PreconditionNotModified
		}
	}
	return PreconditionOK
}

func etagAnyMatch(headerVal, etag string) bool {
	want := StripQuotesAndSpaces(etag)
	for _, part := range strings.Split(headerVal, ",") {
		token := strings.TrimSpace(part)
		if token == "*" {
			return true
		}
		token = strings.TrimPrefix(token, "W/")
		if StripQuotesAndSpaces(token) == want {
			return true
		}
	}
	return false
}

func parseHTTPDate(val string) (time.Time, bool) {
	t, err := http.ParseTime(val)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// modifiedSince reports whether lastModified is after since, using second precision
// like net/http (Last-Modified is truncated to a second).
func modifiedSince(lastModified, since time.Time) bool {
	return !lastModified.Before(since.Add(time.Second))
}
