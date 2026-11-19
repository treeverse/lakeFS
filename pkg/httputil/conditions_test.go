package httputil_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/httputil"
)

func TestCheckGetPreconditions(t *testing.T) {
	t.Parallel()
	etag := `"abc"`
	mod := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	past := "Sat, 01 Jan 2000 00:00:00 GMT"
	same := httputil.HeaderTimestamp(mod)

	tests := []struct {
		name string
		h    http.Header
		want httputil.PreconditionResult
	}{
		{name: "none", want: httputil.PreconditionOK},
		{name: "If-Match match", h: http.Header{"If-Match": {etag}}, want: httputil.PreconditionOK},
		{name: "If-Match stale", h: http.Header{"If-Match": {`"old"`}}, want: httputil.PreconditionFailed},
		{name: "If-Match star", h: http.Header{"If-Match": {"*"}}, want: httputil.PreconditionOK},
		{name: "If-Match unquoted", h: http.Header{"If-Match": {"abc"}}, want: httputil.PreconditionOK},
		{name: "If-Match list", h: http.Header{"If-Match": {`"old", "abc"`}}, want: httputil.PreconditionOK},
		{name: "If-None-Match match", h: http.Header{"If-None-Match": {etag}}, want: httputil.PreconditionNotModified},
		{name: "If-None-Match other", h: http.Header{"If-None-Match": {`"old"`}}, want: httputil.PreconditionOK},
		{name: "If-None-Match star", h: http.Header{"If-None-Match": {"*"}}, want: httputil.PreconditionNotModified},
		{name: "If-Modified-Since same", h: http.Header{"If-Modified-Since": {same}}, want: httputil.PreconditionNotModified},
		{name: "If-Modified-Since past", h: http.Header{"If-Modified-Since": {past}}, want: httputil.PreconditionOK},
		{name: "If-Unmodified-Since same", h: http.Header{"If-Unmodified-Since": {same}}, want: httputil.PreconditionOK},
		{name: "If-Unmodified-Since past", h: http.Header{"If-Unmodified-Since": {past}}, want: httputil.PreconditionFailed},
		{name: "If-Match wins over If-Unmodified-Since", h: http.Header{"If-Match": {etag}, "If-Unmodified-Since": {past}}, want: httputil.PreconditionOK},
		{name: "If-None-Match wins over If-Modified-Since", h: http.Header{"If-None-Match": {etag}, "If-Modified-Since": {past}}, want: httputil.PreconditionNotModified},
		{name: "invalid If-Modified-Since ignored", h: http.Header{"If-Modified-Since": {"not-a-date"}}, want: httputil.PreconditionOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := httputil.CheckGetPreconditions(tt.h, etag, mod)
			if got != tt.want {
				t.Fatalf("CheckGetPreconditions() = %d, want %d", got, tt.want)
			}
		})
	}
}
