package httputil_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestFlusherCaptureHandler(t *testing.T) {
	const responseBody = `{}`
	handlerFunc := httputil.FlusherCaptureHandler(logging.Dummy(), time.Second)
	handler := handlerFunc(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ContentType", "application/json")
		// validate that context holds a flusher
		flusher, ok := r.Context().Value(httputil.FlusherContextKey).(http.Flusher)
		if !ok || flusher == nil {
			t.Errorf("Failed to extract Flusher from context (ok %t), flusher=%v", ok, flusher)
		}
		_, _ = io.WriteString(w, responseBody)
	}))
	svr := httptest.NewServer(handler)
	defer svr.Close()

	resp := testClientPost(t, svr)
	if resp != responseBody {
		t.Fatalf("Response body '%s', expected '%s'", resp, responseBody)
	}
}

func TestLongPost(t *testing.T) {
	const (
		responseBody     = `{}`
		longPostDuration = time.Second
	)
	handlerFunc := httputil.FlusherCaptureHandler(logging.Dummy(), longPostDuration)
	handler := handlerFunc(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		longPost := httputil.NewLongPost(r.Context(), w, longPostDuration, http.StatusOK, logging.Dummy())
		defer longPost.Close()
		time.Sleep(longPostDuration + time.Second)
		w.Header().Set("ContentType", "application/json")
		_, _ = io.WriteString(w, responseBody)
	}))
	svr := httptest.NewServer(handler)
	defer svr.Close()

	resp := testClientPost(t, svr)
	if !strings.HasPrefix(resp, " ") {
		t.Fatalf("Response should have been padded with space (using long post): '%s'", resp)
	}
	pureResponseData := strings.TrimSpace(resp)
	if pureResponseData != responseBody {
		t.Fatalf("Response body '%s', expected '%s'", pureResponseData, responseBody)
	}
}

func testClientPost(t *testing.T, svr *httptest.Server) string {
	resp, err := http.Post(svr.URL, "application/json", strings.NewReader(`{"test":true}`))
	if err != nil {
		t.Fatal("Failed to post request", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal("failed to read response body", err)
	}
	return string(data)
}
