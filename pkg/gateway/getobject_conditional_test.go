package gateway_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/esti"
	"github.com/treeverse/lakefs/pkg/gateway/testutil"
	"github.com/treeverse/lakefs/pkg/httputil"
)

func newGatewayHandler(t *testing.T) http.Handler {
	t.Helper()
	t.Setenv("USE_STORAGE_NAMESPACE", "mem://replay")
	h, _ := testutil.GetBasicHandler(t, &testutil.FakeAuthService{
		BareDomain:      "example.com",
		AccessKeyID:     esti.DefaultAdminAccessKeyID,
		SecretAccessKey: esti.DefaultAdminSecretAccessKey,
		UserID:          "65867",
		Region:          "MockRegion",
	}, repoName)
	return h
}

func signedServe(t *testing.T, h http.Handler, method, target string, body io.Reader, headers http.Header) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	req.Host = "host.domain.com"
	if req.ContentLength > 0 {
		req.Header.Set("Content-Length", strconv.FormatInt(req.ContentLength, 10))
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	for name, values := range headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	creds := aws.Credentials{
		AccessKeyID:     esti.DefaultAdminAccessKeyID,
		SecretAccessKey: esti.DefaultAdminSecretAccessKey,
	}
	err := v4.NewSigner().SignHTTP(t.Context(), creds, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now())
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr.Result()
}

func putObject(t *testing.T, h http.Handler, key, body string) string {
	t.Helper()
	resp := signedServe(t, h, http.MethodPut, key, strings.NewReader(body), nil)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	etag := responseETag(resp.Header)
	require.NotEmpty(t, etag)
	return etag
}

func responseETag(h http.Header) string {
	if vs := h["ETag"]; len(vs) > 0 {
		return vs[0]
	}
	return h.Get("ETag")
}

func TestGetHeadObjectConditionalHeaders(t *testing.T) {
	h := newGatewayHandler(t)
	const (
		key      = "/" + repoName + "/main/f"
		oldBody  = "v1"
		newBody  = "v2"
		pastTime = "Mon, 01 Jan 2000 00:00:00 GMT"
	)

	staleETag := putObject(t, h, key, oldBody)
	currentETag := putObject(t, h, key, newBody)
	require.NotEqual(t, staleETag, currentETag)

	head := signedServe(t, h, http.MethodHead, key, nil, nil)
	_ = head.Body.Close()
	require.Equal(t, http.StatusOK, head.StatusCode)
	lastModified := head.Header.Get("Last-Modified")
	require.NotEmpty(t, lastModified)

	unquoted := httputil.StripQuotesAndSpaces(currentETag)

	tests := []struct {
		name       string
		method     string
		headers    http.Header
		wantStatus int
		wantBody   string
		wantCode   string
	}{
		{
			name:       "GET stale If-Match",
			method:     http.MethodGet,
			headers:    http.Header{"If-Match": {staleETag}},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name:       "HEAD stale If-Match",
			method:     http.MethodHead,
			headers:    http.Header{"If-Match": {staleETag}},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name:       "GET matching If-Match",
			method:     http.MethodGet,
			headers:    http.Header{"If-Match": {currentETag}},
			wantStatus: http.StatusOK,
			wantBody:   newBody,
		},
		{
			name:       "HEAD matching If-Match",
			method:     http.MethodHead,
			headers:    http.Header{"If-Match": {currentETag}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "GET If-None-Match current",
			method:     http.MethodGet,
			headers:    http.Header{"If-None-Match": {currentETag}},
			wantStatus: http.StatusNotModified,
		},
		{
			name:       "HEAD If-None-Match current",
			method:     http.MethodHead,
			headers:    http.Header{"If-None-Match": {currentETag}},
			wantStatus: http.StatusNotModified,
		},
		{
			name:       "GET If-None-Match stale",
			method:     http.MethodGet,
			headers:    http.Header{"If-None-Match": {staleETag}},
			wantStatus: http.StatusOK,
			wantBody:   newBody,
		},
		{
			name:       "GET If-Modified-Since current Last-Modified",
			method:     http.MethodGet,
			headers:    http.Header{"If-Modified-Since": {lastModified}},
			wantStatus: http.StatusNotModified,
		},
		{
			name:       "HEAD If-Modified-Since current Last-Modified",
			method:     http.MethodHead,
			headers:    http.Header{"If-Modified-Since": {lastModified}},
			wantStatus: http.StatusNotModified,
		},
		{
			name:       "GET If-Modified-Since in the past",
			method:     http.MethodGet,
			headers:    http.Header{"If-Modified-Since": {pastTime}},
			wantStatus: http.StatusOK,
			wantBody:   newBody,
		},
		{
			name:       "GET If-Unmodified-Since current Last-Modified",
			method:     http.MethodGet,
			headers:    http.Header{"If-Unmodified-Since": {lastModified}},
			wantStatus: http.StatusOK,
			wantBody:   newBody,
		},
		{
			name:       "GET If-Unmodified-Since in the past",
			method:     http.MethodGet,
			headers:    http.Header{"If-Unmodified-Since": {pastTime}},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name:       "HEAD If-Unmodified-Since in the past",
			method:     http.MethodHead,
			headers:    http.Header{"If-Unmodified-Since": {pastTime}},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name:       "ranged GET stale If-Match",
			method:     http.MethodGet,
			headers:    http.Header{"If-Match": {staleETag}, "Range": {"bytes=0-0"}},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name:       "ranged GET matching If-Match",
			method:     http.MethodGet,
			headers:    http.Header{"If-Match": {currentETag}, "Range": {"bytes=0-0"}},
			wantStatus: http.StatusPartialContent,
			wantBody:   "v",
		},
		{
			name:       "If-Match true wins over If-Unmodified-Since false",
			method:     http.MethodGet,
			headers:    http.Header{"If-Match": {currentETag}, "If-Unmodified-Since": {pastTime}},
			wantStatus: http.StatusOK,
			wantBody:   newBody,
		},
		{
			name:       "If-None-Match false wins over If-Modified-Since true",
			method:     http.MethodGet,
			headers:    http.Header{"If-None-Match": {currentETag}, "If-Modified-Since": {pastTime}},
			wantStatus: http.StatusNotModified,
		},
		{
			name:       "GET If-Match star",
			method:     http.MethodGet,
			headers:    http.Header{"If-Match": {"*"}},
			wantStatus: http.StatusOK,
			wantBody:   newBody,
		},
		{
			name:       "GET If-Match unquoted",
			method:     http.MethodGet,
			headers:    http.Header{"If-Match": {unquoted}},
			wantStatus: http.StatusOK,
			wantBody:   newBody,
		},
		{
			name:       "GET If-Match list containing current",
			method:     http.MethodGet,
			headers:    http.Header{"If-Match": {staleETag + ", " + currentETag}},
			wantStatus: http.StatusOK,
			wantBody:   newBody,
		},
		{
			name:   "presigned GET stale If-Match",
			method: http.MethodGet,
			headers: http.Header{
				"If-Match":   {staleETag},
				"User-Agent": {"client with s3RedirectionSupport set"},
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name:   "presigned ranged GET stale If-Match",
			method: http.MethodGet,
			headers: http.Header{
				"If-Match":   {staleETag},
				"Range":      {"bytes=0-0"},
				"User-Agent": {"client with s3RedirectionSupport set"},
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := signedServe(t, h, tt.method, key, nil, tt.headers)
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, resp.StatusCode, "body=%s", body)
			if tt.wantBody != "" {
				require.Equal(t, tt.wantBody, string(body))
			}
			if tt.wantStatus == http.StatusNotModified {
				require.Empty(t, body)
			}
			if tt.wantCode != "" && tt.method != http.MethodHead {
				require.Contains(t, string(body), tt.wantCode)
			}
		})
	}
}
