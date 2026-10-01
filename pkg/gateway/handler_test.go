package gateway_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/esti"
	"github.com/treeverse/lakefs/pkg/gateway/testutil"
	"github.com/treeverse/lakefs/pkg/httputil"
)

const repoName = "example"

func setupTest(t *testing.T, method, target string, body io.Reader) *http.Response {
	h, _ := testutil.GetBasicHandler(t, &testutil.FakeAuthService{
		BareDomain:      "example.com",
		AccessKeyID:     esti.DefaultAdminAccessKeyID,
		SecretAccessKey: esti.DefaultAdminSecretAccessKey,
		UserID:          "65867",
		Region:          "MockRegion",
	}, repoName)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(method, target, body)
	req.Host = "host.domain.com"
	req.Header.Set("Content-Type", "text/tab - separated - values")

	creds := aws.Credentials{
		AccessKeyID:     esti.DefaultAdminAccessKeyID,
		SecretAccessKey: esti.DefaultAdminSecretAccessKey,
	}

	signer := v4.NewSigner()
	payloadHash := "UNSIGNED-PAYLOAD" // For unsigned payload
	err := signer.SignHTTP(t.Context(), creds, req, payloadHash, "s3", "us-east-1", time.Now())
	require.NoError(t, err)

	h.ServeHTTP(rr, req)
	return rr.Result()
}

func TestPathWithTrailingSlash(t *testing.T) {
	result := setupTest(t, http.MethodHead, "/example/", nil)
	testPathWithTrailingSlash(t, result)
}

func testPathWithTrailingSlash(t *testing.T, result *http.Response) {
	assert.Equal(t, 200, result.StatusCode)
	bytes, err := io.ReadAll(result.Body)
	assert.NoError(t, err)
	assert.Len(t, bytes, 0)
	assert.Contains(t, result.Header, "X-Amz-Request-Id")
}

// TestListObjectsMalformedPrefixStatus asserts that a prefix the gateway cannot resolve is reported
// as 400 Bad Request, as the S3 API does, and not as 403 Access Denied. ListObjects fails closed on
// an unresolvable prefix by returning the resolve error from RequiredPermissions, so the request is
// rejected before it is authorized; RepoOperationHandler is responsible for mapping that error to a
// client error rather than to a denial.
func TestListObjectsMalformedPrefixStatus(t *testing.T) {
	for _, tt := range []struct {
		name   string
		prefix string
	}{
		{name: "bare separator", prefix: "%2F"},
		{name: "empty first component", prefix: "%2F%2Ffoo"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := setupTest(t, http.MethodGet,
				"/"+repoName+"?list-type=2&prefix="+tt.prefix+"&delimiter=%2F", nil)
			assert.Equal(t, http.StatusBadRequest, result.StatusCode)
		})
	}
}

// TestBucketSubResourceStatusUnchanged pins the end-to-end status of the bucket-level sub-resources
// served by ListObjects. RequiredPermissions runs before Handle demuxes them, so deriving the
// permission from the prefix alone would reject a prefix these operations ignore: ?location and
// ?versioning would go from 200 to 400, and ?uploads from 501 to 400.
func TestBucketSubResourceStatusUnchanged(t *testing.T) {
	for _, tt := range []struct {
		name  string
		query string
		want  int
	}{
		{name: "location", query: "location", want: http.StatusOK},
		{name: "location with malformed prefix", query: "location&prefix=%2F", want: http.StatusOK},
		{name: "versioning", query: "versioning", want: http.StatusOK},
		{name: "versioning with malformed prefix", query: "versioning&prefix=%2F", want: http.StatusOK},
		// ListMultipartUploads rejects prefix, delimiter and encoding-type as not implemented.
		{name: "uploads with prefix", query: "uploads&prefix=%2F", want: http.StatusNotImplemented},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := setupTest(t, http.MethodGet, "/"+repoName+"?"+tt.query, nil)
			assert.Equal(t, tt.want, result.StatusCode)
		})
	}
}

func signedRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req.Host = "host.domain.com"
	creds := aws.Credentials{
		AccessKeyID:     esti.DefaultAdminAccessKeyID,
		SecretAccessKey: esti.DefaultAdminSecretAccessKey,
	}
	err := v4.NewSigner().SignHTTP(t.Context(), creds, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now())
	require.NoError(t, err)
	return req
}

func TestAuthRejectionListsUnsignedHeaders(t *testing.T) {
	h, _ := testutil.GetBasicHandler(t, &testutil.FakeAuthService{
		BareDomain:      "example.com",
		AccessKeyID:     esti.DefaultAdminAccessKeyID,
		SecretAccessKey: esti.DefaultAdminSecretAccessKey,
		UserID:          "65867",
		Region:          "MockRegion",
	}, repoName)
	target := "/" + repoName + "/main/file.txt"

	testCases := []struct {
		name string
		req  func(t *testing.T) *http.Request
	}{
		{
			name: "authorization header",
			req: func(t *testing.T) *http.Request {
				return signedRequest(t, http.MethodPut, target)
			},
		},
		{
			name: "presigned URL",
			req: func(t *testing.T) *http.Request {
				creds := aws.Credentials{AccessKeyID: esti.DefaultAdminAccessKeyID, SecretAccessKey: esti.DefaultAdminSecretAccessKey}
				unsigned := httptest.NewRequest(http.MethodPut, "http://host.domain.com"+target+"?X-Amz-Expires=900", nil)
				presignedURL, _, err := v4.NewSigner().PresignHTTP(t.Context(), creds, unsigned, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now())
				require.NoError(t, err)
				return httptest.NewRequest(http.MethodPut, presignedURL, nil)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.req(t)
			req.Header.Set("X-Amz-Copy-Source", "/"+repoName+"/main/secret.txt")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			result := rr.Result()

			require.Equal(t, http.StatusForbidden, result.StatusCode)
			body, err := io.ReadAll(result.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), "<Code>AccessDenied</Code>")
			require.Contains(t, string(body), "<Message>There were headers present in the request which were not signed</Message>")
			require.Contains(t, string(body), "<HeadersNotSigned>x-amz-copy-source</HeadersNotSigned>")
		})
	}
}

func TestContextCancellation(t *testing.T) {
	h, _ := testutil.GetBasicHandler(t, &testutil.FakeAuthService{
		BareDomain:      "example.com",
		AccessKeyID:     "AKIAIO5FODNN7EXAMPLE",
		SecretAccessKey: "MockAccessSecretKey",
		UserID:          "65867",
		Region:          "MockRegion",
	}, repoName)

	rr := httptest.NewRecorder()

	// Create a context that is already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	req := httptest.NewRequest(http.MethodGet, "/"+repoName+"/main/nonexistent-file.txt", nil)
	req = req.WithContext(ctx)
	req.Header["Content-Type"] = []string{"text/plain"}
	req.Header["X-Amz-Content-Sha256"] = []string{"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
	req.Header["X-Amz-Date"] = []string{"20200517T093907Z"}
	req.Header["Host"] = []string{"host.domain.com"}
	req.Header["Authorization"] = []string{"AWS4-HMAC-SHA256 Credential=AKIAIO5FODNN7EXAMPLE/20200517/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=cdb193f2140d1d0c093adc7aba9a62bc3c75f84b117100888553115900f39223"}

	h.ServeHTTP(rr, req)
	result := rr.Result()

	// Verify that the status code is 499 (Client Closed Request)
	assert.Equal(t, httputil.HttpStatusClientClosedRequest, result.StatusCode, "Expected status code 499 for cancelled context")
}
