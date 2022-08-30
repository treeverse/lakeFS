package gateway_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/treeverse/lakefs/pkg/gateway/testutil"
)

const repoName = "example"

func setupTest(t *testing.T, method, target string, body io.Reader) *http.Response {
	h, _ := testutil.GetBasicHandler(t, &testutil.FakeAuthService{
		BareDomain:      "example.com",
		AccessKeyID:     "AKIAIO5FODNN7EXAMPLE",
		SecretAccessKey: "MockAccessSecretKey",
		UserID:          "65867",
		Region:          "MockRegion",
	}, databaseURI, repoName)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(method, target, body)
	req.Header["Content-Type"] = []string{"text/tab - separated - values"}
	req.Header["X-Amz-Content-Sha256"] = []string{"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
	req.Header["X-Amz-Date"] = []string{"20200517T093907Z"}
	req.Header["Host"] = []string{"host.domain.com"}
	req.Header["Authorization"] = []string{"AWS4-HMAC-SHA256 Credential=AKIAIO5FODNN7EXAMPLE/20200517/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=cdb193f2140d1d0c093adc7aba9a62bc3c75f84b117100888553115900f39223"}
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
