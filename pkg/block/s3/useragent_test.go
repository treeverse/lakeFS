package s3_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/treeverse/lakefs/pkg/block/params"
	s3a "github.com/treeverse/lakefs/pkg/block/s3"
)

// roundTripFunc adapts a function into an http.RoundTripper so we can
// intercept the outgoing request and inspect its headers without hitting a
// real S3 endpoint.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// TestLoadConfigSetsLakeFSUserAgent verifies that S3 requests built from the
// adapter's aws.Config carry the lakefs/<version> User-Agent product, so that
// lakeFS-originated traffic is identifiable on the underlying object store's
// server-side request logs (AWS S3, Backblaze B2, MinIO, etc.).
//
// Regression guard for the AddUserAgentKeyValue middleware registered in
// LoadConfig.
func TestLoadConfigSetsLakeFSUserAgent(t *testing.T) {
	t.Parallel()

	var captured string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		captured = req.Header.Get("User-Agent")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})

	ctx := t.Context()
	cfg, err := s3a.LoadConfig(ctx, params.S3{
		Region: "us-east-1",
		Credentials: params.S3Credentials{
			AccessKeyID:     "test",
			SecretAccessKey: "test",
		},
	})
	require.NoError(t, err)
	cfg.HTTPClient = &http.Client{Transport: transport}

	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://127.0.0.1:1")
	})
	_, _ = client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String("test")})

	require.NotEmpty(t, captured, "expected User-Agent header on outgoing request")
	require.Contains(t, captured, "lakefs/",
		"User-Agent does not include the lakefs/ product (got %q)", captured)
}
