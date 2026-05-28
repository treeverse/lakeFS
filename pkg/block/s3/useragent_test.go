package s3_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/treeverse/lakefs/pkg/block/params"
	s3a "github.com/treeverse/lakefs/pkg/block/s3"
)

// TestLoadConfigSetsLakeFSUserAgent verifies that S3 requests built from the
// adapter's aws.Config carry the lakefs/<version> User-Agent product.
func TestLoadConfigSetsLakeFSUserAgent(t *testing.T) {
	t.Parallel()

	userAgentCh := make(chan string, 2)

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		userAgentCh <- r.Header.Get("User-Agent")
		close(userAgentCh)
	}))
	t.Cleanup(srv.Close)

	ctx := t.Context()
	cfg, err := s3a.LoadConfig(ctx, params.S3{
		Region: "us-east-1",
		Credentials: params.S3Credentials{
			AccessKeyID:     "test",
			SecretAccessKey: "test",
		},
	})
	require.NoError(t, err)

	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(srv.URL)
	})
	_, _ = client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String("test")})

	captured := <-userAgentCh
	require.Contains(t, captured, "lakefs/",
		"User-Agent does not include the lakefs/ product (got %q)", captured)
}
