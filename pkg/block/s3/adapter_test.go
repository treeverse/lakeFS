package s3_test

import (
	"context"
	"net/url"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/params"
	s3a "github.com/treeverse/lakefs/pkg/block/s3"
	"github.com/treeverse/lakefs/pkg/config"
)

func getS3BlockAdapter(t *testing.T, opts []s3a.AdapterOption) *s3a.Adapter {
	s3params := params.S3{
		Region:               "us-east-1",
		Endpoint:             blockURL,
		ForcePathStyle:       true,
		DiscoverBucketRegion: false,
		Credentials: params.S3Credentials{
			AccessKeyID:     minioTestAccessKeyID,
			SecretAccessKey: minioTestSecretAccessKey,
		},
	}
	if opts == nil {
		opts = make([]s3a.AdapterOption, 0, 1)
	}
	opts = append(opts, s3a.WithNowFactory(blocktest.NowMockDefault))

	adapter, err := s3a.NewAdapter(context.Background(), s3params, opts...)
	if err != nil {
		t.Fatal("cannot create s3 adapter: ", err)
	}
	return adapter
}

// TestS3Adapter tests basic functionality of the S3 block adapter(backed by MinIO)
func TestS3Adapter(t *testing.T) {
	basePath, err := url.JoinPath("s3://", bucketName)
	require.NoError(t, err)
	localPath, err := url.JoinPath(basePath, "lakefs")
	require.NoError(t, err)
	externalPath, err := url.JoinPath(basePath, "external")
	require.NoError(t, err)

	adapter := getS3BlockAdapter(t, nil)
	blocktest.AdapterTest(t, adapter, localPath, externalPath)
}

// TestS3AdapterPresignedOverride tests basic functionality of the S3 block adapter along with the desired behavior of
// overriding the pre-signed URL endpoint
func TestS3AdapterPresignedOverride(t *testing.T) {
	basePath, err := url.JoinPath("s3://", bucketName)
	require.NoError(t, err)
	localPath, err := url.JoinPath(basePath, "lakefs")
	require.NoError(t, err)
	externalPath, err := url.JoinPath(basePath, "external")
	require.NoError(t, err)

	oeu, _ := url.Parse("https://myendpoint.com")
	adapter := getS3BlockAdapter(t, []s3a.AdapterOption{s3a.WithPreSignedEndpoint(oeu.String())})
	blocktest.AdapterPresignedEndpointOverrideTest(t, adapter, localPath, externalPath, oeu)
}

// TestAdapterNamespace tests the namespace validity regex with various paths
func TestAdapterNamespace(t *testing.T) {
	adapter := getS3BlockAdapter(t, nil)
	expr, err := regexp.Compile(adapter.GetStorageNamespaceInfo(config.SingleBlockstoreID).ValidityRegex)
	require.NoError(t, err)

	tests := []struct {
		Name      string
		Namespace string
		Success   bool
	}{
		{
			Name:      "valid_path",
			Namespace: "s3://bucket/path/to/repo1",
			Success:   true,
		},
		{
			Name:      "double_slash",
			Namespace: "s3://bucket/path//to/repo1",
			Success:   true,
		},
		{
			Name:      "invalid_schema",
			Namespace: "s3:/test/adls/core/windows/net",
			Success:   false,
		},
		{
			Name:      "invalid_path",
			Namespace: "https://test/adls/core/windows/net",
			Success:   false,
		},
		{
			Name:      "invalid_string",
			Namespace: "this is a bad string",
			Success:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			require.Equal(t, tt.Success, expr.MatchString(tt.Namespace))
		})
	}
}
