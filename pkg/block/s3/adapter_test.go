package s3_test

import (
	"context"
	"net/url"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/block/s3"
)

func getS3BlockAdapter(t *testing.T) *s3.Adapter {
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
	adapter, err := s3.NewAdapter(context.Background(), s3params)
	if err != nil {
		t.Fatal("cannot create s3 adapter: ", err)
	}
	return adapter
}

func TestS3Adapter(t *testing.T) {
	basePath, err := url.JoinPath("s3://", bucketName)
	require.NoError(t, err)
	localPath, err := url.JoinPath(basePath, "lakefs")
	require.NoError(t, err)
	externalPath, err := url.JoinPath(basePath, "external")
	require.NoError(t, err)

	adapter := getS3BlockAdapter(t)
	blocktest.AdapterTest(t, adapter, localPath, externalPath)
}

func TestAdapterNamespace(t *testing.T) {
	adapter := getS3BlockAdapter(t)
	expr, err := regexp.Compile(adapter.GetStorageNamespaceInfo().ValidityRegex)
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
