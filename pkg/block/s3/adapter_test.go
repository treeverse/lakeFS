package s3_test

import (
	"net/url"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/s3"
)

func getAdapter() *s3.Adapter {
	sess := session.Must(session.NewSession(
		aws.NewConfig().WithCredentials(credentials.NewCredentials(
			&credentials.StaticProvider{
				Value: credentials.Value{
					AccessKeyID:     minioTestAccessKeyID,
					SecretAccessKey: minioTestSecretAccessKey,
				},
			})).WithEndpoint(blockURL).
			WithRegion("us-east-1").
			WithS3ForcePathStyle(true)),
	)
	adapter := s3.NewAdapter(sess, s3.WithDiscoverBucketRegion(false))
	return adapter
}

func TestS3Adapter(t *testing.T) {
	basePath, err := url.JoinPath("s3://", bucketName)
	require.NoError(t, err)
	localPath, err := url.JoinPath(basePath, "lakefs")
	require.NoError(t, err)
	externalPath, err := url.JoinPath(basePath, "external")
	require.NoError(t, err)

	adapter := getAdapter()
	blocktest.AdapterTest(t, adapter, localPath, externalPath)
}

func TestAdapterNamespace(t *testing.T) {
	adapter := getAdapter()
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
