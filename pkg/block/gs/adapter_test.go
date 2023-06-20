package gs_test

import (
	"net/url"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/gs"
)

func getAdapter() *gs.Adapter {
	adapter := gs.NewAdapter(client)
	return adapter
}

func TestS3Adapter(t *testing.T) {
	basePath, err := url.JoinPath("gs://", bucketName)
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
			Namespace: "gs://bucket/path/to/repo1",
			Success:   true,
		},
		{
			Name:      "double_slash",
			Namespace: "gs://bucket/path//to/repo1",
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
