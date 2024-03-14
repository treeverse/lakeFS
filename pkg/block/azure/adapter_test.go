package azure_test

import (
	"context"
	"net/url"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/params"
)

func TestAzureAdapter(t *testing.T) {
	basePath, err := url.JoinPath(blockURL, containerName)
	require.NoError(t, err)
	localPath, err := url.JoinPath(basePath, "lakefs")
	require.NoError(t, err)
	externalPath, err := url.JoinPath(basePath, "external")
	require.NoError(t, err)

	adapter, err := azure.NewAdapter(context.Background(), params.Azure{
		StorageAccount:   accountName,
		StorageAccessKey: accountKey,
		TestEndpointURL:  blockURL,
	})
	require.NoError(t, err, "create new adapter")
	blocktest.AdapterTest(t, adapter, localPath, externalPath)
}

func TestAdapterNamespace(t *testing.T) {
	tests := []struct {
		Name      string
		Namespace string
		Success   bool
		China     bool
	}{
		{
			Name:      "valid_https",
			Namespace: "https://test.blob.core.windows.net/container1/repo1",
			Success:   true,
		},
		{
			Name:      "valid_https",
			Namespace: "https://test.blob.core.windows.net/container1/repo1",
			Success:   false,
			China:     true,
		},
		{
			Name:      "valid_https_china",
			Namespace: "https://test.blob.core.chinacloudapi.cn/container1/repo1",
			Success:   false,
		},
		{
			Name:      "valid_https_china",
			Namespace: "https://test.blob.core.chinacloudapi.cn/container1/repo1",
			Success:   true,
			China:     true,
		},
		{
			Name:      "valid_http",
			Namespace: "http://test.blob.core.windows.net/container1/repo1",
			Success:   true,
		},
		{
			Name:      "invalid_subdomain",
			Namespace: "https://test.adls.core.windows.net/container1/repo1",
			Success:   false,
		},
		{
			Name:      "partial",
			Namespace: "https://test.adls.core.windows.n",
			Success:   false,
		},
		{
			Name:      "s3",
			Namespace: "s3://test/adls/core/windows/net",
			Success:   false,
		},
		{
			Name:      "invalid_string",
			Namespace: "this is a bad string",
			Success:   false,
		},
		{
			Name:      "invalid_https_china_mix_1",
			Namespace: "https://test.blob.core.chinacloudapi.net/container1/repo1",
			Success:   false,
		},
		{
			Name:      "invalid_https_china_mix_2",
			Namespace: "https://test.blob.core.windows.cn/container1/repo1",
			Success:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			adapter, err := azure.NewAdapter(context.Background(), params.Azure{
				StorageAccount:   accountName,
				StorageAccessKey: accountKey,
				TestEndpointURL:  blockURL,
				ChinaCloud:       tt.China,
			})
			require.NoError(t, err, "create new adapter")

			expr, err := regexp.Compile(adapter.GetStorageNamespaceInfo().ValidityRegex)
			require.NoError(t, err)

			require.Equal(t, tt.Success, expr.MatchString(tt.Namespace))
		})
	}
}
