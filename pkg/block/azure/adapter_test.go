package azure_test

import (
	"net/url"
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

	adapter, err := azure.NewAdapter(params.Azure{
		StorageAccount:   accountName,
		StorageAccessKey: accountKey,
		TestEndpointURL:  blockURL,
	})
	require.NoError(t, err, "create new adapter")

	namespaceTestParams := []blocktest.NamespaceTestParams{
		{
			Name:      "valid_https",
			Namespace: "https://test.blob.core.windows.net/container1/repo1",
			Success:   true,
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
	}
	blocktest.SetupNamespaceTest(namespaceTestParams)
	blocktest.AdapterTest(t, adapter, localPath, externalPath)
}
