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

	blocktest.TestAdapter(t, adapter, localPath, externalPath)
}
