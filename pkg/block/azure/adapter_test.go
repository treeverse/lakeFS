package azure_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/params"
)

func TestAzureAdapter(t *testing.T) {
	adapter, err := azure.NewAdapter(params.Azure{
		StorageAccount:   accountName,
		StorageAccessKey: accountKey,
		URL:              &blockURL,
	})
	if err != nil {
		t.Fatal("Failed to create new adapter", err)
	}

	blocktest.TestAdapter(t, adapter, blockURL+"/"+containerName)
}
