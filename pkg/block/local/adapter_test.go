package local_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/local"
)

const testStorageNamespace = "local://test"

func TestLocalAdapter(t *testing.T) {
	adapter, err := local.NewAdapter("/tmp")
	if err != nil {
		t.Fatal("Failed to create new adapter", err)
	}

	blocktest.TestAdapter(t, adapter, testStorageNamespace)
}
