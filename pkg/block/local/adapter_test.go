package local_test

import (
	"path"
	"testing"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/local"
)

const testStorageNamespace = "local://test"

func TestLocalAdapter(t *testing.T) {
	tmpDir := t.TempDir()
	localPath := path.Join(tmpDir, "lakefs")
	externalPath := block.BlockstoreTypeLocal + "://" + path.Join(tmpDir, "lakefs", "external")
	adapter, err := local.NewAdapter(localPath)
	if err != nil {
		t.Fatal("Failed to create new adapter", err)
	}

	blocktest.TestAdapter(t, adapter, testStorageNamespace, externalPath)
}
