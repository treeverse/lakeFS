package local_test

import (
	"path"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/local"
)

const testStorageNamespace = "local://test"

func TestLocalAdapter(t *testing.T) {
	tmpDir := t.TempDir()
	localPath := path.Join(tmpDir, "lakefs")
	externalPath := block.BlockstoreTypeLocal + "://" + path.Join(tmpDir, "lakefs", "external")
	adapter, err := local.NewAdapter(localPath, local.WithRemoveEmptyDir(false))
	if err != nil {
		t.Fatal("Failed to create new adapter", err)
	}
	blocktest.AdapterTest(t, adapter, testStorageNamespace, externalPath)
}

func TestAdapterNamespace(t *testing.T) {
	tmpDir := t.TempDir()
	localPath := path.Join(tmpDir, "lakefs")
	adapter, err := local.NewAdapter(localPath, local.WithRemoveEmptyDir(false))
	expr, err := regexp.Compile(adapter.GetStorageNamespaceInfo().ValidityRegex)
	require.NoError(t, err)

	tests := []struct {
		Name      string
		Namespace string
		Success   bool
	}{
		{
			Name:      "valid_path",
			Namespace: "local://test/path/to/repo1",
			Success:   true,
		},
		{
			Name:      "invalid_path",
			Namespace: "~/test/path/to/repo1",
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
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			require.Equal(t, tt.Success, expr.MatchString(tt.Namespace))
		})
	}
}
