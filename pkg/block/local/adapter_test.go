package local_test

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/local"
	"github.com/treeverse/lakefs/pkg/config"
)

const testStorageNamespace = "local://test"

// TestLocalAdapter tests the Local Storage Adapter for basic storage functionality
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

// TestAdapterNamespace tests the namespace validity regex with various paths
func TestAdapterNamespace(t *testing.T) {
	tmpDir := t.TempDir()
	localPath := path.Join(tmpDir, "lakefs")
	adapter, err := local.NewAdapter(localPath, local.WithRemoveEmptyDir(false))
	require.NoError(t, err, "create new adapter")
	expr, err := regexp.Compile(adapter.GetStorageNamespaceInfo(config.SingleBlockstoreID).ValidityRegex)
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

// TestPathTraversalPrevention verifies that the adapter correctly blocks path traversal attacks,
// including prefix-based bypass attempts where a malicious path shares a prefix with the base path.
func TestPathTraversalPrevention(t *testing.T) {
	tmpDir := t.TempDir()
	localPath := path.Join(tmpDir, "lakefs")
	adapter, err := local.NewAdapter(localPath, local.WithRemoveEmptyDir(false))
	require.NoError(t, err, "create new adapter")

	tests := []struct {
		name           string
		namespace      string
		identifier     string
		expectErr      bool
		errMsgContains string
	}{
		{
			name:       "valid_path_under_base",
			namespace:  "local://valid/path",
			identifier: "object.txt",
			expectErr:  false,
		},
		{
			name:           "prefix_bypass_attempt",
			namespace:      "local://../lakefs_evil",
			identifier:     "malicious.txt",
			expectErr:      true,
			errMsgContains: "bad path traversal blocked",
		},
		{
			name:           "dotdot_traversal",
			namespace:      "local://foo/../../etc",
			identifier:     "passwd",
			expectErr:      true,
			errMsgContains: "bad path traversal blocked",
		},
		{
			name:           "identifier_dotdot_traversal",
			namespace:      "local://valid/path",
			identifier:     "../../../etc/passwd",
			expectErr:      true,
			errMsgContains: "bad path traversal blocked",
		},
		{
			name:       "identifier_valid_nested_path",
			namespace:  "local://valid/path",
			identifier: "subdir/nested/object.txt",
			expectErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := block.ObjectPointer{
				StorageNamespace: tt.namespace,
				Identifier:       tt.identifier,
			}
			_, err := adapter.Get(t.Context(), obj)
			if tt.expectErr {
				require.ErrorContains(t, err, tt.errMsgContains)
			}
			// For valid paths, we get a "file not found" error which is fine,
			// as we are testing the path traversal prevention.
		})
	}
}

// TestPathTraversalDataAccess verifies that path traversal via identifier cannot be used
// to access files outside the storage namespace. This test creates actual files to demonstrate
// the vulnerability where an attacker can escape the namespace directory to read sibling files.
func TestPathTraversalDataAccess(t *testing.T) {
	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "lakefs")
	adapter, err := local.NewAdapter(localPath, local.WithRemoveEmptyDir(false))
	require.NoError(t, err, "create new adapter")

	// Create a secret file outside the namespace but inside adapter's base path
	// Structure:
	//   /tmpDir/lakefs/
	//     └── repo1/
	//         ├── namespace/       <- user's namespace (local://repo1/namespace)
	//         └── other/secret.txt <- file that should NOT be accessible from namespace
	//
	// With identifier "subdir/../../other/secret.txt" and namespace "local://repo1/namespace":
	// - Full path resolves to: /tmpDir/lakefs/repo1/namespace/subdir/../../other/secret.txt
	// - After path.Join cleanup: /tmpDir/lakefs/repo1/other/secret.txt
	// - This escapes the namespace directory but stays inside adapter's base path
	secretDir := filepath.Join(localPath, "repo1", "other")
	err = os.MkdirAll(secretDir, 0o755)
	require.NoError(t, err, "create secret directory")

	secretFile := filepath.Join(secretDir, "secret.txt")
	secretContent := []byte("TOP SECRET DATA - should not be accessible via traversal")
	err = os.WriteFile(secretFile, secretContent, 0o644)
	require.NoError(t, err, "create secret file")

	// Verify the secret file exists
	_, err = os.Stat(secretFile)
	require.NoError(t, err, "secret file should exist")

	// Attempt to access the secret file via path traversal from a different namespace
	// The namespace is "local://repo1/namespace" but we try to escape to "../other/secret.txt"
	obj := block.ObjectPointer{
		StorageNamespace: "local://repo1/namespace",
		Identifier:       "subdir/../../other/secret.txt",
	}

	reader, err := adapter.Get(t.Context(), obj)

	// The adapter SHOULD block this with a path traversal error
	// If it doesn't block, we have a vulnerability
	if err == nil {
		// Vulnerability exists - we could read the file!
		data, readErr := io.ReadAll(reader)
		_ = reader.Close()
		require.NoError(t, readErr)
		t.Fatalf("PATH TRAVERSAL VULNERABILITY: Successfully read secret file via traversal! Content: %s", string(data))
	}

	// Verify the error is specifically about path traversal, not just "file not found"
	require.ErrorContains(t, err, "bad path traversal blocked",
		"adapter should block path traversal attempts with explicit error, got: %v", err)
}
