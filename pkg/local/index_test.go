package local_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	repo  = "foo"
	ref   = "bar"
	uPath = "baz"
	head  = "head"
)

var (
	testPath = uPath
	testUri  = &uri.URI{
		Repository: repo,
		Ref:        ref,
		Path:       &testPath,
	}
)

func writeIndex(t *testing.T, dir string) {
	_, err := local.WriteIndex(dir, testUri, head, "")
	require.NoError(t, err)
}

func TestWriteIndex(t *testing.T) {
	expectedContent := fmt.Sprintf("src: lakefs://%s/%s/%s\nat_head: %s\nactive_operation: \"\"\n", repo, ref, uPath, head)
	tmpDir := t.TempDir()
	writeIndex(t, tmpDir)
	buf, err := os.ReadFile(filepath.Join(tmpDir, local.IndexFileName))
	require.NoError(t, err)
	require.Equal(t, expectedContent, string(buf))
}

func TestReadIndex(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "path", "to", "index")
	require.NoError(t, os.MkdirAll(indexPath, os.ModePerm))
	writeIndex(t, indexPath)

	// Verify error on no index
	_, err := local.ReadIndex(tmpDir)
	require.ErrorIs(t, err, fs.ErrNotExist)

	// Read index from path
	res, err := local.ReadIndex(indexPath)
	require.NoError(t, err)
	require.Equal(t, indexPath, res.LocalPath())
	require.Equal(t, head, res.AtHead)
	require.Equal(t, testUri.String(), res.PathURI)
}

func TestFindIndices(t *testing.T) {
	root := t.TempDir()
	indicesFound := []string{
		filepath.Join(root, "path", "one"),
		filepath.Join(root, "path", "two"),
		filepath.Join(root, "path", "three", "four"),
		filepath.Join(root, "path", "three", "five", "six"),
		filepath.Join(root, "path2"),
	}
	indicesNotFound := []string{
		filepath.Join(root, "path", "one", "shouldNotFind"),
		filepath.Join(root, "path", "three", "four", "five", "shouldNotFind"),
	}
	for _, dir := range append(indicesFound, indicesNotFound...) {
		require.NoError(t, os.MkdirAll(dir, os.ModePerm))
		writeIndex(t, dir)
	}
	// Create some files
	for i, dir := range indicesFound {
		_, err := os.Create(filepath.Join(dir, fmt.Sprintf("file_%d", i)))
		require.NoError(t, err)
	}

	// Check on root
	dirs, err := local.FindIndices(root)
	require.NoError(t, err)
	require.Equal(t, len(indicesFound), len(dirs))
	for _, dir := range indicesFound {
		rel, err := filepath.Rel(root, dir)
		require.NoError(t, err)
		require.Contains(t, dirs, rel)
	}

	// Check on different sub path that was not supposed to be found from root
	dirs, err = local.FindIndices(filepath.Join(root, "path", "three", "four", "five"))
	require.NoError(t, err)
	require.Equal(t, 1, len(dirs))
	require.Equal(t, "shouldNotFind", dirs[0])

	// Create file on root and check only one result
	writeIndex(t, root)
	dirs, err = local.FindIndices(root)
	require.NoError(t, err)
	require.Equal(t, 1, len(dirs))
	require.Equal(t, ".", dirs[0])
}
