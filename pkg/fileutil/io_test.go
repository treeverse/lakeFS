package fileutil_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/fileutil"
)

func TestFindInParents(t *testing.T) {
	root := t.TempDir()
	dirTree := filepath.Join(root, "foo", "bar", "baz", "taz")
	require.NoError(t, os.MkdirAll(dirTree, fileutil.DefaultDirectoryMask))
	t.Run("does not exist", func(t *testing.T) {
		found, err := fileutil.FindInParents(root, ".doesnotexist21348329043289")
		if err != nil {
			t.Fatal(err)
		}
		if found != "" {
			t.Errorf("expected found to be empty, got %v", found)
		}
	})

	tests := []struct {
		name     string
		deep     string
		filename string
		filepath string
		find     bool
	}{
		{
			name:     "find_at_leaf",
			deep:     filepath.Join(root, "foo", "bar", "baz"),
			filename: "some_file0",
			filepath: filepath.Join(root, "foo", "bar", "baz", "some_file0"),
			find:     true,
		},
		{
			name:     "find_at_root",
			deep:     filepath.Join(root, "foo", "bar", "baz"),
			filename: "some_file1",
			filepath: filepath.Join(root, "some_file1"),
			find:     true,
		},
		{
			name:     "find_at_subpath",
			deep:     filepath.Join(root, "foo", "bar", "baz", "taz"),
			filename: "some_file2",
			filepath: filepath.Join(root, "foo", "some_file2"),
			find:     true,
		},
		{
			name:     "not_found_above",
			deep:     filepath.Join(root, "foo", "bar", "baz"),
			filename: "some_file3",
			filepath: filepath.Join(root, "foo", "bar", "baz", "taz", "some_file3"),
			find:     false,
		},
		{
			name:     "doesnt_exist",
			deep:     filepath.Join(root, "foo", "bar", "baz"),
			filename: ".doesnotexist21348329043289",
			filepath: filepath.Join(root, "foo", "bar", "some_file4"),
			find:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Create(tt.filepath)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			found, err := fileutil.FindInParents(tt.deep, tt.filename)
			require.NoError(t, err)
			if tt.find {
				require.Equal(t, tt.filepath, found)
			} else {
				require.Equal(t, "", found)
			}
		})
	}
}
