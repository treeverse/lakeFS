package fileutil_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/fileutil"
)

func TestFindInParents(t *testing.T) {
	root := t.TempDir()
	dirTree := filepath.Join(root, "foo", "bar", "baz", "taz")
	require.NoError(t, os.MkdirAll(dirTree, fileutil.DefaultDirectoryMask))
	t.Run("dir does not exist", func(t *testing.T) {
		found, err := fileutil.FindInParents(filepath.Join(root, "no_dir"), "file")
		require.ErrorIs(t, err, fs.ErrNotExist)
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

func TestPruneEmptyDirectories(t *testing.T) {
	root := t.TempDir()

	cases := []struct {
		name     string
		paths    []string
		expected []string
	}{
		{
			name: "prune_deep",
			paths: []string{
				"a/b/",
				"a/b/c.txt",
				"a/d/",
				"a/e/a/b/",
			},
			expected: []string{
				"a/d",
				"a/e",
				"a/e/a",
				"a/e/a/b",
			},
		},
		{
			name: "prune_deep_keep_neighbor",
			paths: []string{
				"a/b/",
				"a/b/c.txt",
				"a/d/",
				"a/e/a/b/",
				"b.txt",
				"c/",
				"c/b.txt",
				"d/a/b/",
			},
			expected: []string{
				"a/d",
				"a/e",
				"a/e/a",
				"a/e/a/b",
				"d",
				"d/a",
				"d/a/b",
			},
		},
		{
			name:     "prune_keep_root",
			paths:    []string{},
			expected: []string{},
		},
		{
			name: "prune_all",
			paths: []string{
				"a/",
				"a/b/",
				"a/b/c/",
				"a/d/",
				"a/e/",
				"a/e/a/",
				"a/e/a/b/",
			},
			expected: []string{
				"a",
				"a/b",
				"a/b/c",
				"a/d",
				"a/e",
				"a/e/a",
				"a/e/a/b",
			},
		},
		{
			name: "nothing_to_prune",
			paths: []string{
				"a/b/",
				"a/b/c.txt",
				"d.txt",
			},
			expected: []string{},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var files []string

			currentRoot := filepath.Join(root, tt.name)
			require.NoError(t, os.Mkdir(currentRoot, fileutil.DefaultDirectoryMask))

			// create directory tree
			for _, entry := range tt.paths {
				fullPath := filepath.Join(currentRoot, entry)
				if strings.HasSuffix(entry, string(os.PathSeparator)) {
					// create dir
					require.NoError(t, os.MkdirAll(fullPath, fileutil.DefaultDirectoryMask))
				} else {
					// create file
					f, err := os.Create(fullPath)
					require.NoError(t, err)
					require.NoError(t, f.Close())
					files = append(files, f.Name())
				}
			}

			// prune
			removedDirs, err := fileutil.PruneEmptyDirectories(currentRoot)
			require.NoError(t, err)

			// make relative
			removedDirsRel := make([]string, len(removedDirs))
			for i, d := range removedDirs {
				relPath, _ := filepath.Rel(currentRoot, d)
				removedDirsRel[i] = relPath
			}

			// Verify root
			_, err = fileutil.IsDir(currentRoot)
			require.NoError(t, err)

			// Compare pruned list
			require.ElementsMatch(t, tt.expected, removedDirsRel)

			// Verify files
			for _, f := range files {
				_, err = os.ReadFile(f)
				require.NoError(t, err)
			}
		})
	}
}
