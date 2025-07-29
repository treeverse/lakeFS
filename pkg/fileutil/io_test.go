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

func TestVerifyNoSymlinksInPath(t *testing.T) {
	root := t.TempDir()

	// Create a directory structure
	dir2 := filepath.Join(root, "dir1", "dir2")
	dir3 := filepath.Join(root, "dir1", "dir2", "dir3")

	require.NoError(t, os.MkdirAll(dir3, fileutil.DefaultDirectoryMask))

	// Create a file in the deepest directory
	testFile := filepath.Join(dir3, "test.txt")
	f, err := os.Create(testFile)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Create a bad directory and symdir symlink under dir1
	badDir := filepath.Join(root, "bad")
	require.NoError(t, os.MkdirAll(badDir, fileutil.DefaultDirectoryMask))

	symdirPath := filepath.Join(root, "dir1", "symdir")
	require.NoError(t, os.Symlink(badDir, symdirPath))

	t.Run("no_symlinks", func(t *testing.T) {
		err := fileutil.VerifyNoSymlinksInPath(testFile, root)
		require.NoError(t, err)
	})

	t.Run("symlink_in_middle", func(t *testing.T) {
		// Create a symlink for dir2
		symlinkDir := filepath.Join(root, "symlink_dir")
		require.NoError(t, os.Symlink(dir2, symlinkDir))

		// Test with symlink in the path - should fail
		testFileWithSymlink := filepath.Join(symlinkDir, "dir3", "test.txt")
		err := fileutil.VerifyNoSymlinksInPath(testFileWithSymlink, root)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is a symlink")
	})

	t.Run("symlink_at_root", func(t *testing.T) {
		// Create a symlink for the root directory
		symlinkRoot := filepath.Join(t.TempDir(), "symlink_root")
		require.NoError(t, os.Symlink(root, symlinkRoot))

		// Test with symlink at root - should pass (root is excluded from check)
		testFileInSymlinkRoot := filepath.Join(symlinkRoot, "dir1", "dir2", "dir3", "test.txt")
		err := fileutil.VerifyNoSymlinksInPath(testFileInSymlinkRoot, symlinkRoot)
		require.NoError(t, err)
	})

	t.Run("non_existent_path", func(t *testing.T) {
		nonExistentFile := filepath.Join(root, "dir1", "non_existent", "file.txt")
		err := fileutil.VerifyNoSymlinksInPath(nonExistentFile, root)
		require.NoError(t, err)
	})

	t.Run("relative_paths", func(t *testing.T) {
		// Test with relative paths - should work (function converts to absolute)
		// Change to the root directory first
		originalWorkdir, err := os.Getwd()
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.Chdir(originalWorkdir))
		}()

		require.NoError(t, os.Chdir(root))
		relPath := filepath.Join("dir1", "dir2", "dir3", "test.txt")
		err = fileutil.VerifyNoSymlinksInPath(relPath, root)
		require.NoError(t, err)
	})

	t.Run("symlink_at_second_level", func(t *testing.T) {
		symdirIncludedPath := filepath.Join(root, "dir1", "symdir", "dir3")
		err := fileutil.VerifyNoSymlinksInPath(symdirIncludedPath, root)
		require.Error(t, err)
	})
}
