package local_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/api/apiutil"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/local"
)

const diffTestCorrectTime = 1691570412

func TestDiffLocal(t *testing.T) {
	osUid := os.Getuid()
	osGid := os.Getgid()
	umask := syscall.Umask(0)
	syscall.Umask(umask)

	cases := []struct {
		Name                   string
		IncludeUnixPermissions bool
		IncludeGID             bool
		IncludeUID             bool
		LocalPath              string
		InitLocalPath          func() string
		CleanLocalPath         func(localPath string)
		RemoteList             []apigen.ObjectStats
		Expected               local.Changes
	}{
		{
			Name:      "t1_no_diff",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(64),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
				},
			},
			Expected: []*local.Change{},
		},
		{
			Name:                   "t1_no_diff_include_folders",
			IncludeUnixPermissions: true,
			IncludeGID:             true,
			IncludeUID:             true,
			LocalPath:              "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(64),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, local.DefaultFilePermissions-umask),
				}, {
					Path:      "sub/",
					SizeBytes: swag.Int64(1),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, local.DefaultDirectoryPermissions-umask),
				}, {
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, local.DefaultFilePermissions-umask),
				}, {
					Path:      "sub/folder/",
					SizeBytes: swag.Int64(1),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, local.DefaultDirectoryPermissions-umask),
				}, {
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, local.DefaultFilePermissions-umask),
				},
			},
			Expected: []*local.Change{},
		},
		{
			Name:      "t1_modified",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(64),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     169095766,
				}, {
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(12),
					Mtime:     1690957665,
				},
			},
			Expected: []*local.Change{
				{
					Path: "sub/f.txt",
					Type: local.ChangeTypeModified,
				},
				{
					Path: "sub/folder/f.txt",
					Type: local.ChangeTypeModified,
				},
			},
		},
		{
			Name:      "t1_local_before",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(64),
					Mtime:     diffTestCorrectTime,
				},
				{
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
				},
			},
			Expected: []*local.Change{
				{
					Path: "sub/f.txt",
					Type: local.ChangeTypeAdded,
				},
			},
		},
		{
			Name:      "t1_local_after",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(64),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "tub/r.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     1690957665,
				},
			},
			Expected: []*local.Change{
				{
					Path: "sub/f.txt",
					Type: local.ChangeTypeAdded,
				},
				{
					Path: "sub/folder/f.txt",
					Type: local.ChangeTypeAdded,
				},
				{
					Path: "tub/r.txt",
					Type: local.ChangeTypeRemoved,
				},
			},
		},
		{
			Name:      "t1_hidden_changed",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(17),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
				},
			},
			Expected: []*local.Change{
				{
					Path: ".hidden-file",
					Type: local.ChangeTypeModified,
				},
			},
		},
		{
			Name:                   "t1_folder_added",
			IncludeUnixPermissions: true,
			IncludeGID:             true,
			IncludeUID:             true,
			LocalPath:              "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(64),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/",
					SizeBytes: swag.Int64(1),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     diffTestCorrectTime,
				},
			},
			Expected: []*local.Change{
				{
					Path: ".hidden-file",
					Type: local.ChangeTypeModified,
				},
				{
					Path: "sub/",
					Type: local.ChangeTypeModified,
				},
				{
					Path: "sub/f.txt",
					Type: local.ChangeTypeModified,
				},
				{
					Path: "sub/folder/",
					Type: local.ChangeTypeAdded,
				},
				{
					Path: "sub/folder/f.txt",
					Type: local.ChangeTypeAdded,
				},
			},
		},
		{
			Name:                   "t1_unix_permissions_modified",
			IncludeUnixPermissions: true,
			IncludeGID:             true,
			IncludeUID:             true,
			LocalPath:              "testdata/localdiff/t1/sub",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      "f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, 755),
				}, {
					Path:      "folder/",
					SizeBytes: swag.Int64(1),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid+1, local.DefaultDirectoryPermissions-umask),
				}, {
					Path:      "folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid+1, osGid, local.DefaultFilePermissions-umask),
				},
			},
			Expected: []*local.Change{
				{
					Path: "f.txt",
					Type: local.ChangeTypeModified,
				},
				{
					Path: "folder/",
					Type: local.ChangeTypeModified,
				},
				{
					Path: "folder/f.txt",
					Type: local.ChangeTypeModified,
				},
			},
		},
		{
			Name:                   "t1_unix_permissions_modified_only_gid",
			IncludeUnixPermissions: true,
			IncludeGID:             true,
			LocalPath:              "testdata/localdiff/t1/sub",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      "f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, 755),
				}, {
					Path:      "folder/",
					SizeBytes: swag.Int64(1),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid+1, local.DefaultDirectoryPermissions-umask),
				}, {
					Path:      "folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid+1, osGid, local.DefaultFilePermissions-umask),
				},
			},
			Expected: []*local.Change{
				{
					Path: "f.txt",
					Type: local.ChangeTypeModified,
				},
				{
					Path: "folder/",
					Type: local.ChangeTypeModified,
				},
			},
		},
		{
			Name:                   "t1_unix_permissions_modified_only_uid",
			IncludeUnixPermissions: true,
			IncludeUID:             true,
			LocalPath:              "testdata/localdiff/t1/sub",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      "f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, 755),
				}, {
					Path:      "folder/",
					SizeBytes: swag.Int64(1),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid+1, local.DefaultDirectoryPermissions-umask),
				}, {
					Path:      "folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid+1, osGid, local.DefaultFilePermissions-umask),
				},
			},
			Expected: []*local.Change{
				{
					Path: "f.txt",
					Type: local.ChangeTypeModified,
				},
				{
					Path: "folder/f.txt",
					Type: local.ChangeTypeModified,
				},
			},
		},
		{
			Name:                   "t1_empty_folder_removed",
			IncludeUnixPermissions: true,
			IncludeGID:             true,
			IncludeUID:             true,
			LocalPath:              "testdata/localdiff/t1/sub/folder",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      "empty-folder/",
					SizeBytes: swag.Int64(1),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid+1, osGid, local.DefaultDirectoryPermissions-umask),
				}, {
					Path:      "f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, local.DefaultFilePermissions-umask),
				},
			},
			Expected: []*local.Change{
				{
					Path: "empty-folder/",
					Type: local.ChangeTypeRemoved,
				},
			},
		},
		{
			Name:                   "t1_empty_folder_ignored",
			IncludeUnixPermissions: false,
			LocalPath:              "testdata/localdiff/t1/sub/folder",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      "empty-folder/",
					SizeBytes: swag.Int64(1),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid+1, osGid, local.DefaultDirectoryPermissions-umask),
				}, {
					Path:      "f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, local.DefaultFilePermissions-umask),
				},
			},
			Expected: []*local.Change{},
		},
		{
			Name:                   "empty_folder_added",
			IncludeUnixPermissions: true,
			IncludeGID:             true,
			IncludeUID:             true,
			InitLocalPath: func() string {
				return createTempEmptyFolder(t)
			},
			CleanLocalPath: func(localPath string) {
				_ = os.RemoveAll(localPath)
			},
			RemoteList: []apigen.ObjectStats{},
			Expected: []*local.Change{
				{
					Path: "empty-folder/",
					Type: local.ChangeTypeAdded,
				},
			},
		},
		{
			Name:                   "empty_folder_ignored",
			IncludeUnixPermissions: false,
			InitLocalPath: func() string {
				return createTempEmptyFolder(t)
			},
			CleanLocalPath: func(localPath string) {
				_ = os.RemoveAll(localPath)
			},
			RemoteList: []apigen.ObjectStats{},
			Expected:   []*local.Change{},
		},
	}

	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			if tt.InitLocalPath != nil {
				tt.LocalPath = tt.InitLocalPath()
			}

			fixTime(t, tt.LocalPath, tt.IncludeUnixPermissions)
			fixUnixPermissions(t, tt.LocalPath)

			left := tt.RemoteList
			sort.SliceStable(left, func(i, j int) bool {
				return left[i].Path < left[j].Path
			})
			lc := make(chan apigen.ObjectStats, len(left))
			makeChan(lc, left)

			changes, err := local.DiffLocalWithHead(lc, tt.LocalPath, local.Config{
				IncludePerm: tt.IncludeUnixPermissions,
				IncludeUID:  tt.IncludeUID,
				IncludeGID:  tt.IncludeGID,
			})

			if tt.CleanLocalPath != nil {
				tt.CleanLocalPath(tt.LocalPath)
			}

			if err != nil {
				t.Fatal(err)
			}

			// Sort changes for comparison
			sort.Slice(changes, func(i, j int) bool {
				return changes[i].Path < changes[j].Path
			})
			sort.Slice(tt.Expected, func(i, j int) bool {
				return tt.Expected[i].Path < tt.Expected[j].Path
			})

			// Debug output
			if len(changes) != len(tt.Expected) {
				t.Logf("Expected changes: %v", tt.Expected)
				t.Logf("Actual changes: %v", changes)
				for i, change := range changes {
					t.Logf("Change %d: Path=%s, Type=%s", i, change.Path, local.ChangeTypeString(change.Type))
				}
			}

			require.Equal(t, len(tt.Expected), len(changes),
				"expected %d changes, got %d", len(tt.Expected), len(changes))
			for i, c := range changes {
				require.Equal(t, c.Path, tt.Expected[i].Path, "wrong path")
				require.Equal(t, c.Type, tt.Expected[i].Type, "wrong type")
			}
		})
	}
}

func createTempEmptyFolder(t *testing.T) string {
	dataDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	_ = os.Mkdir(filepath.Join(dataDir, "empty-folder"), 0o755)
	return dataDir
}

func getPermissionsMetadata(uid, gid, mode int) *apigen.ObjectUserMetadata {
	return &apigen.ObjectUserMetadata{
		AdditionalProperties: map[string]string{
			local.POSIXPermissionsMetadataKey: fmt.Sprintf("{\"UID\":%d,\"GID\":%d,\"Mode\":%d}", uid, gid, mode),
		},
	}
}

func makeChan[T any](c chan<- T, l []T) {
	for _, o := range l {
		c <- o
	}
	close(c)
}

func fixTime(t *testing.T, localPath string, includeDirs bool) {
	err := filepath.WalkDir(localPath, func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() || includeDirs {
			return os.Chtimes(path, time.Now(), time.Unix(diffTestCorrectTime, 0))
		}
		return nil
	})
	require.NoError(t, err)
}

func fixUnixPermissions(t *testing.T, localPath string) {
	err := filepath.WalkDir(localPath, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		perm := local.GetDefaultPermissions(d.IsDir())
		if err := os.Chown(path, os.Getuid(), os.Getgid()); err != nil {
			return err
		}
		return os.Chmod(path, perm.Mode)
	})
	require.NoError(t, err)
}

// hasPrefix returns true if slice starts with prefix.
func hasPrefix(slice, prefix []string) bool {
	if len(slice) < len(prefix) {
		return false
	}
	return slices.Equal(slice[:len(prefix)], prefix)
}

// cleanPrefixes removes all paths from fileList which are prefixes of
// another path.  The result is a list of paths which can be created as
// files in a POSIX directory structure.  As a special case, an empty string
// is considered a self-prefix: it will be a directory, which already
// exists.
func cleanPrefixes(t testing.TB, fileList []string) []string {
	t.Helper()
	for i, file := range fileList {
		if file != "" && file[0] == '/' {
			file = "." + file
		}
		fileList[i] = filepath.Clean(file)
	}
	sort.Strings(fileList)

	ret := make([]string, 0, len(fileList))
	var cur []string

	for _, file := range fileList {
		// Split on "/" explicitly: this is how we construct our
		// test data.
		//
		// TODO(ariels): Fuzzing on Windows will probably fail
		//     because of this, and we will need to translate
		//     backslashes to slashes above.
		next := strings.Split(file, "/")
		if file == "." || next[len(next)-1] == "" {
			continue
		}
		if cur != nil && hasPrefix(next, cur) {
			continue
		}
		cur = next
		ret = append(ret, file)
	}
	return ret
}

// setupFiles creates a directory structure containing all files in
// fileList.  fileList should already be cleaned of prefixes by
// cleanPrefixes.
func setupFiles(t testing.TB, fileList []string) string {
	t.Helper()
	dir := t.TempDir() + string(filepath.Separator)
	for _, file := range fileList {
		p := filepath.Join(dir, file)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), os.ModePerm))
		fd, err := os.Create(p)
		require.NoError(t, err)
		_ = fd.Close()
	}
	return dir
}

func getSortedFilesAndDirs(fileList []string) []string {
	res := make([]string, 0)
	visited := make(map[string]bool)

	sort.Strings(fileList)

	for _, file := range fileList {
		subDirs := strings.Split(file, "/")
		path := ""
		for _, subDir := range subDirs[:len(subDirs)-1] {
			path = fmt.Sprintf("%s%s/", path, subDir)
			if _, ok := visited[path]; !ok {
				// add the sub-directory
				res = append(res, path)
				visited[path] = true
			}
		}
		// add the file
		res = append(res, file)
	}

	return res
}

func TestWalkS3(t *testing.T) {
	cases := []struct {
		Name     string
		FileList []string
	}{
		{
			Name:     "reverse order",
			FileList: []string{"imported/new-prefix/prefix-1/file000001", "imported./new-prefix/prefix-1/file002100"},
		},
		{
			Name:     "file in the middle",
			FileList: []string{"imported/file000001", "imported/new-prefix/prefix-1/file000001", "imported./new-prefix/prefix-1/file002100"},
		},
		{
			Name:     "dirs at the end",
			FileList: []string{"imported/new-prefix/prefix-1/file000001", "imported./new-prefix/prefix-1/file002100", "file000001"},
		},
		{
			Name:     "files mixed with directories",
			FileList: []string{"imported/0000/1", "imported/00000", "imported/00010/1"},
		},
		{
			Name:     "file in between",
			FileList: []string{"imported,/0000/1", "imported.", "imported/00010/1"},
		},
	}
	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			dir := setupFiles(t, tt.FileList)
			var walkOrder []string
			err := local.WalkS3(dir, func(p string, info fs.FileInfo, err error) error {
				walkOrder = append(walkOrder, strings.TrimPrefix(p, dir))
				return nil
			})

			require.NoError(t, err)
			sortedFilesAndDirs := getSortedFilesAndDirs(tt.FileList)
			require.Equal(t, sortedFilesAndDirs, walkOrder)
		})
	}
}

func FuzzWalkS3(f *testing.F) {
	testcases := [][]string{
		{"imported/file000001", "imported/new-prefix/prefix-1/file000001", "imported./new-prefix/prefix-1/file002100"},
		{"imported/new-prefix/prefix-1/file000001", "imported./new-prefix/prefix-1/file002100", "file000001"},
		{"imported/0000/1", "imported/00000", "imported/00010/1"},
	}

	// Go fuzzing only supports string test cases.  Since \0 is not
	// valid in POSIX filenames, decode that as a separator.
	for _, tc := range testcases {
		f.Add(strings.Join(tc, "\x00"))
	}
	f.Fuzz(func(t *testing.T, tc string) {
		tcf := strings.Split(tc, "\x00")
		files := cleanPrefixes(t, tcf)

		dir := setupFiles(t, files)
		var walkOrder []string
		err := local.WalkS3(dir, func(p string, info fs.FileInfo, err error) error {
			walkOrder = append(walkOrder, strings.TrimPrefix(p, dir))
			return nil
		})
		require.NoError(t, err)

		sortedFilesAndDirs := getSortedFilesAndDirs(files)
		if len(sortedFilesAndDirs) == 0 {
			// require.Equal doesn't understand empty slices;
			// our code represents empty slices as nil.
			sortedFilesAndDirs = nil
		}

		require.Equal(t, sortedFilesAndDirs, walkOrder)
	})
}

func TestDiffLocal_symlinks(t *testing.T) {
	// Helpers used to setup environment for each test
	createRegularFile := func(t *testing.T, localPath string, filename string, content string) {
		filePath := filepath.Join(localPath, filename)
		err := os.WriteFile(filePath, []byte(content), 0o644)
		require.NoError(t, err)
		// Fix the mtime to match our test constant
		fixTime(t, filePath, false)
	}

	createSymlink := func(t *testing.T, localPath string, filename string, target string) {
		filePath := filepath.Join(localPath, filename)
		err := os.Symlink(target, filePath)
		require.NoError(t, err)
		// Fix the mtime to match our test constant
		// Use Lchtimes if available, otherwise skip for broken symlinks
		err = os.Chtimes(filePath, time.Now(), time.Unix(diffTestCorrectTime, 0))
		if err != nil {
			// If we can't set the time (e.g., on broken symlinks), that's ok for tests
			t.Logf("Warning: could not set mtime for symlink %s: %v", filePath, err)
		}
	}

	// Test cases
	cases := []struct {
		name                string
		symlinkSupport      bool
		skipNonRegularFiles bool
		setupLocal          func(t *testing.T, localPath string) // Setup local filesystem
		remoteList          []apigen.ObjectStats
		expectedChanges     []*local.Change
		expectError         bool
		errorContains       string
	}{
		{
			name:                "symlink_disabled_skip_non_regular_files_ignores_symlinks",
			symlinkSupport:      false,
			skipNonRegularFiles: true,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "target_file", "target content")
				createSymlink(t, localPath, "symlink_file.txt", "target_file")
				createSymlink(t, localPath, "symlink_dir", "target_dir")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "target_file",
					SizeBytes: swag.Int64(14), // matches "target content" length
					Mtime:     diffTestCorrectTime,
				},
			},
			expectedChanges: []*local.Change{},
		},
		{
			name:                "symlink_disabled_no_skip_regular_files_errors_on_symlinks",
			symlinkSupport:      false,
			skipNonRegularFiles: false,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "target_file", "target content")
				createSymlink(t, localPath, "symlink_file.txt", "target_file")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "target_file",
					SizeBytes: swag.Int64(14), // matches "target content" length
					Mtime:     diffTestCorrectTime,
				},
			},
			expectError:   true,
			errorContains: "not a regular file",
		},
		{
			name:                "symlink_enabled_includes_symlinks_to_files",
			symlinkSupport:      true,
			skipNonRegularFiles: false,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "target_file", "target content")
				createSymlink(t, localPath, "symlink_file.txt", "target_file")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "target_file",
					SizeBytes: swag.Int64(14), // matches "target content" length
					Mtime:     diffTestCorrectTime,
				},
			},
			expectedChanges: []*local.Change{
				{
					Path: "symlink_file.txt",
					Type: local.ChangeTypeAdded,
				},
			},
		},
		{
			name:                "symlink_enabled_includes_symlinks_to_directories",
			symlinkSupport:      true,
			skipNonRegularFiles: false,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "target_file", "target content")
				createSymlink(t, localPath, "symlink_dir", "target_dir")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "target_file",
					SizeBytes: swag.Int64(14), // matches "target content" length
					Mtime:     diffTestCorrectTime,
				},
			},
			expectedChanges: []*local.Change{
				{
					Path: "symlink_dir",
					Type: local.ChangeTypeAdded,
				},
			},
		},
		{
			name:                "symlink_enabled_includes_both_file_and_dir_symlinks",
			symlinkSupport:      true,
			skipNonRegularFiles: false,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "regular_file.txt", "test content")
				createSymlink(t, localPath, "symlink_file.txt", "target_file")
				createSymlink(t, localPath, "symlink_dir", "target_dir")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "regular_file.txt",
					SizeBytes: swag.Int64(12), // matches "test content" length
					Mtime:     diffTestCorrectTime,
				},
			},
			expectedChanges: []*local.Change{
				{
					Path: "symlink_dir",
					Type: local.ChangeTypeAdded,
				},
				{
					Path: "symlink_file.txt",
					Type: local.ChangeTypeAdded,
				},
			},
		},
		{
			name:                "symlink_modified_in_enabled_mode",
			symlinkSupport:      true,
			skipNonRegularFiles: false,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "regular_file.txt", "test content")
				createSymlink(t, localPath, "symlink_file.txt", "different_target")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "regular_file.txt",
					SizeBytes: swag.Int64(12), // matches "test content" length
					Mtime:     diffTestCorrectTime,
				},
				{
					Path:      "symlink_file.txt",
					SizeBytes: swag.Int64(0),
					Mtime:     diffTestCorrectTime,
					Metadata: &apigen.ObjectUserMetadata{
						AdditionalProperties: map[string]string{
							apiutil.SymlinkMetadataKey: "original_target",
						},
					},
				},
			},
			expectedChanges: []*local.Change{
				{
					Path: "symlink_file.txt",
					Type: local.ChangeTypeModified,
				},
			},
		},
		{
			name:                "symlink_removed_in_enabled_mode",
			symlinkSupport:      true,
			skipNonRegularFiles: false,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "regular_file.txt", "test content")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "regular_file.txt",
					SizeBytes: swag.Int64(12), // matches "test content" length
					Mtime:     diffTestCorrectTime,
				},
				{
					Path:      "symlink_file.txt",
					SizeBytes: swag.Int64(0),
					Mtime:     diffTestCorrectTime,
					Metadata: &apigen.ObjectUserMetadata{
						AdditionalProperties: map[string]string{
							apiutil.SymlinkMetadataKey: "target",
						},
					},
				},
			},
			expectedChanges: []*local.Change{
				{
					Path: "symlink_file.txt",
					Type: local.ChangeTypeRemoved,
				},
			},
		},
		{
			name:                "broken_symlink_in_enabled_mode",
			symlinkSupport:      true,
			skipNonRegularFiles: false,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "regular_file.txt", "test content")
				createSymlink(t, localPath, "broken_symlink.txt", "nonexistent_path")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "regular_file.txt",
					SizeBytes: swag.Int64(12), // matches "test content" length
					Mtime:     diffTestCorrectTime,
				},
			},
			expectedChanges: []*local.Change{
				{
					Path: "broken_symlink.txt",
					Type: local.ChangeTypeAdded,
				},
			},
		},
		{
			name:                "broken_symlink_disabled_skip_non_regular_files",
			symlinkSupport:      false,
			skipNonRegularFiles: true,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "regular_file.txt", "test content")
				createSymlink(t, localPath, "broken_symlink.txt", "nonexistent_path")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "regular_file.txt",
					SizeBytes: swag.Int64(12), // matches "test content" length
					Mtime:     diffTestCorrectTime,
				},
			},
			expectedChanges: []*local.Change{},
		},
		{
			name:                "symlink_enabled_skip_non_regular_files_ignores_symlinks",
			symlinkSupport:      true,
			skipNonRegularFiles: true,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "regular_file.txt", "test content")
				createSymlink(t, localPath, "symlink_file.txt", "target_file")
				createSymlink(t, localPath, "symlink_dir", "target_dir")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "regular_file.txt",
					SizeBytes: swag.Int64(12), // matches "test content" length
					Mtime:     diffTestCorrectTime,
				},
			},
			expectedChanges: []*local.Change{},
		},
		{
			name:                "symlink_enabled_no_changes",
			symlinkSupport:      true,
			skipNonRegularFiles: false,
			setupLocal: func(t *testing.T, localPath string) {
				createRegularFile(t, localPath, "regular_file.txt", "test content")
				createRegularFile(t, localPath, "target_file", "target content") // Create the target so symlink is not broken
				createSymlink(t, localPath, "symlink_file.txt", "target_file")
			},
			remoteList: []apigen.ObjectStats{
				{
					Path:      "regular_file.txt",
					SizeBytes: swag.Int64(12), // matches content length
					Mtime:     diffTestCorrectTime,
				},
				{
					Path:      "symlink_file.txt",
					SizeBytes: swag.Int64(0),
					Mtime:     diffTestCorrectTime,
					Metadata: &apigen.ObjectUserMetadata{
						AdditionalProperties: map[string]string{
							apiutil.SymlinkMetadataKey: "target_file",
						},
					},
				},
				{
					Path:      "target_file",
					SizeBytes: swag.Int64(14), // matches "target content" length
					Mtime:     diffTestCorrectTime,
				},
			},
			expectedChanges: []*local.Change{},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			testDir := t.TempDir()

			tt.setupLocal(t, testDir)

			// Create channel for remote objects
			remoteChan := make(chan apigen.ObjectStats, len(tt.remoteList))
			makeChan(remoteChan, tt.remoteList)

			// Run diff
			cfg := local.Config{
				SkipNonRegularFiles: tt.skipNonRegularFiles,
				SymlinkSupport:      tt.symlinkSupport,
			}

			changes, err := local.DiffLocalWithHead(remoteChan, testDir, cfg)

			// Check for expected errors
			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
				return
			}

			require.NoError(t, err)

			// Sort changes for comparison
			sort.Slice(changes, func(i, j int) bool {
				return changes[i].Path < changes[j].Path
			})
			sort.Slice(tt.expectedChanges, func(i, j int) bool {
				return tt.expectedChanges[i].Path < tt.expectedChanges[j].Path
			})

			require.Equal(t, len(tt.expectedChanges), len(changes),
				"expected %d changes, got %d", len(tt.expectedChanges), len(changes))

			for i, expected := range tt.expectedChanges {
				require.Equal(t, expected.Path, changes[i].Path,
					"change %d: expected path %s, got %s", i, expected.Path, changes[i].Path)
				require.Equalf(t, expected.Type, changes[i].Type,
					"change %d: expected type %s, got %s", i,
					local.ChangeTypeString(expected.Type),
					local.ChangeTypeString(changes[i].Type))
			}
		})
	}
}
