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
		LocalPath              string
		InitLocalPath          func() string
		CleanLocalPath         func(localPath string)
		RemoteList             []apigen.ObjectStats
		Expected               []*local.Change
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
			RemoteList: []apigen.ObjectStats{{
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
					Metadata:  getPermissionsMetadata(osUid+1, osGid, local.DefaultDirectoryPermissions-umask),
				}, {
					Path:      "folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
					Metadata:  getPermissionsMetadata(osUid, osGid, local.DefaultFilePermissions-umask),
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
			Name:                   "t1_empty_folder_removed",
			IncludeUnixPermissions: true,
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
			})

			if tt.CleanLocalPath != nil {
				tt.CleanLocalPath(tt.LocalPath)
			}

			if err != nil {
				t.Fatal(err)
			}

			if len(changes) != len(tt.Expected) {
				t.Fatalf("expected %d changes, got %d\n\n%v", len(tt.Expected), len(changes), changes)
			}
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
	_ = os.Mkdir(filepath.Join(dataDir, "empty-folder"), 0755)
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
	err := filepath.WalkDir(localPath, func(path string, d fs.DirEntry, err error) error {
		perm := local.GetDefaultPermissions(d.IsDir())
		if err = os.Chown(path, os.Getuid(), os.Getgid()); err != nil {
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
