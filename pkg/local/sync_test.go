// Until https://github.com/jedib0t/go-pretty/issues/322 is resolved
//go:build !race

package local_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

func TestSyncManager_download(t *testing.T) {
	currentUID := os.Getuid()
	currentGID := os.Getgid()
	ctx := context.Background()

	testCases := []struct {
		Name             string
		Contents         []byte
		Metadata         map[string]string
		Path             string
		UnixPermEnabled  bool
		FailAttempts     int
		ExpectError      bool
		ExpectedAttempts int
	}{
		{
			Name:     "basic download",
			Contents: []byte("foobar\n"),
			Metadata: map[string]string{},
			Path:     "my_object",
		},
		{
			Name:     "download with client mtime",
			Contents: []byte("foobar\n"),
			Metadata: map[string]string{
				local.ClientMtimeMetadataKey: strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 10),
			},
			Path: "my_object",
		},
		{
			Name:     "download file POSIX perm metadata disabled",
			Contents: []byte("foobar\n"),
			Metadata: map[string]string{
				local.POSIXPermissionsMetadataKey: "{\"UID\":0,\"GID\":0,\"Mode\":775}",
			},
			Path: "my_object",
		},
		{
			Name:            "download file POSIX perm enabled no metadata",
			Contents:        []byte("foobar\n"),
			Metadata:        map[string]string{},
			Path:            "my_object",
			UnixPermEnabled: true,
		},
		{
			Name:     "download file POSIX perm enabled with metadata",
			Contents: []byte("foobar\n"),
			Metadata: map[string]string{
				local.POSIXPermissionsMetadataKey: fmt.Sprintf("{\"UID\":%d, \"GID\": %d, \"Mode\":%d}", currentUID, currentGID, 0o100755),
			},
			Path:            "my_object",
			UnixPermEnabled: true,
		},
		{
			Name:            "download folder POSIX perm no metadata",
			Contents:        nil,
			Metadata:        map[string]string{},
			Path:            "folder1/",
			UnixPermEnabled: true,
		},
		{
			Name:     "download folder POSIX perm with metadata",
			Contents: nil,
			Metadata: map[string]string{
				local.POSIXPermissionsMetadataKey: fmt.Sprintf("{\"UID\":%d, \"GID\": %d, \"Mode\":%d}", currentUID, currentGID, 0o40770),
			},
			Path:            "folder2/",
			UnixPermEnabled: true,
		},
		{
			Name:             "succeeds on second attempt",
			Contents:         []byte("foobar\n"),
			Metadata:         map[string]string{},
			Path:             "my_object",
			FailAttempts:     1,
			ExpectError:      false,
			ExpectedAttempts: 2,
		},
		{
			Name:             "fails after three attempts",
			Contents:         []byte("foobar\n"),
			Metadata:         map[string]string{},
			Path:             "my_object",
			FailAttempts:     3,
			ExpectError:      true,
			ExpectedAttempts: 3,
		},
	}

	for _, tt := range testCases {
		umask := syscall.Umask(0)
		syscall.Umask(umask)

		t.Run(tt.Name, func(t *testing.T) {
			// We must create the test at the user home dir otherwise we will file to chown
			home, err := os.UserHomeDir()
			require.NoError(t, err)
			testFolderPath := filepath.Join(home, fmt.Sprintf("sync_manager_test_%s", xid.New().String()))
			require.NoError(t, os.MkdirAll(testFolderPath, os.ModePerm))
			defer func() {
				_ = os.RemoveAll(testFolderPath)
			}()
			sizeBytes := int64(len(tt.Contents))
			mtime := time.Now().Unix()
			metadata := &apigen.ObjectUserMetadata{AdditionalProperties: tt.Metadata}
			var downloadAttempts int
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Content-Type-Options", "nosniff")
				var res interface{}
				switch {
				case strings.Contains(r.RequestURI, "/stat"):
					w.Header().Set("Content-Type", "application/json")
					res = &apigen.ObjectStats{
						Metadata:  metadata,
						Mtime:     mtime,
						Path:      tt.Path,
						SizeBytes: &sizeBytes,
					}
					require.NoError(t, json.NewEncoder(w).Encode(res))
				case strings.HasSuffix(r.URL.Path, "/objects"):
					downloadAttempts++
					if tt.FailAttempts > 0 && downloadAttempts <= tt.FailAttempts {
						// attempt fails during io.Copy
						hj, ok := w.(http.Hijacker)
						require.True(t, ok, "http server must support hijacking")
						conn, _, err := hj.Hijack()
						require.NoError(t, err)
						// write partial response and close connection
						_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: " + strconv.Itoa(len(tt.Contents)) + "\r\n\r\n" + string(tt.Contents[:2])))
						conn.Close()
						return
					}
					// successful attempt
					w.Header().Set("Content-Type", "text/plain")
					w.Header().Set("Content-Length", fmt.Sprintf("%d", sizeBytes))
					_, err := w.Write(tt.Contents)
					require.NoError(t, err)
				default:
					t.Fatal("Unexpected request")
				}
				w.WriteHeader(http.StatusOK)
			})
			server := httptest.NewServer(h)
			defer server.Close()

			testClient := getTestClient(t, server.URL)
			s := local.NewSyncManager(ctx, testClient, server.Client(), local.Config{
				SyncFlags: local.SyncFlags{
					Parallelism:      1,
					Presign:          false,
					PresignMultipart: false,
				},
				MaxDownloadRetries: 2,
				IncludePerm:        tt.UnixPermEnabled,
			})
			u := &uri.URI{
				Repository: "repo",
				Ref:        "main",
				Path:       nil,
			}
			changes := make(chan *local.Change, 2)
			changes <- &local.Change{
				Source: local.ChangeSourceRemote,
				Path:   tt.Path,
				Type:   local.ChangeTypeAdded,
			}
			close(changes)
			err = s.Sync(testFolderPath, u, changes)
			if tt.ExpectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				localPath := fmt.Sprintf("%s%c%s", testFolderPath, os.PathSeparator, tt.Path) // Have to build manually due to Clean
				stat, err := os.Stat(localPath)
				require.NoError(t, err)

				if !stat.IsDir() {
					data, err := os.ReadFile(localPath)
					require.NoError(t, err)
					require.Equal(t, tt.Contents, data)
				}

				// Check mtime
				expectedMTime := mtime
				if clientMTime, ok := tt.Metadata[local.ClientMtimeMetadataKey]; ok {
					expectedMTime, err = strconv.ParseInt(clientMTime, 10, 64)
					require.NoError(t, err)
				}
				require.Equal(t, expectedMTime, stat.ModTime().Unix())

				// Check perm
				expectedUser := os.Getuid()
				expectedGroup := os.Getgid()
				expectedMode := local.DefaultFilePermissions - umask
				if stat.IsDir() {
					expectedMode = local.DefaultDirectoryPermissions - umask
				}

				if tt.UnixPermEnabled {
					if value, ok := tt.Metadata[local.POSIXPermissionsMetadataKey]; ok {
						perm := &local.POSIXPermissions{}
						require.NoError(t, json.Unmarshal([]byte(value), &perm))
						expectedUser = perm.UID
						expectedGroup = perm.GID
						expectedMode = int(perm.Mode)
					}
				}

				if sys, ok := stat.Sys().(*syscall.Stat_t); ok {
					uid := int(sys.Uid)
					gid := int(sys.Gid)
					require.Equal(t, expectedUser, uid)
					require.Equal(t, expectedGroup, gid)
					require.Equal(t, expectedMode, int(sys.Mode))
				} else {
					t.Fatal("failed to get stat")
				}
			}

			if tt.FailAttempts > 0 {
				require.EqualValues(t, tt.ExpectedAttempts, downloadAttempts, "download should be attempted the expected number of times")
			}
		})
	}
}

func TestSyncManager_upload(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		Name            string
		Path            string
		Permissions     *local.POSIXPermissions
		UnixPermEnabled bool
		Mtime           int64
	}{
		{
			Name:  "basic",
			Path:  "my_object",
			Mtime: time.Now().Unix(),
		},
		{
			Name:  "with client mtime",
			Mtime: time.Now().Add(24 * time.Hour).Unix(),
			Path:  "my_object",
		},
		{
			Name:        "file POSIX perm metadata disabled",
			Permissions: &local.POSIXPermissions{Mode: local.DefaultDirectoryPermissions},
			Path:        "my_object",
		},
		{
			Name:            "file POSIX perm enabled no metadata",
			Path:            "my_object",
			UnixPermEnabled: true,
		},
		{
			Name:            "file POSIX perm enabled with metadata",
			Path:            "my_object",
			UnixPermEnabled: true,
			Permissions:     &local.POSIXPermissions{Mode: os.FileMode(0o100755)},
		},
		{
			Name:            "folder POSIX perm no metadata",
			Path:            "folder1/",
			UnixPermEnabled: true,
		},
		{
			Name:            "folder POSIX perm with metadata",
			Path:            "folder2/",
			UnixPermEnabled: true,
			Permissions:     &local.POSIXPermissions{Mode: os.FileMode(0o40770)},
		},
	}
	umask := syscall.Umask(0)
	syscall.Umask(umask)

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			// We must create the test at the user home dir otherwise we will file to chown
			home := t.TempDir()
			t.Setenv("HOME", home)
			testFolderPath := filepath.Join(home, fmt.Sprintf("sync_manager_test_%s", xid.New().String()))
			require.NoError(t, os.MkdirAll(testFolderPath, os.ModePerm))
			defer func() {
				_ = os.RemoveAll(testFolderPath)
			}()
			mode := os.FileMode(local.DefaultFilePermissions)
			if tt.Permissions != nil {
				mode = tt.Permissions.Mode
			}
			localPath := fmt.Sprintf("%s%c%s", testFolderPath, os.PathSeparator, tt.Path) // Have to build manually due to Clean
			isDir := strings.HasSuffix(localPath, uri.PathSeparator)
			if isDir {
				require.NoError(t, os.Mkdir(localPath, mode))
			} else {
				require.NoError(t, os.WriteFile(localPath, []byte("foobar\n"), mode))
			}
			require.NoError(t, os.Chtimes(localPath, time.Now(), time.Unix(tt.Mtime, 0)))

			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/objects") {
					t.Fatal("Unexpected request")
				}

				// Check Chown
				perm := local.POSIXPermissions{}
				data := []byte(r.Header.Get(apiutil.LakeFSHeaderInternalPrefix + "POSIX-permissions"))
				if len(data) > 0 {
					require.NoError(t, json.Unmarshal(data, &perm))
				} else {
					perm = local.GetDefaultPermissions(isDir)
				}
				if tt.UnixPermEnabled && tt.Permissions != nil {
					expectedPerm := local.POSIXPermissions{
						POSIXOwnership: local.POSIXOwnership{
							UID: os.Getuid(),
							GID: os.Getgid(),
						},
						Mode: os.FileMode(int(tt.Permissions.Mode) &^ umask),
					}
					require.Equal(t, expectedPerm, perm)
				}

				// Check Mtime
				expectedMtime := fmt.Sprintf("%d", tt.Mtime)
				fileMtime := r.Header.Get(apiutil.LakeFSHeaderInternalPrefix + "client-mtime")
				require.Equal(t, expectedMtime, fileMtime)

				w.WriteHeader(http.StatusOK)
			})
			server := httptest.NewServer(h)
			defer server.Close()

			testClient := getTestClient(t, server.URL)
			s := local.NewSyncManager(ctx, testClient, server.Client(), local.Config{
				SyncFlags: local.SyncFlags{
					Parallelism:      1,
					Presign:          false,
					PresignMultipart: false,
					NoProgress:       true,
				},
				IncludePerm: tt.UnixPermEnabled,
				IncludeGID:  tt.UnixPermEnabled,
				IncludeUID:  tt.UnixPermEnabled,
			})
			changes := make(chan *local.Change, 2)
			changes <- &local.Change{
				Source: local.ChangeSourceLocal,
				Path:   tt.Path,
				Type:   local.ChangeTypeAdded,
			}
			close(changes)

			remoteURI := &uri.URI{
				Repository: "repo",
				Ref:        "main",
			}
			require.NoError(t, s.Sync(testFolderPath, remoteURI, changes))
		})
	}
}

func TestSyncManager_download_symlinks(t *testing.T) {
	currentUID := os.Getuid()
	currentGID := os.Getgid()
	ctx := context.Background()

	testCases := []struct {
		Name              string
		Contents          []byte
		Metadata          map[string]string
		Path              string
		SymlinkMode       local.SymlinkMode
		ExpectedIsSymlink bool
		ExpectedTarget    string
	}{
		{
			Name:     "download symlink in support mode",
			Contents: []byte{}, // Empty content for symlink
			Metadata: map[string]string{
				local.SymlinkMetadataKey: "/path/to/target",
			},
			Path:              "my_symlink",
			SymlinkMode:       local.SymlinkModeSupport,
			ExpectedIsSymlink: true,
			ExpectedTarget:    "/path/to/target",
		},
		{
			Name:              "download symlink in follow mode",
			Contents:          []byte("target content"),
			Metadata:          map[string]string{},
			Path:              "my_symlink",
			SymlinkMode:       local.SymlinkModeFollow,
			ExpectedIsSymlink: false,
			ExpectedTarget:    "",
		},
		{
			Name:     "download symlink in skip mode",
			Contents: []byte{}, // Should not be downloaded
			Metadata: map[string]string{
				local.SymlinkMetadataKey: "/path/to/target",
			},
			Path:              "my_symlink",
			SymlinkMode:       local.SymlinkModeSkip,
			ExpectedIsSymlink: false, // Should not exist
			ExpectedTarget:    "",
		},
		{
			Name:     "download symlink with relative target",
			Contents: []byte{},
			Metadata: map[string]string{
				local.SymlinkMetadataKey: "relative/target",
			},
			Path:              "my_symlink",
			SymlinkMode:       local.SymlinkModeSupport,
			ExpectedIsSymlink: true,
			ExpectedTarget:    "relative/target",
		},
		{
			Name:     "download symlink with POSIX permissions",
			Contents: []byte{},
			Metadata: map[string]string{
				local.SymlinkMetadataKey:          "/path/to/target",
				local.POSIXPermissionsMetadataKey: fmt.Sprintf("{\"UID\":%d, \"GID\": %d, \"Mode\":%d}", currentUID, currentGID, 0o100755),
			},
			Path:              "my_symlink",
			SymlinkMode:       local.SymlinkModeSupport,
			ExpectedIsSymlink: true,
			ExpectedTarget:    "/path/to/target",
		},
	}

	for _, tt := range testCases {
		umask := syscall.Umask(0)
		syscall.Umask(umask)

		t.Run(tt.Name, func(t *testing.T) {
			// We must create the test at the user home dir otherwise we will fail to chown
			home, err := os.UserHomeDir()
			require.NoError(t, err)
			testFolderPath := filepath.Join(home, fmt.Sprintf("sync_manager_symlink_test_%s", xid.New().String()))
			require.NoError(t, os.MkdirAll(testFolderPath, os.ModePerm))
			defer func() {
				_ = os.RemoveAll(testFolderPath)
			}()

			sizeBytes := int64(len(tt.Contents))
			mtime := time.Now().Unix()
			metadata := &apigen.ObjectUserMetadata{AdditionalProperties: tt.Metadata}

			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Content-Type-Options", "nosniff")
				var res interface{}
				switch {
				case strings.Contains(r.RequestURI, "/stat"):
					w.Header().Set("Content-Type", "application/json")
					res = &apigen.ObjectStats{
						Metadata:  metadata,
						Mtime:     mtime,
						Path:      tt.Path,
						SizeBytes: &sizeBytes,
					}
					require.NoError(t, json.NewEncoder(w).Encode(res))
				case strings.HasSuffix(r.URL.Path, "/objects"):
					w.Header().Set("Content-Type", "text/plain")
					w.Header().Set("Content-Length", fmt.Sprintf("%d", sizeBytes))
					_, err := w.Write(tt.Contents)
					require.NoError(t, err)
				default:
					t.Fatal("Unexpected request")
				}
				w.WriteHeader(http.StatusOK)
			})
			server := httptest.NewServer(h)
			defer server.Close()

			testClient := getTestClient(t, server.URL)
			s := local.NewSyncManager(ctx, testClient, server.Client(), local.Config{
				SyncFlags: local.SyncFlags{
					Parallelism:      1,
					Presign:          false,
					PresignMultipart: false,
				},
				IncludePerm: true,
				SymlinkMode: tt.SymlinkMode,
			})

			u := &uri.URI{
				Repository: "repo",
				Ref:        "main",
				Path:       nil,
			}
			changes := make(chan *local.Change, 2)
			changes <- &local.Change{
				Source: local.ChangeSourceRemote,
				Path:   tt.Path,
				Type:   local.ChangeTypeAdded,
			}
			close(changes)

			err = s.Sync(testFolderPath, u, changes)
			if tt.SymlinkMode == local.SymlinkModeSkip {
				// In skip mode, the symlink should not be downloaded
				require.NoError(t, err)
				localPath := filepath.Join(testFolderPath, tt.Path)
				_, err := os.Stat(localPath)
				require.True(t, os.IsNotExist(err), "Symlink should not exist in skip mode")
				return
			}
			require.NoError(t, err)

			localPath := filepath.Join(testFolderPath, tt.Path)
			stat, err := os.Stat(localPath)
			require.NoError(t, err)

			if tt.ExpectedIsSymlink {
				// Check if it's a symlink
				symlinkInfo, err := os.Lstat(localPath)
				require.NoError(t, err)
				require.True(t, symlinkInfo.Mode()&os.ModeSymlink != 0, "Expected symlink but got regular file")

				// Check symlink target
				target, err := os.Readlink(localPath)
				require.NoError(t, err)
				require.Equal(t, tt.ExpectedTarget, target)

				// Check mtime
				expectedMTime := mtime
				if clientMTime, ok := tt.Metadata[local.ClientMtimeMetadataKey]; ok {
					expectedMTime, err = strconv.ParseInt(clientMTime, 10, 64)
					require.NoError(t, err)
				}
				require.Equal(t, expectedMTime, stat.ModTime().Unix())

				// Check permissions if POSIX permissions are enabled
				if value, ok := tt.Metadata[local.POSIXPermissionsMetadataKey]; ok {
					perm := &local.POSIXPermissions{}
					require.NoError(t, json.Unmarshal([]byte(value), &perm))

					if sys, ok := stat.Sys().(*syscall.Stat_t); ok {
						uid := int(sys.Uid)
						gid := int(sys.Gid)
						require.Equal(t, perm.UID, uid)
						require.Equal(t, perm.GID, gid)
						require.Equal(t, int(perm.Mode), int(sys.Mode))
					}
				}
			} else {
				// Should be a regular file with content
				require.True(t, stat.Mode().IsRegular(), "Expected regular file but got symlink")
				data, err := os.ReadFile(localPath)
				require.NoError(t, err)
				require.Equal(t, tt.Contents, data)
			}
		})
	}
}

func TestSyncManager_upload_symlinks(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		Name             string
		Path             string
		SymlinkMode      local.SymlinkMode
		SymlinkTarget    string
		ExpectedUpload   bool
		ExpectedMetadata map[string]string
	}{
		{
			Name:           "upload symlink in support mode",
			Path:           "my_symlink",
			SymlinkMode:    local.SymlinkModeSupport,
			SymlinkTarget:  "/path/to/target",
			ExpectedUpload: true,
			ExpectedMetadata: map[string]string{
				local.SymlinkMetadataKey: "/path/to/target",
			},
		},
		{
			Name:             "upload symlink in follow mode",
			Path:             "my_symlink",
			SymlinkMode:      local.SymlinkModeFollow,
			SymlinkTarget:    "/path/to/target",
			ExpectedUpload:   true,
			ExpectedMetadata: map[string]string{
				// Should not have symlink metadata, should upload target content
			},
		},
		{
			Name:             "upload symlink in skip mode",
			Path:             "my_symlink",
			SymlinkMode:      local.SymlinkModeSkip,
			SymlinkTarget:    "/path/to/target",
			ExpectedUpload:   false,
			ExpectedMetadata: map[string]string{},
		},
		{
			Name:           "upload symlink with relative target",
			Path:           "my_symlink",
			SymlinkMode:    local.SymlinkModeSupport,
			SymlinkTarget:  "relative/target",
			ExpectedUpload: true,
			ExpectedMetadata: map[string]string{
				local.SymlinkMetadataKey: "relative/target",
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			// We must create the test at the user home dir otherwise we will fail to chown
			home := t.TempDir()
			t.Setenv("HOME", home)
			testFolderPath := filepath.Join(home, fmt.Sprintf("sync_manager_symlink_upload_test_%s", xid.New().String()))
			require.NoError(t, os.MkdirAll(testFolderPath, os.ModePerm))
			defer func() {
				_ = os.RemoveAll(testFolderPath)
			}()

			localPath := filepath.Join(testFolderPath, tt.Path)

			// Create the symlink
			require.NoError(t, os.Symlink(tt.SymlinkTarget, localPath))

			// Set mtime for testing
			mtime := time.Now().Unix()
			require.NoError(t, os.Chtimes(localPath, time.Now(), time.Unix(mtime, 0)))

			uploadCalled := false
			var uploadedMetadata map[string]string

			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/objects") {
					t.Fatal("Unexpected request")
				}

				uploadCalled = true

				// Check symlink metadata
				symlinkTarget := r.Header.Get(apiutil.LakeFSHeaderInternalPrefix + "symlink-target")
				if tt.ExpectedMetadata[local.SymlinkMetadataKey] != "" {
					require.Equal(t, tt.ExpectedMetadata[local.SymlinkMetadataKey], symlinkTarget)
				}

				// Check mtime
				expectedMtime := fmt.Sprintf("%d", mtime)
				fileMtime := r.Header.Get(apiutil.LakeFSHeaderInternalPrefix + "client-mtime")
				require.Equal(t, expectedMtime, fileMtime)

				// Store metadata for verification
				uploadedMetadata = make(map[string]string)
				for key, values := range r.Header {
					if strings.HasPrefix(key, apiutil.LakeFSHeaderInternalPrefix) {
						uploadedMetadata[strings.TrimPrefix(key, apiutil.LakeFSHeaderInternalPrefix)] = values[0]
					}
				}

				w.WriteHeader(http.StatusOK)
			})
			server := httptest.NewServer(h)
			defer server.Close()

			testClient := getTestClient(t, server.URL)
			s := local.NewSyncManager(ctx, testClient, server.Client(), local.Config{
				SyncFlags: local.SyncFlags{
					Parallelism:      1,
					Presign:          false,
					PresignMultipart: false,
					NoProgress:       true,
				},
				IncludePerm: true,
				SymlinkMode: tt.SymlinkMode,
			})

			changes := make(chan *local.Change, 2)
			changes <- &local.Change{
				Source: local.ChangeSourceLocal,
				Path:   tt.Path,
				Type:   local.ChangeTypeAdded,
			}
			close(changes)

			remoteURI := &uri.URI{
				Repository: "repo",
				Ref:        "main",
			}

			err := s.Sync(testFolderPath, remoteURI, changes)
			require.NoError(t, err)

			// Verify upload behavior
			if tt.ExpectedUpload {
				require.True(t, uploadCalled, "Expected upload to be called")

				// Verify symlink metadata if expected
				if expectedTarget, ok := tt.ExpectedMetadata[local.SymlinkMetadataKey]; ok {
					require.Equal(t, expectedTarget, uploadedMetadata["symlink-target"])
				}
			} else {
				require.False(t, uploadCalled, "Expected upload to be skipped")
			}
		})
	}
}

func getTestClient(t *testing.T, endpoint string) *apigen.ClientWithResponses {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 100
	httpClient := &http.Client{
		Transport: transport,
	}

	serverEndpoint, err := apiutil.NormalizeLakeFSEndpoint(endpoint)
	require.NoError(t, err)

	client, err := apigen.NewClientWithResponses(
		serverEndpoint,
		apigen.WithHTTPClient(httpClient),
	)
	require.NoError(t, err)

	return client
}
