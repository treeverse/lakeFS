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
		Name            string
		Contents        []byte
		Metadata        map[string]string
		Path            string
		UnixPermEnabled bool
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
			s := local.NewSyncManager(ctx, testClient, server.Client(), local.SyncFlags{
				Parallelism:      1,
				Presign:          false,
				PresignMultipart: false,
			}, tt.UnixPermEnabled)
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
			require.NoError(t, s.Sync(testFolderPath, u, changes))
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
			Name:  "basic upload",
			Path:  "my_object",
			Mtime: time.Now().Unix(),
		},
		{
			Name:  "download with client mtime",
			Mtime: time.Now().Add(24 * time.Hour).Unix(),
			Path:  "my_object",
		},
		{
			Name:        "download file POSIX perm metadata disabled",
			Permissions: &local.POSIXPermissions{Mode: local.DefaultDirectoryPermissions},
			Path:        "my_object",
		},
		{
			Name:            "download file POSIX perm enabled no metadata",
			Path:            "my_object",
			UnixPermEnabled: true,
		},
		{
			Name:            "download file POSIX perm enabled with metadata",
			Path:            "my_object",
			UnixPermEnabled: true,
			Permissions:     &local.POSIXPermissions{Mode: os.FileMode(0o100755)},
		},
		{
			Name:            "download folder POSIX perm no metadata",
			Path:            "folder1/",
			UnixPermEnabled: true,
		},
		{
			Name:            "download folder POSIX perm with metadata",
			Path:            "folder2/",
			UnixPermEnabled: true,
			Permissions:     &local.POSIXPermissions{Mode: os.FileMode(0o40770)},
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
				switch {
				case strings.HasSuffix(r.URL.Path, "/objects"):
					// Check Chown
					perm := local.POSIXPermissions{}
					data := []byte(r.Header.Get(apiutil.LakeFSHeaderInternalPrefix + "POSIX-permissions"))
					if len(data) > 0 {
						require.NoError(t, json.Unmarshal(data, &perm))
					} else {
						perm = local.GetDefaultPermissions(isDir)
					}
					expectedPerm := perm
					if tt.UnixPermEnabled && tt.Permissions != nil {
						expectedPerm.Mode = tt.Permissions.Mode
					}
					require.Equal(t, expectedPerm, perm)

					// Check Mtime
					require.Equal(t, fmt.Sprintf("%d", tt.Mtime), r.Header.Get(apiutil.LakeFSHeaderInternalPrefix+"client-mtime"))
				default:
					t.Fatal("Unexpected request")
				}
				w.WriteHeader(http.StatusOK)
			})
			server := httptest.NewServer(h)
			defer server.Close()

			testClient := getTestClient(t, server.URL)
			s := local.NewSyncManager(ctx, testClient, server.Client(), local.SyncFlags{
				Parallelism:      1,
				Presign:          false,
				PresignMultipart: false,
			}, tt.UnixPermEnabled)
			u := &uri.URI{
				Repository: "repo",
				Ref:        "main",
				Path:       nil,
			}
			changes := make(chan *local.Change, 2)
			changes <- &local.Change{
				Source: local.ChangeSourceLocal,
				Path:   tt.Path,
				Type:   local.ChangeTypeAdded,
			}
			close(changes)
			require.NoError(t, s.Sync(testFolderPath, u, changes))
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
