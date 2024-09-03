package esti

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/rs/xid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"golang.org/x/exp/slices"
)

const mainBranch = "main"

const minHTTPErrorStatusCode = 400

const (
	ViperStorageNamespaceKey = "storage_namespace"
	ViperBlockstoreType      = "blockstore_type"
)

var errNotVerified = errors.New("lakeFS failed")

var nonAlphanumericSequence = regexp.MustCompile("[^a-zA-Z0-9]+")

// skipOnSchemaMismatch matches the rawURL schema to the current tested storage namespace schema
func skipOnSchemaMismatch(t *testing.T, rawURL string) {
	t.Helper()
	namespaceURL, err := url.Parse(viper.GetString(ViperStorageNamespaceKey))
	if err != nil {
		t.Fatal("Failed to parse configured storage_namespace", err)
	}
	pathURL, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal("Failed to parse rawURL", err)
	}

	if namespaceURL.Scheme != pathURL.Scheme {
		t.Skip("Skip test on different URL schema type")
	}
}

// verifyResponse returns an error based on failed if resp failed to perform action.  It uses
// body in errors.
func verifyResponse(resp *http.Response, body []byte) error {
	if resp.StatusCode >= minHTTPErrorStatusCode {
		return fmt.Errorf("%w: got %d %s: %s", errNotVerified, resp.StatusCode, resp.Status, string(body))
	}
	return nil
}

// makeRepositoryName changes name to make it an acceptable repository name by replacing all
// non-alphanumeric characters with a `-`.
func makeRepositoryName(name string) string {
	return nonAlphanumericSequence.ReplaceAllString(name, "-")
}

func setupTest(t testing.TB) (context.Context, logging.Logger, string) {
	ctx := context.Background()
	name := makeRepositoryName(t.Name())
	log := logger.WithField("testName", name)
	repo := createRepositoryForTest(ctx, t)
	log.WithField("repo", repo).Info("Created repository")
	return ctx, log, repo
}

func tearDownTest(repoName string) {
	ctx := context.Background()
	deleteRepositoryIfAskedTo(ctx, repoName)
}

func createRepositoryForTest(ctx context.Context, t testing.TB) string {
	name := strings.ToLower(t.Name())
	return createRepositoryByName(ctx, t, name)
}

func createRepositoryByName(ctx context.Context, t testing.TB, name string) string {
	storageNamespace := generateUniqueStorageNamespace(name)
	name = makeRepositoryName(name)
	createRepository(ctx, t, name, storageNamespace, false)
	return name
}

func createReadOnlyRepositoryByName(ctx context.Context, t testing.TB, name string) string {
	storageNamespace := generateUniqueStorageNamespace(name)
	name = makeRepositoryName(name)
	createRepository(ctx, t, name, storageNamespace, true)
	return name
}

func createRepositoryUnique(ctx context.Context, t testing.TB) string {
	name := generateUniqueRepositoryName()
	return createRepositoryByName(ctx, t, name)
}

func generateUniqueRepositoryName() string {
	return "repo-" + xid.New().String()
}

func generateUniqueStorageNamespace(repoName string) string {
	ns := viper.GetString("storage_namespace")
	if !strings.HasSuffix(ns, "/") {
		ns += "/"
	}
	return ns + xid.New().String() + "/" + repoName
}

func createRepository(ctx context.Context, t testing.TB, name string, repoStorage string, isReadOnly bool) {
	logger.WithFields(logging.Fields{
		"repository":        name,
		"storage_namespace": repoStorage,
		"name":              name,
	}).Debug("Create repository for test")
	resp, err := client.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
		DefaultBranch:    apiutil.Ptr(mainBranch),
		Name:             name,
		StorageNamespace: repoStorage,
		ReadOnly:         &isReadOnly,
	})
	require.NoErrorf(t, err, "failed to create repository '%s', storage '%s'", name, repoStorage)
	require.NoErrorf(t, verifyResponse(resp.HTTPResponse, resp.Body),
		"create repository '%s', storage '%s'", name, repoStorage)
}

func deleteRepositoryIfAskedTo(ctx context.Context, repositoryName string) {
	deleteRepositories := viper.GetBool("delete_repositories")
	if deleteRepositories {
		resp, err := client.DeleteRepositoryWithResponse(ctx, repositoryName, &apigen.DeleteRepositoryParams{})
		if err != nil {
			logger.WithError(err).WithField("repo", repositoryName).Error("Request to delete repository failed")
		} else if resp.StatusCode() != http.StatusNoContent {
			logger.WithFields(logging.Fields{"repo": repositoryName, "status_code": resp.StatusCode()}).Error("Request to delete repository failed")
		} else {
			logger.WithField("repo", repositoryName).Info("Deleted repository")
		}
	}
}

const (
	// randomDataContentLength is the content length used for small
	// objects.  It is intentionally not a round number.
	randomDataContentLength = 16

	// minDataContentLengthForMultipart is the content length for all
	// parts of a multipart upload except the last.  Its value -- 5MiB
	// -- is defined in the S3 protocol, and cannot be changed.
	minDataContentLengthForMultipart = 5 << 20

	// largeDataContentLength is >minDataContentLengthForMultipart,
	// which is large enough to require multipart operations.
	largeDataContentLength = 6 << 20
)

func uploadFileRandomDataAndReport(ctx context.Context, repo, branch, objPath string, direct bool) (checksum, content string, err error) {
	objContent := randstr.String(randomDataContentLength)
	checksum, err = uploadFileAndReport(ctx, repo, branch, objPath, objContent, direct)
	if err != nil {
		return "", "", err
	}
	return checksum, objContent, nil
}

func uploadFileAndReport(ctx context.Context, repo, branch, objPath, objContent string, direct bool) (checksum string, err error) {
	// Upload using direct access
	if direct {
		stats, err := uploadContentDirect(ctx, client, repo, branch, objPath, nil, "", strings.NewReader(objContent))
		if err != nil {
			return "", err
		}
		return stats.Checksum, nil
	}
	// Upload using API
	resp, err := uploadContent(ctx, repo, branch, objPath, objContent)
	if err != nil {
		return "", err
	}
	if err := verifyResponse(resp.HTTPResponse, resp.Body); err != nil {
		return "", err
	}
	return resp.JSON201.Checksum, nil
}

// uploadContentDirect uploads contents as a file using client-side ("direct") access to underlying storage.
// It requires credentials both to lakeFS and to underlying storage, but considerably reduces the load on the lakeFS
// server.
func uploadContentDirect(ctx context.Context, client apigen.ClientWithResponsesInterface, repoID, branchID, objPath string, metadata map[string]string, contentType string, contents io.ReadSeeker) (*apigen.ObjectStats, error) {
	resp, err := client.GetPhysicalAddressWithResponse(ctx, repoID, branchID, &apigen.GetPhysicalAddressParams{
		Path: objPath,
	})
	if err != nil {
		return nil, fmt.Errorf("get physical address to upload object: %w", err)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("%w: %s (status code %d)", helpers.ErrRequestFailed, resp.Status(), resp.StatusCode())
	}
	stagingLocation := resp.JSON200

	for { // Return from inside loop
		physicalAddress := apiutil.Value(stagingLocation.PhysicalAddress)
		parsedAddress, err := url.Parse(physicalAddress)
		if err != nil {
			return nil, fmt.Errorf("parse physical address URL %s: %w", physicalAddress, err)
		}

		adapter, err := NewAdapter(parsedAddress.Scheme)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", parsedAddress.Scheme, err)
		}

		stats, err := adapter.Upload(ctx, parsedAddress, contents)
		if err != nil {
			return nil, fmt.Errorf("upload to backing store: %w", err)
		}

		resp, err := client.LinkPhysicalAddressWithResponse(ctx, repoID, branchID, &apigen.LinkPhysicalAddressParams{
			Path: objPath,
		}, apigen.LinkPhysicalAddressJSONRequestBody{
			Checksum:  stats.ETag,
			SizeBytes: stats.Size,
			Staging:   *stagingLocation,
			UserMetadata: &apigen.StagingMetadata_UserMetadata{
				AdditionalProperties: metadata,
			},
			ContentType: &contentType,
		})
		if err != nil {
			return nil, fmt.Errorf("link object to backing store: %w", err)
		}
		if resp.JSON200 != nil {
			return resp.JSON200, nil
		}
		if resp.JSON409 == nil {
			return nil, fmt.Errorf("link object to backing store: %w (status code %d)", helpers.ErrRequestFailed, resp.StatusCode())
		}
		// Try again!
		if _, err = contents.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind: %w", err)
		}
	}
}

func uploadContent(ctx context.Context, repo string, branch string, objPath string, objContent string) (*apigen.UploadObjectResponse, error) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	contentWriter, err := w.CreateFormFile("content", filepath.Base(objPath))
	if err != nil {
		return nil, fmt.Errorf("create form file: %w", err)
	}
	_, err = contentWriter.Write([]byte(objContent))
	if err != nil {
		return nil, fmt.Errorf("write content: %w", err)
	}
	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("close form file: %w", err)
	}
	return client.UploadObjectWithBodyWithResponse(ctx, repo, branch, &apigen.UploadObjectParams{
		Path: objPath,
	}, w.FormDataContentType(), &b)
}

func uploadFileRandomData(ctx context.Context, t *testing.T, repo, branch, objPath string) (checksum, content string) {
	checksum, content, err := uploadFileRandomDataAndReport(ctx, repo, branch, objPath, false)
	require.NoError(t, err, "failed to upload file", repo, branch, objPath)
	return checksum, content
}

func listRepositoryObjects(ctx context.Context, t *testing.T, repository string, ref string) []apigen.ObjectStats {
	t.Helper()
	const amount = 5
	var entries []apigen.ObjectStats
	var after string
	for {
		resp, err := client.ListObjectsWithResponse(ctx, repository, ref, &apigen.ListObjectsParams{
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(amount)),
		})
		require.NoError(t, err, "listing objects")
		require.NoErrorf(t, verifyResponse(resp.HTTPResponse, resp.Body),
			"failed to list repo %s ref %s after %s amount %d", repository, ref, after, amount)

		entries = append(entries, resp.JSON200.Results...)
		after = resp.JSON200.Pagination.NextOffset
		if !resp.JSON200.Pagination.HasMore {
			break
		}
	}
	return entries
}

func listRepositoriesIDs(t *testing.T, ctx context.Context) []string {
	repos := listRepositories(t, ctx)
	ids := make([]string, len(repos))
	for i, repo := range repos {
		ids[i] = repo.Id
	}
	return ids
}

func listRepositories(t *testing.T, ctx context.Context) []apigen.Repository {
	var after string
	const repoPerPage = 2
	var listedRepos []apigen.Repository
	for {
		resp, err := client.ListRepositoriesWithResponse(ctx, &apigen.ListRepositoriesParams{
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(repoPerPage)),
		})
		require.NoError(t, err, "list repositories")
		require.NoErrorf(t, verifyResponse(resp.HTTPResponse, resp.Body),
			"failed to list repositories after %s amount %d", after, repoPerPage)
		require.Equal(t, http.StatusOK, resp.StatusCode())
		payload := resp.JSON200
		listedRepos = append(listedRepos, payload.Results...)
		if !payload.Pagination.HasMore {
			break
		}
		after = payload.Pagination.NextOffset
	}
	return listedRepos
}

// requireBlockstoreType Skips test if blockstore type doesn't match the required type
func requireBlockstoreType(t testing.TB, requiredTypes ...string) {
	blockstoreType := viper.GetString(config.BlockstoreTypeKey)
	if !slices.Contains(requiredTypes, blockstoreType) {
		t.Skipf("Required blockstore types: %v, got: %s", requiredTypes, blockstoreType)
	}
}

func isBasicAuth() bool {
	return viper.GetBool("auth.basic")
}
