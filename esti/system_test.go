package esti

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

const mainBranch = "main"

const minHTTPErrorStatusCode = 400

var errNotVerified = errors.New("lakeFS failed")

var nonAlphanumericSequence = regexp.MustCompile("[^a-zA-Z0-9]+")

// skipOnSchemaMismatch matches the rawURL schema to the current tested storage namespace schema
func skipOnSchemaMismatch(t *testing.T, rawURL string) {
	t.Helper()
	namespaceURL, err := url.Parse(viper.GetString("storage_namespace"))
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

func setupTest(t *testing.T) (context.Context, logging.Logger, string) {
	SkipTestIfAskedTo(t)
	ctx := context.Background()
	name := makeRepositoryName(t.Name())
	logger := logger.WithField("testName", name)
	repo := createRepositoryForTest(ctx, t)
	logger.WithField("repo", repo).Info("Created repository")
	return ctx, logger, repo
}

func tearDownTest(repoName string) {
	ctx := context.Background()
	deleteRepositoryIfAskedTo(ctx, repoName)
}

func createRepositoryForTest(ctx context.Context, t *testing.T) string {
	name := strings.ToLower(t.Name())
	return createRepositoryByName(ctx, t, name)
}

func createRepositoryByName(ctx context.Context, t *testing.T, name string) string {
	storageNamespace := generateUniqueStorageNamespace(name)
	name = makeRepositoryName(name)
	createRepository(ctx, t, name, storageNamespace)
	return name
}

func createRepositoryUnique(ctx context.Context, t *testing.T) string {
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

func createRepository(ctx context.Context, t *testing.T, name string, repoStorage string) {
	logger.WithFields(logging.Fields{
		"repository":        name,
		"storage_namespace": repoStorage,
		"name":              name,
	}).Debug("Create repository for test")
	resp, err := client.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
		DefaultBranch:    api.StringPtr(mainBranch),
		Name:             name,
		StorageNamespace: repoStorage,
	})
	require.NoErrorf(t, err, "failed to create repository '%s', storage '%s'", name, repoStorage)
	require.NoErrorf(t, verifyResponse(resp.HTTPResponse, resp.Body),
		"create repository '%s', storage '%s'", name, repoStorage)
}

func deleteRepositoryIfAskedTo(ctx context.Context, repositoryName string) {
	deleteRepositories := viper.GetBool("delete_repositories")
	if deleteRepositories {
		resp, err := client.DeleteRepositoryWithResponse(ctx, repositoryName)
		if err != nil {
			logger.WithError(err).WithField("repo", repositoryName).Error("Request to delete repository failed")
		} else if resp.StatusCode() != http.StatusNoContent {
			logger.WithFields(logging.Fields{"repo": repositoryName, "status_code": resp.StatusCode()}).Error("Request to delete repository failed")
		} else {
			logger.WithField("repo", repositoryName).Info("Deleted repository")
		}
	}
}

const randomDataContentLength = 16

func uploadFileRandomDataAndReport(ctx context.Context, repo, branch, objPath string, direct bool) (checksum, content string, err error) {
	objContent := randstr.String(randomDataContentLength)
	checksum, err = uploadFileAndReport(ctx, repo, branch, objPath, objContent, direct)
	if err != nil {
		return "", "", err
	}
	return checksum, objContent, nil
}

func uploadFileAndReport(ctx context.Context, repo, branch, objPath, objContent string, direct bool) (checksum string, err error) {
	if direct {
		stats, err := helpers.ClientUploadDirect(ctx, client, repo, branch, objPath, nil, "", strings.NewReader(objContent))
		if err != nil {
			return "", err
		}
		return stats.Checksum, nil
	} else {
		resp, err := uploadContent(ctx, repo, branch, objPath, objContent)
		if err != nil {
			return "", err
		}
		if err := verifyResponse(resp.HTTPResponse, resp.Body); err != nil {
			return "", err
		}
		return resp.JSON201.Checksum, nil
	}
}

func uploadContent(ctx context.Context, repo string, branch string, objPath string, objContent string) (*api.UploadObjectResponse, error) {
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
	return client.UploadObjectWithBodyWithResponse(ctx, repo, branch, &api.UploadObjectParams{
		Path: objPath,
	}, w.FormDataContentType(), &b)
}

func uploadFileRandomData(ctx context.Context, t *testing.T, repo, branch, objPath string, direct bool) (checksum, content string) {
	checksum, content, err := uploadFileRandomDataAndReport(ctx, repo, branch, objPath, direct)
	require.NoError(t, err, "failed to upload file", repo, branch, objPath, "direct:", direct)
	return checksum, content
}

func listRepositoryObjects(ctx context.Context, t *testing.T, repository string, ref string) []api.ObjectStats {
	t.Helper()
	const amount = 5
	var entries []api.ObjectStats
	var after string
	for {
		resp, err := client.ListObjectsWithResponse(ctx, repository, ref, &api.ListObjectsParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
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

func listRepositories(t *testing.T, ctx context.Context) []api.Repository {
	var after string
	const repoPerPage = 2
	var listedRepos []api.Repository
	for {
		resp, err := client.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(repoPerPage),
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

// requireBlockstoreType Skips test if blockstore type doesn't match required type
func requireBlockstoreType(t *testing.T, requiredType string) {
	blockstoreType := viper.GetString(config.BlockstoreTypeKey)
	if blockstoreType != requiredType {
		t.Skip(fmt.Sprintf("Test %s requires blockstore type: %s, got: %s", t.Name(), requiredType, blockstoreType))
	}
}
