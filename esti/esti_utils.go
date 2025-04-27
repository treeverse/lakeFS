//nolint:unused
package esti

// TODO (niro): All the unused errors is because our esti tests filenames are suffixed with _test
// TODO (niro): WE will need to rename all the esti tests file names to instead using test prefix and not suffix

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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cenkalti/backoff/v4"
	pebblesst "github.com/cockroachdb/pebble/sstable"
	"github.com/go-openapi/swag"
	"github.com/hashicorp/go-multierror"
	"github.com/rs/xid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler/sstable"
	"github.com/treeverse/lakefs/pkg/logging"
)

type ArrayFlags []string

type HookResponse struct {
	Path        string
	Err         error
	Data        []byte
	QueryParams map[string][]string
}

var (
	logger      logging.Logger
	client      apigen.ClientWithResponsesInterface
	endpointURL string
	svc         *s3.Client

	metaClientJarPath  string
	sparkImageTag      string
	repositoriesToKeep ArrayFlags
	groupsToKeep       ArrayFlags
	usersToKeep        ArrayFlags
	policiesToKeep     ArrayFlags
)

const (
	DefaultAdminAccessKeyID     = "AKIAIOSFDNN7EXAMPLEQ"                     //nolint:gosec
	DefaultAdminSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" //nolint:gosec
	AdminUsername               = "esti"

	mainBranch               = "main"
	minHTTPErrorStatusCode   = 400
	ViperStorageNamespaceKey = "storage_namespace"
	ViperBlockstoreType      = "blockstore_type"
)

var (
	nonAlphanumericSequence = regexp.MustCompile("[^a-zA-Z0-9]+")

	errNotVerified     = errors.New("lakeFS failed")
	errWrongStatusCode = errors.New("wrong status code")
	errRunResultsSize  = errors.New("run results size")
)

func (i *ArrayFlags) String() string {
	return strings.Join(*i, " ")
}

func (i *ArrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func EnvCleanup(client apigen.ClientWithResponsesInterface, repositoriesToKeep, groupsToKeep, usersToKeep, policiesToKeep ArrayFlags) error {
	ctx := context.Background()
	errRepos := DeleteAllRepositories(ctx, client, repositoriesToKeep)
	errGroups := DeleteAllGroups(ctx, client, groupsToKeep)
	errPolicies := DeleteAllPolicies(ctx, client, policiesToKeep)
	errUsers := DeleteAllUsers(ctx, client, usersToKeep)
	return multierror.Append(errRepos, errGroups, errPolicies, errUsers).ErrorOrNil()
}

func DeleteAllRepositories(ctx context.Context, client apigen.ClientWithResponsesInterface, repositoriesToKeep ArrayFlags) error {
	// collect repositories to delete
	var (
		repositoriesToDelete []string
		nextOffset           string
	)

	for {
		resp, err := client.ListRepositoriesWithResponse(ctx, &apigen.ListRepositoriesParams{After: apiutil.Ptr(apigen.PaginationAfter(nextOffset))})
		if err != nil {
			return fmt.Errorf("list repositories: %w", err)
		}
		if resp.StatusCode() != http.StatusOK {
			return fmt.Errorf("list repositories: status %s: %w", resp.Status(), errWrongStatusCode)
		}
		for _, repo := range resp.JSON200.Results {
			if !slices.Contains(repositoriesToKeep, repo.Id) {
				repositoriesToDelete = append(repositoriesToDelete, repo.Id)
			}
		}
		if !resp.JSON200.Pagination.HasMore {
			break
		}
		nextOffset = resp.JSON200.Pagination.NextOffset
	}

	var errs *multierror.Error
	for _, id := range repositoriesToDelete {
		resp, err := client.DeleteRepositoryWithResponse(ctx, id, &apigen.DeleteRepositoryParams{})
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("delete repository: %s, err: %w", id, err))
		} else if resp.StatusCode() != http.StatusNoContent {
			errs = multierror.Append(errs, fmt.Errorf("delete repository: %s, status %s: %w", id, resp.Status(), errWrongStatusCode))
		}
	}

	return errs.ErrorOrNil()
}

func DeleteAllGroups(ctx context.Context, client apigen.ClientWithResponsesInterface, groupsToKeep ArrayFlags) error {
	// list groups to delete
	var (
		groupsToDelete []string
		nextOffset     string
	)
	for {
		resp, err := client.ListGroupsWithResponse(ctx, &apigen.ListGroupsParams{After: apiutil.Ptr(apigen.PaginationAfter(nextOffset))})
		if err != nil {
			return fmt.Errorf("list groups: %w", err)
		}
		if resp.StatusCode() != http.StatusOK {
			return fmt.Errorf("list groups: status %s: %w", resp.Status(), errWrongStatusCode)
		}
		for _, group := range resp.JSON200.Results {
			if !slices.Contains(groupsToKeep, swag.StringValue(group.Name)) {
				groupsToDelete = append(groupsToDelete, group.Id)
			}
		}
		if !resp.JSON200.Pagination.HasMore {
			break
		}
		nextOffset = resp.JSON200.Pagination.NextOffset
	}

	// delete groups
	var errs *multierror.Error
	for _, id := range groupsToDelete {
		resp, err := client.DeleteGroupWithResponse(ctx, id)
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("delete group: %s, err: %w", id, err))
		} else if resp.StatusCode() != http.StatusNoContent {
			errs = multierror.Append(errs, fmt.Errorf("delete group: %s, status %s: %w", id, resp.Status(), errWrongStatusCode))
		}
	}
	return errs.ErrorOrNil()
}

func DeleteAllUsers(ctx context.Context, client apigen.ClientWithResponsesInterface, usersToKeep ArrayFlags) error {
	// collect users to delete
	var (
		usersToDelete []string
		nextOffset    string
	)
	for {
		resp, err := client.ListUsersWithResponse(ctx, &apigen.ListUsersParams{After: apiutil.Ptr(apigen.PaginationAfter(nextOffset))})
		if err != nil {
			return fmt.Errorf("list users: %w", err)
		}
		if resp.JSON200 == nil {
			return fmt.Errorf("list users, status %s: %w", resp.Status(), errWrongStatusCode)
		}
		for _, user := range resp.JSON200.Results {
			if !slices.Contains(usersToKeep, user.Id) {
				usersToKeep = usersToDelete
				usersToKeep = append(usersToKeep, user.Id)
			}
		}
		if !resp.JSON200.Pagination.HasMore {
			break
		}
		nextOffset = resp.JSON200.Pagination.NextOffset
	}

	// delete users
	var errs *multierror.Error
	for _, id := range usersToDelete {
		resp, err := client.DeleteUserWithResponse(ctx, id)
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("delete user %s: %w", id, err))
		} else if resp.StatusCode() != http.StatusNoContent {
			errs = multierror.Append(errs, fmt.Errorf("delete user %s, status %s: %w", id, resp.Status(), errWrongStatusCode))
		}
	}
	return errs.ErrorOrNil()
}

func DeleteAllPolicies(ctx context.Context, client apigen.ClientWithResponsesInterface, policiesToKeep ArrayFlags) error {
	// list policies to delete
	var (
		policiesToDelete []string
		nextOffset       string
	)
	for {
		resp, err := client.ListPoliciesWithResponse(ctx, &apigen.ListPoliciesParams{After: apiutil.Ptr(apigen.PaginationAfter(nextOffset))})
		if err != nil {
			return fmt.Errorf("list policies: %w", err)
		}
		if resp.JSON200 == nil {
			return fmt.Errorf("list policies, status %s: %w", resp.Status(), errWrongStatusCode)
		}
		for _, policy := range resp.JSON200.Results {
			if !slices.Contains(policiesToKeep, policy.Id) {
				policiesToDelete = append(policiesToDelete, policy.Id)
			}
		}
		if !resp.JSON200.Pagination.HasMore {
			break
		}
		nextOffset = resp.JSON200.Pagination.NextOffset
	}

	// delete policies
	var errs *multierror.Error
	for _, id := range policiesToDelete {
		resp, err := client.DeletePolicyWithResponse(ctx, id)
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("delete policy %s: %w", id, err))
		} else if resp.StatusCode() != http.StatusNoContent {
			errs = multierror.Append(errs, fmt.Errorf("delete policy %s, status %s: %w", id, resp.Status(), errWrongStatusCode))
		}
	}
	return errs.ErrorOrNil()
}

// CleanupUser - A helper function to remove created users during test
func CleanupUser(t testing.TB, ctx context.Context, client apigen.ClientWithResponsesInterface, userName string) {
	getResp, err := client.GetUserWithResponse(ctx, userName)
	require.NoError(t, err)
	if getResp.StatusCode() == http.StatusNotFound { // skip if already deleted
		return
	}
	require.NotNil(t, getResp.JSON200)
	resp, err := client.DeleteUserWithResponse(ctx, userName)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode())
}

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

// VerifyResponse returns an error based on failed if resp failed to perform action.  It uses
// body in errors.
func VerifyResponse(resp *http.Response, body []byte) error {
	if resp.StatusCode >= minHTTPErrorStatusCode {
		return fmt.Errorf("%w: got %d %s: %s", errNotVerified, resp.StatusCode, resp.Status, string(body))
	}
	return nil
}

// MakeRepositoryName changes name to make it an acceptable repository name by replacing all
// non-alphanumeric characters with a `-`.
func MakeRepositoryName(name string) string {
	return nonAlphanumericSequence.ReplaceAllString(name, "-")
}

func setupTest(t testing.TB) (context.Context, logging.Logger, string) {
	ctx := context.Background()
	name := MakeRepositoryName(t.Name())
	log := logger.WithField("testName", name)
	repo := createRepositoryUnique(ctx, t)
	log.WithField("repo", repo).Info("Created repository")
	return ctx, log, repo
}

func tearDownTest(repoName string) {
	ctx := context.Background()
	DeleteRepositoryIfAskedTo(ctx, repoName)
}

func createRepositoryByName(ctx context.Context, t testing.TB, name string) string {
	storageNamespace := GenerateUniqueStorageNamespace(name)
	name = MakeRepositoryName(name)
	createRepository(ctx, t, name, storageNamespace, false)
	return name
}

func createReadOnlyRepositoryByName(ctx context.Context, t testing.TB, name string) string {
	storageNamespace := GenerateUniqueStorageNamespace(name)
	repoName := GenerateUniqueRepositoryName()
	createRepository(ctx, t, repoName, storageNamespace, true)
	return repoName
}

func createRepositoryUnique(ctx context.Context, t testing.TB) string {
	name := GenerateUniqueRepositoryName()
	return createRepositoryByName(ctx, t, name)
}

func GenerateUniqueRepositoryName() string {
	return "repo-" + xid.New().String()
}

func GenerateUniqueStorageNamespace(repoName string) string {
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
	require.NoErrorf(t, VerifyResponse(resp.HTTPResponse, resp.Body),
		"create repository '%s', storage '%s'", name, repoStorage)
}

func DeleteRepositoryIfAskedTo(ctx context.Context, repositoryName string) {
	deleteRepositories := viper.GetBool("delete_repositories")
	if deleteRepositories {
		resp, err := client.DeleteRepositoryWithResponse(ctx, repositoryName, &apigen.DeleteRepositoryParams{Force: swag.Bool(true)})
		switch {
		case err != nil:
			logger.WithError(err).WithField("repo", repositoryName).Error("Request to delete repository failed")
		case resp.StatusCode() != http.StatusNoContent:
			logger.WithFields(logging.Fields{"repo": repositoryName, "status_code": resp.StatusCode()}).Error("Request to delete repository failed")
		default:
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

func UploadFileRandomDataAndReport(ctx context.Context, repo, branch, objPath string, direct bool, clt apigen.ClientWithResponsesInterface) (checksum, content string, err error) {
	objContent := randstr.String(randomDataContentLength)
	checksum, err = UploadFileAndReport(ctx, repo, branch, objPath, objContent, direct, clt)
	if err != nil {
		return "", "", err
	}
	return checksum, objContent, nil
}

func UploadFileAndReport(ctx context.Context, repo, branch, objPath, objContent string, direct bool, clt apigen.ClientWithResponsesInterface) (checksum string, err error) {
	// Upload using direct access
	if direct {
		stats, err := uploadContentDirect(ctx, client, repo, branch, objPath, nil, "", strings.NewReader(objContent))
		if err != nil {
			return "", err
		}
		return stats.Checksum, nil
	}
	// Upload using API
	resp, err := UploadContent(ctx, repo, branch, objPath, objContent, clt)
	if err != nil {
		return "", err
	}
	if err := VerifyResponse(resp.HTTPResponse, resp.Body); err != nil {
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

func UploadContent(ctx context.Context, repo, branch, objPath, objContent string, clt apigen.ClientWithResponsesInterface) (*apigen.UploadObjectResponse, error) {
	if clt == nil {
		clt = client
	}
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

	return clt.UploadObjectWithBodyWithResponse(ctx, repo, branch, &apigen.UploadObjectParams{
		Path: objPath,
	}, w.FormDataContentType(), &b)
}

func UploadFileRandomData(ctx context.Context, t *testing.T, repo, branch, objPath string, clt apigen.ClientWithResponsesInterface) (checksum, content string) {
	checksum, content, err := UploadFileRandomDataAndReport(ctx, repo, branch, objPath, false, clt)
	require.NoError(t, err, "failed to upload file", repo, branch, objPath)
	return checksum, content
}

func ListRepositoryObjects(ctx context.Context, t *testing.T, repository string, ref string, clt apigen.ClientWithResponsesInterface) []apigen.ObjectStats {
	t.Helper()
	const amount = 5
	var entries []apigen.ObjectStats
	var after string
	for {
		resp, err := clt.ListObjectsWithResponse(ctx, repository, ref, &apigen.ListObjectsParams{
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(amount)),
		})
		require.NoError(t, err, "listing objects")
		require.NoErrorf(t, VerifyResponse(resp.HTTPResponse, resp.Body),
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
		require.NoErrorf(t, VerifyResponse(resp.HTTPResponse, resp.Body),
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

// isBlockstoreType returns nil if the blockstore type is one of requiredTypes, or the actual
// type of the blockstore.
func isBlockstoreType(requiredTypes ...string) *string {
	blockstoreType := viper.GetString(config.BlockstoreTypeKey)
	if slices.Contains(requiredTypes, blockstoreType) {
		return nil
	}
	return &blockstoreType
}

// RequireBlockstoreType Skips test if blockstore type doesn't match the required type
func RequireBlockstoreType(t testing.TB, requiredTypes ...string) {
	if blockstoreType := isBlockstoreType(requiredTypes...); blockstoreType != nil {
		t.Skipf("Required blockstore types: %v, got: %s", requiredTypes, *blockstoreType)
	}
}

func isBasicAuth(t testing.TB, ctx context.Context) bool {
	t.Helper()
	return getRBACState(t, ctx) == "none"
}

func isAdvancedAuth(t testing.TB, ctx context.Context) bool {
	return slices.Contains([]string{"external", "internal"}, getRBACState(t, ctx))
}

func getRBACState(t testing.TB, ctx context.Context) string {
	setupState := getServerConfig(t, ctx)
	return swag.StringValue(setupState.LoginConfig.RBAC)
}

func getServerConfig(t testing.TB, ctx context.Context) *apigen.SetupState {
	t.Helper()
	resp, err := client.GetSetupStateWithResponse(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	return resp.JSON200
}

func GravelerIterator(data []byte) (*sstable.Iterator, error) {
	// read file descriptor
	reader, err := pebblesst.NewMemReader(data, pebblesst.ReaderOptions{})
	if err != nil {
		return nil, err
	}

	// create an iterator over the whole thing
	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}

	// wrap it in a Graveler iterator
	dummyDeref := func() error { return nil }
	return sstable.NewIterator(iter, dummyDeref), nil
}

func WaitForListRepositoryRunsLen(ctx context.Context, t *testing.T, repo, ref string, l int, clt apigen.ClientWithResponsesInterface) *apigen.ActionRunList {
	const MaxIntervalSecs = 5
	const MaxElapsedSecs = 30

	if clt == nil {
		clt = client
	}
	var runs *apigen.ActionRunList
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = MaxIntervalSecs * time.Second
	bo.MaxElapsedTime = MaxElapsedSecs * time.Second
	listFunc := func() error {
		runsResp, err := clt.ListRepositoryRunsWithResponse(ctx, repo, &apigen.ListRepositoryRunsParams{
			Commit: apiutil.Ptr(ref),
		})
		require.NoError(t, err)
		runs = runsResp.JSON200
		require.NotNil(t, runs)
		if len(runs.Results) == l {
			return nil
		}
		return fmt.Errorf("size: %d: expected: %d, %w", len(runs.Results), l, errRunResultsSize)
	}
	err := backoff.Retry(listFunc, bo)
	require.NoError(t, err)
	return runs
}

// ResponseWithTimeout wait for webhook response
func ResponseWithTimeout(s *WebhookServer, timeout time.Duration) (*HookResponse, error) {
	select {
	case res := <-s.respCh:
		return &res, nil
	case <-time.After(timeout):
		return nil, ErrWebhookTimeout
	}
}

// CheckFilesWereGarbageCollected checks that the actual list of presigned URLs matches the expected list in terms of existence status
func CheckFilesWereGarbageCollected(t *testing.T, expectedExisting map[string]bool, presignedURLs map[string]string) {
	for file, expected := range expectedExisting {
		checkFileWasGarbageCollected(t, presignedURLs, file, expected)
	}
}

func checkFileWasGarbageCollected(t *testing.T, presignedURLs map[string]string, file string, expected bool) {
	r, err := http.Get(presignedURLs[file])
	if err != nil {
		t.Fatalf("%s, expected no error, got err=%s", "Http request to presigned url", err)
	}
	defer func() {
		if err := r.Body.Close(); err != nil {
			t.Logf("Failed to close response body: %v", err)
		}
	}()
	if r.StatusCode > 299 && r.StatusCode != 404 {
		t.Fatalf("Unexpected status code in http request: %d", r.StatusCode)
	}
	if r.StatusCode >= 200 && r.StatusCode <= 299 && !expected {
		t.Fatalf("Didn't expect %s to exist, but it did", file)
	}
	if r.StatusCode == 404 && expected {
		t.Fatalf("Expected %s to exist, but it didn't", file)
	}
}
