package nessie

import (
	"context"
	"strings"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/swag"
	"github.com/rs/xid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/logging"
)

const (
	masterBranch = "master"
)

func setupTest(t *testing.T) (context.Context, logging.Logger, string) {
	ctx := context.Background()
	logger := logger.WithField("testName", t.Name())
	repo := createRepositoryForTest(ctx, t)
	logger.WithField("repo", repo).Info("Created repository")
	return ctx, logger, repo
}

func createRepositoryForTest(ctx context.Context, t *testing.T) string {
	name := strings.ToLower(t.Name())
	storageNamespace := viper.GetString("storage_namespace")
	repoStorage := storageNamespace + "/" + name
	createRepository(ctx, t, name, repoStorage)
	return name
}

func createRepositoryUnique(ctx context.Context, t *testing.T) string {
	id := xid.New().String()
	name := "repo-" + id
	storage := viper.GetString("storage_namespace") + "/" + id
	createRepository(ctx, t, name, storage)
	return name
}

func createRepository(ctx context.Context, t *testing.T, name string, repoStorage string) {
	logger.WithFields(logging.Fields{
		"repository":        name,
		"storage_namespace": repoStorage,
		"name":              name,
	}).Debug("Create repository for test")
	_, err := client.Repositories.CreateRepository(repositories.NewCreateRepositoryParamsWithContext(ctx).
		WithRepository(&models.RepositoryCreation{
			DefaultBranch:    masterBranch,
			ID:               swag.String(name),
			StorageNamespace: swag.String(repoStorage),
		}), nil)
	require.NoErrorf(t, err, "failed to create repository '%s', storage '%s'", name, repoStorage)
}

func uploadFileRandomData(ctx context.Context, t *testing.T, repo, branch, objPath string) (checksum, content string) {
	const contentLength = 16
	objContent := randstr.Hex(contentLength)
	contentReader := runtime.NamedReader("content", strings.NewReader(objContent))
	stats, err := client.Objects.UploadObject(
		objects.NewUploadObjectParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(branch).
			WithPath(objPath).
			WithContent(contentReader), nil)

	require.NoError(t, err, "failed to upload file")
	return stats.Payload.Checksum, objContent
}

func listRepositoryObjects(ctx context.Context, t *testing.T, repository string, ref string) []*models.ObjectStats {
	const amount = 5
	var entries []*models.ObjectStats
	var after string
	for {
		resp, err := client.Objects.ListObjects(
			objects.NewListObjectsParamsWithContext(ctx).
				WithRepository(repository).
				WithRef(ref).
				WithAfter(swag.String(after)).
				WithAmount(swag.Int64(amount)),
			nil)
		require.NoError(t, err, "listing objects")

		entries = append(entries, resp.Payload.Results...)
		after = resp.Payload.Pagination.NextOffset
		if !swag.BoolValue(resp.Payload.Pagination.HasMore) {
			break
		}
	}
	return entries
}

func listRepositoriesIDs(t *testing.T, ctx context.Context) []string {
	repos := listRepositories(t, ctx)
	ids := make([]string, len(repos))
	for i, repo := range repos {
		ids[i] = repo.ID
	}
	return ids
}

func listRepositories(t *testing.T, ctx context.Context) []*models.Repository {
	var after string
	repoPerPage := swag.Int64(2)
	var listedRepos []*models.Repository
	for {
		listResp, err := client.Repositories.
			ListRepositories(repositories.NewListRepositoriesParamsWithContext(ctx).
				WithAmount(repoPerPage).
				WithAfter(swag.String(after)), nil)
		require.NoError(t, err, "list repositories")
		payload := listResp.Payload
		listedRepos = append(listedRepos, payload.Results...)
		if !swag.BoolValue(payload.Pagination.HasMore) {
			break
		}
		after = payload.Pagination.NextOffset
	}
	return listedRepos
}
