package nessie

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
)

func TestRepositoryBasicOps(t *testing.T) {
	ctx := context.Background()
	const numOfRepos = 5

	// create repositories
	repos := make([]string, numOfRepos)
	for i := range repos {
		repos[i] = createRepositoryUnique(ctx, t)
	}

	// list repositories - make sure we have the ones we created
	listedRepos := listRepositoriesIDs(t, ctx)
	for _, repo := range repos {
		require.Contains(t, listedRepos, repo, "repository missing in listing")
	}

	// get repo and try creating another repo with the same storage namespace
	testDupStorageNamespace(t, repos[0])

	// delete repositories
	for _, repo := range repos {
		resp, err := client.DeleteRepositoryWithResponse(ctx, repo)
		require.NoErrorf(t, err, "failed to delete repository %s, storage %s", repo)
		require.Equal(t, http.StatusNoContent, resp.StatusCode())
	}
}

func testDupStorageNamespace(t *testing.T, repo string) {
	t.Helper()
	ctx := context.Background()
	respGetRepo, err := client.GetRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "failed to get repo")
	require.Equal(t, http.StatusOK, respGetRepo.StatusCode())

	respUpload, err := uploadContent(ctx, repo, respGetRepo.JSON200.DefaultBranch, "path/to/file", "bla-bla")
	require.NoError(t, err, "failed to upload file")
	require.Equal(t, http.StatusCreated, respUpload.StatusCode())

	respCommit, err := client.CommitWithResponse(ctx, repo, respGetRepo.JSON200.DefaultBranch, api.CommitJSONRequestBody{
		Message: "nessie:repo test",
	})
	require.NoError(t, err, "failed to commit")
	require.Equal(t, http.StatusCreated, respCommit.StatusCode())

	respCreate, err := client.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
		Name:             generateUniqueRepositoryName(),
		StorageNamespace: respGetRepo.JSON200.StorageNamespace,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, respCreate.StatusCode())
	require.Contains(t, respCreate.JSON400.Message, "non-empty storage namespace")
}
