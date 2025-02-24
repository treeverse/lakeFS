package esti

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/logging"
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

	// delete repositories
	for _, repo := range repos {
		resp, err := client.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{})
		require.NoErrorf(t, err, "failed to delete repository %s", repo)
		require.Equal(t, http.StatusNoContent, resp.StatusCode())
	}
}

func TestRepositoryCreateSampleRepo(t *testing.T) {
	ctx := context.Background()
	name := generateUniqueRepositoryName()
	storageNamespace := generateUniqueStorageNamespace(name)
	name = MakeRepositoryName(name)
	logger.WithFields(logging.Fields{
		"repository":        name,
		"storage_namespace": storageNamespace,
		"name":              name,
	}).Debug("Create repository for test")
	resp, err := client.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
		DefaultBranch:    apiutil.Ptr(mainBranch),
		Name:             name,
		StorageNamespace: storageNamespace,
		SampleData:       swag.Bool(true),
	})
	require.NoErrorf(t, err, "failed to create repository '%s', storage '%s'", name, storageNamespace)
	require.NoErrorf(t, verifyResponse(resp.HTTPResponse, resp.Body),
		"create repository '%s', storage '%s'", name, storageNamespace)
	_, err = client.GetRepositoryWithResponse(ctx, name)
	require.NoErrorf(t, err, "failed to get repository '%s'", name)
	listResp, err := client.ListObjectsWithResponse(ctx, name, mainBranch, &apigen.ListObjectsParams{})
	require.NoErrorf(t, err, "failed to list objects in repository '%s'", name)
	require.NotEmptyf(t, listResp.JSON200.Results, "repository '%s' has no objects in main branch", name)
}
