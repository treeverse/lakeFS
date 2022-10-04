package esti

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepositoryBasicOps(t *testing.T) {
	SkipTestIfAskedTo(t)
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
		resp, err := client.DeleteRepositoryWithResponse(ctx, repo)
		require.NoErrorf(t, err, "failed to delete repository %s, storage %s", repo)
		require.Equal(t, http.StatusNoContent, resp.StatusCode())
	}
}
