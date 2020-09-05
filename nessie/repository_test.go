package nessie

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
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
	listedRepos := listRepositories(t, ctx)
	for _, repo := range repos {
		require.Contains(t, listedRepos, repo, "repository missing in listing")
	}

	// delete repositories
	for _, repo := range repos {
		_, err := client.Repositories.DeleteRepository(
			repositories.NewDeleteRepositoryParamsWithContext(ctx).
				WithRepository(repo), nil)
		require.NoErrorf(t, err, "failed to delete repository %s, storage %s", repo)
	}
}
