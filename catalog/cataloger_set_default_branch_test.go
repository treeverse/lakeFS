package catalog

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func Test_cataloger_SetDefaultBranch(t *testing.T) {
	const masterBranch = "master"
	const newBranch = "new"

	ctx := context.Background()
	c := testCataloger(t, WithCacheEnabled(false))
	defer func() { _ = c.Close() }()

	// create repository
	repository := testCatalogerRepo(t, ctx, c, "repository", masterBranch)
	repo, err := c.GetRepository(ctx, repository)
	testutil.MustDo(t, "create repository", err)
	if repo.DefaultBranch != masterBranch {
		t.Fatalf("Default branch=%s, expected=%s", repo.DefaultBranch, masterBranch)
	}

	// new branch and verify it is not default
	_, err = c.CreateBranch(ctx, repository, newBranch, masterBranch)
	testutil.MustDo(t, "create branch", err)
	repo, err = c.GetRepository(ctx, repository)
	testutil.MustDo(t, "get repository", err)
	if repo.DefaultBranch != masterBranch {
		t.Fatalf("Default branch=%s, expected=%s", repo.DefaultBranch, masterBranch)
	}

	// change default branch and verify
	err = c.SetDefaultBranch(ctx, repository, newBranch)
	testutil.MustDo(t, "set new default branch", err)
	repo, err = c.GetRepository(ctx, repository)
	testutil.MustDo(t, "get repository", err)
	if repo.DefaultBranch != newBranch {
		t.Fatalf("Default branch=%s, expected=%s", repo.DefaultBranch, newBranch)
	}
}
