package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/auth"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/api/gen/client/retention"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/testutil"
	"github.com/treeverse/lakefs/upload"
)

func TestHandler_ListRepositoriesHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("list no repos", func(t *testing.T) {
		resp, err := clt.Repositories.ListRepositories(&repositories.ListRepositoriesParams{},
			httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey))

		if err != nil {
			t.Fatal(err)
		}

		if resp.GetPayload() == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.GetPayload().Results) != 0 {
			t.Fatalf("expected 0 repositories, got %d", len(resp.GetPayload().Results))
		}
	})

	t.Run("list some repos", func(t *testing.T) {
		// write some repos
		ctx := context.Background()
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "foo1", "s3://foo1", "master"))
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "foo2", "s3://foo1", "master"))
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "foo3", "s3://foo1", "master"))

		resp, err := clt.Repositories.ListRepositories(&repositories.ListRepositoriesParams{},
			httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey))

		if err != nil {
			t.Fatal(err)
		}

		if resp.GetPayload() == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.GetPayload().Results) != 3 {
			t.Fatalf("expected 3 repositories, got %d", len(resp.GetPayload().Results))
		}
	})

	t.Run("paginate repos", func(t *testing.T) {
		// write some repos
		resp, err := clt.Repositories.ListRepositories(&repositories.ListRepositoriesParams{
			Amount: swag.Int64(2),
		}, httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey))

		if err != nil {
			t.Fatal(err)
		}

		if resp.GetPayload() == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.GetPayload().Results) != 2 {
			t.Fatalf("expected 3 repositories, got %d", len(resp.GetPayload().Results))
		}

		if !swag.BoolValue(resp.GetPayload().Pagination.HasMore) {
			t.Fatalf("expected more results from paginator, got none")
		}
	})

	t.Run("paginate repos after", func(t *testing.T) {
		// write some repos
		resp, err := clt.Repositories.ListRepositories(&repositories.ListRepositoriesParams{
			Amount: swag.Int64(2),
			After:  swag.String("foo2"),
		}, httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey))

		if err != nil {
			t.Fatal(err)
		}

		if resp.GetPayload() == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.GetPayload().Results) != 1 {
			t.Fatalf("expected 1 repository, got %d", len(resp.GetPayload().Results))
		}

		if swag.BoolValue(resp.GetPayload().Pagination.HasMore) {
			t.Fatalf("expected no more results from paginator")
		}

		if !strings.EqualFold(resp.GetPayload().Results[0].ID, "foo3") {
			t.Fatalf("expected last pagination result to be foo3, got %s instead",
				resp.GetPayload().Results[0].ID)
		}
	})
}

func TestHandler_GetRepoHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("get missing repo", func(t *testing.T) {
		_, err := clt.Repositories.GetRepository(&repositories.GetRepositoryParams{
			Repository: "foo1",
		}, httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey))

		if err == nil {
			t.Fatalf("expected err calling missing repo")
		}

		if _, ok := err.(*repositories.GetRepositoryNotFound); !ok {
			t.Fatalf("expected not found error getting missing repo")
		}
	})

	t.Run("get existing repo", func(t *testing.T) {
		const testBranchName = "non-default"
		testutil.Must(t,
			deps.cataloger.CreateRepository(context.Background(), "foo1", "s3://foo1", testBranchName))
		resp, err := clt.Repositories.GetRepository(&repositories.GetRepositoryParams{
			Repository: "foo1",
		}, httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey))

		if err != nil {
			t.Fatalf("unexpected err calling get repo, %v", err)
		}

		if !strings.EqualFold(resp.GetPayload().DefaultBranch, testBranchName) {
			t.Fatalf("unexpected branch name %s, expected %s",
				resp.GetPayload().DefaultBranch, testBranchName)
		}
	})
}

func TestHandler_CommitsGetBranchCommitLogHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	ctx := context.Background()
	t.Run("get missing branch", func(t *testing.T) {
		err := deps.cataloger.CreateRepository(ctx, "repo1", "ns1", "master")
		if err != nil {
			t.Fatal(err)
		}
		_, err = clt.Commits.GetBranchCommitLog(&commits.GetBranchCommitLogParams{
			Branch:     "otherbranch",
			Repository: "repo1",
		}, bauth)
		if err == nil {
			t.Fatalf("expected error getting a branch that doesn't exist")
		}
	})

	t.Run("get branch log", func(t *testing.T) {
		err := deps.cataloger.CreateRepository(ctx, "repo2", "ns1", "master")
		if err != nil {
			t.Fatal(err)
		}
		const commitsLen = 2
		for i := 0; i < commitsLen; i++ {
			n := strconv.Itoa(i + 1)
			p := "foo/bar" + n
			testutil.MustDo(t, "create entry bar"+n, deps.cataloger.CreateEntry(ctx, "repo2", "master",
				catalog.Entry{Path: p, PhysicalAddress: "bar" + n + "addr", CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n},
				catalog.CreateEntryParams{},
			))
			if _, err := deps.cataloger.Commit(ctx, "repo2", "master", "commit"+n, "some_user", nil); err != nil {
				t.Fatalf("failed to commit '%s': %s", p, err)
			}
		}
		resp, err := clt.Commits.GetBranchCommitLog(&commits.GetBranchCommitLogParams{
			Branch:     "master",
			Repository: "repo2",
		}, bauth)
		if err != nil {
			t.Fatalf("unexpected error getting log of commits: %s", err)
		}
		const expectedCommits = commitsLen + 1 // one for the branch creation
		commitsLog := resp.GetPayload().Results
		if len(commitsLog) != expectedCommits {
			t.Fatalf("Log %d commits, expected %d", len(commitsLog), expectedCommits)
		}
	})
}

func TestHandler_GetCommitHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("get missing commit", func(t *testing.T) {
		_, err := clt.Commits.GetCommit(&commits.GetCommitParams{
			CommitID:   "b0a989d946dca26496b8280ca2bb0a96131a48b362e72f1789e498815992fffa",
			Repository: "foo1",
		}, bauth)
		if err == nil {
			t.Fatalf("expected err calling missing commit")
		}

		if _, ok := err.(*commits.GetCommitNotFound); !ok {
			t.Fatalf("expected not found error getting missing commit")
		}
	})

	t.Run("get existing commit", func(t *testing.T) {
		ctx := context.Background()
		err := deps.cataloger.CreateRepository(ctx, "foo1", "s3://foo1", "master")
		testutil.Must(t, err)
		testutil.MustDo(t, "create entry bar1", deps.cataloger.CreateEntry(ctx, "foo1", "master",
			catalog.Entry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"},
			catalog.CreateEntryParams{},
		))
		commit1, err := deps.cataloger.Commit(ctx, "foo1", "master", "some message", DefaultUserID, nil)
		testutil.Must(t, err)
		reference1, err := deps.cataloger.GetBranchReference(ctx, "foo1", "master")
		if err != nil {
			t.Fatal(err)
		}
		if reference1 != commit1.Reference {
			t.Fatalf("Commit reference %s, not equals to branch reference %s", commit1, reference1)
		}
		resp, err := clt.Commits.GetCommit(&commits.GetCommitParams{
			CommitID:   commit1.Reference,
			Repository: "foo1",
		}, bauth)
		if err != nil {
			t.Fatalf("unexpected err calling commit: %s", err)
		}

		committer := resp.GetPayload().Committer
		if committer != DefaultUserID {
			t.Fatalf("unexpected commit id %s, expected %s",
				committer, DefaultUserID)
		}
	})
}

func TestHandler_CommitHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})
	t.Run("commit non-existent commit", func(t *testing.T) {
		_, err := clt.Commits.Commit(&commits.CommitParams{
			Branch: "master",
			Commit: &models.CommitCreation{
				Message:  swag.String("some message"),
				Metadata: nil,
			},
			Repository: "foo1",
		}, bauth)

		if err == nil {
			t.Fatalf("expected err calling missing repo for commit")
		}

		if _, ok := err.(*commits.CommitDefault); !ok {
			t.Fatalf("expected not found error when missing commit repo, got %v", err)
		}
	})

	t.Run("commit success", func(t *testing.T) {
		ctx := context.Background()
		testutil.MustDo(t, "create repo foo1",
			deps.cataloger.CreateRepository(ctx, "foo1", "s3://foo1", "master"))
		testutil.MustDo(t, "commit bar on foo1", deps.cataloger.CreateEntry(ctx, "foo1", "master",
			catalog.Entry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil},
			catalog.CreateEntryParams{},
		))
		_, err := clt.Commits.Commit(&commits.CommitParams{
			Branch: "master",
			Commit: &models.CommitCreation{
				Message:  swag.String("some message"),
				Metadata: nil,
			},
			Repository: "foo1",
		}, bauth)
		if err != nil {
			t.Fatalf("unexpected error on commit: %s", err)
		}
	})
}

func TestHandler_CreateRepositoryHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("create repo success", func(t *testing.T) {
		resp, err := clt.Repositories.CreateRepository(&repositories.CreateRepositoryParams{
			Repository: &models.RepositoryCreation{
				StorageNamespace: swag.String("s3://foo-bucket/"),
				ID:               swag.String("my-new-repo"),
				DefaultBranch:    "master",
			},
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected error creating valid repo: %s", err)
		}

		if !strings.EqualFold(resp.GetPayload().ID, "my-new-repo") {
			t.Fatalf("got unexpected repo when creating my-new-repo: %s", resp.GetPayload().ID)
		}
	})

	t.Run("create repo duplicate", func(t *testing.T) {
		ctx := context.Background()
		err := deps.cataloger.CreateRepository(ctx, "repo2", "s3://foo1/", "master")
		if err != nil {
			t.Fatal(err)
		}
		_, err = clt.Repositories.CreateRepository(&repositories.CreateRepositoryParams{
			Repository: &models.RepositoryCreation{
				StorageNamespace: swag.String("s3://foo-bucket/"),
				ID:               swag.String("repo2"),
				DefaultBranch:    "master",
			},
		}, bauth)

		if err == nil {
			t.Fatalf("expected error creating duplicate repo")
		}
	})
}

func TestHandler_DeleteRepositoryHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	ctx := context.Background()
	t.Run("delete repo success", func(t *testing.T) {
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "my-new-repo", "s3://foo1/", "master"))

		_, err := clt.Repositories.DeleteRepository(&repositories.DeleteRepositoryParams{
			Repository: "my-new-repo",
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected error deleting repo: %s", err)
		}

		_, err = deps.cataloger.GetRepository(ctx, "my-new-repo")
		if !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected repo to be gone, instead got error: %s", err)
		}
	})

	t.Run("delete repo doesnt exist", func(t *testing.T) {
		_, err := clt.Repositories.DeleteRepository(&repositories.DeleteRepositoryParams{
			Repository: "my-other-repo",
		}, bauth)

		if err == nil {
			t.Fatalf("expected error deleting repo that doesnt exist")
		}
	})

	t.Run("delete repo doesnt delete other repos", func(t *testing.T) {
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "rr0", "s3://foo1", "master"))
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "rr1", "s3://foo1", "master"))
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "rr11", "s3://foo1", "master"))
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "rr2", "s3://foo1", "master"))
		_, err := clt.Repositories.DeleteRepository(&repositories.DeleteRepositoryParams{
			Repository: "rr1",
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected error deleting repo: %s", err)
		}

		_, err = deps.cataloger.GetRepository(ctx, "rr0")
		if err != nil {
			t.Fatalf("unexpected error getting other repo: %s", err)
		}
		_, err = deps.cataloger.GetRepository(ctx, "rr11")
		if err != nil {
			t.Fatalf("unexpected error getting other repo: %s", err)
		}
		_, err = deps.cataloger.GetRepository(ctx, "rr2")
		if err != nil {
			t.Fatalf("unexpected error getting other repo: %s", err)
		}
	})
}

func TestHandler_ListBranchesHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	// setup repository

	t.Run("list branches only default", func(t *testing.T) {
		ctx := context.Background()
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "repo1", "s3://foo1", "master"))
		resp, err := clt.Branches.ListBranches(&branches.ListBranchesParams{
			Amount:     swag.Int64(-1),
			Repository: "repo1",
		}, bauth)
		if err != nil {
			t.Fatalf("unexpected error listing branches: %s", err)
		}
		if len(resp.GetPayload().Results) != 1 {
			t.Fatalf("expected 1 branch, got %d", len(resp.GetPayload().Results))
		}
	})

	t.Run("list branches pagination", func(t *testing.T) {
		ctx := context.Background()
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "repo2", "s3://foo2", "master"))
		for i := 0; i < 7; i++ {
			branchName := "master" + strconv.Itoa(i+1)
			_, err := deps.cataloger.CreateBranch(ctx, "repo2", branchName, "master")
			testutil.MustDo(t, "create branch "+branchName, err)
		}
		resp, err := clt.Branches.ListBranches(&branches.ListBranchesParams{
			Amount:     swag.Int64(2),
			Repository: "repo2",
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetPayload().Results) != 2 {
			t.Fatalf("expected 2 branches to return, got %d", len(resp.GetPayload().Results))
		}

		resp, err = clt.Branches.ListBranches(&branches.ListBranchesParams{
			Amount:     swag.Int64(2),
			After:      swag.String("master1"),
			Repository: "repo2",
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}
		results := resp.GetPayload().Results
		if len(results) != 2 {
			t.Fatalf("expected 2 branches to return, got %d", len(results))
		}
		const expectedReference = "master2"
		retReference := results[0]
		if retReference != expectedReference {
			t.Fatalf("expected '%s' as the first result for the second page, got '%s' instead",
				expectedReference, retReference)
		}
	})

	t.Run("list branches repo doesnt exist", func(t *testing.T) {
		_, err := clt.Branches.ListBranches(&branches.ListBranchesParams{
			Amount:     swag.Int64(2),
			Repository: "repoX",
		}, bauth)
		if err == nil {
			t.Fatal("expected error calling list branches on repo that doesnt exist")
		}
	})
}

func TestHandler_GetBranchHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("get default branch", func(t *testing.T) {
		ctx := context.Background()
		const testBranch = "master"
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "repo1", "s3://foo1", testBranch))
		resp, err := clt.Branches.GetBranch(&branches.GetBranchParams{
			Branch:     testBranch,
			Repository: "repo1",
		}, bauth)
		if err != nil {
			t.Fatalf("unexpected error getting branch: %s", err)
		}
		reference := resp.GetPayload()
		if reference == "" {
			t.Fatalf("Got no reference for branch '%s'", testBranch)
		}
	})

	t.Run("get missing branch", func(t *testing.T) {
		_, err := clt.Branches.GetBranch(&branches.GetBranchParams{
			Branch:     "master333",
			Repository: "repo1",
		}, bauth)
		if err == nil {
			t.Fatal("expected error getting branch that doesnt exist")
		}
	})

	t.Run("get branch for missing repo", func(t *testing.T) {
		_, err := clt.Branches.GetBranch(&branches.GetBranchParams{
			Branch:     "master",
			Repository: "repo3",
		}, bauth)
		if err == nil {
			t.Fatal("expected error getting branch for repo that doesnt exist")
		}
	})
}

func TestHandler_CreateBranchHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("create branch success", func(t *testing.T) {
		ctx := context.Background()
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "repo1", "s3://foo1", "master"))
		const newBranchName = "master2"
		resp, err := clt.Branches.CreateBranch(&branches.CreateBranchParams{
			Branch: &models.BranchCreation{
				Name:   swag.String(newBranchName),
				Source: swag.String("master"),
			},
			Repository: "repo1",
		}, bauth)
		if err != nil {
			t.Fatalf("unexpected error creating branch: %s", err)
		}
		reference := resp.GetPayload()
		if len(reference) == 0 {
			t.Fatalf("branch %s creation got no reference", newBranchName)
		}
	})

	t.Run("create branch missing commit", func(t *testing.T) {
		_, err := clt.Branches.CreateBranch(&branches.CreateBranchParams{
			Branch: &models.BranchCreation{
				Source: swag.String("a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"),
				Name:   swag.String("master3"),
			},
			Repository: "repo1",
		}, bauth)
		if err == nil {
			t.Fatal("expected error creating branch with a commit that doesnt exist")
		}
	})

	t.Run("create branch missing repo", func(t *testing.T) {
		_, err := clt.Branches.CreateBranch(&branches.CreateBranchParams{
			Branch: &models.BranchCreation{
				Source: swag.String("master"),
				Name:   swag.String("master8"),
			},
			Repository: "repo5",
		}, bauth)
		if err == nil {
			t.Fatal("expected error creating branch with a repo that doesnt exist")
		}
	})
}

func TestHandler_DeleteBranchHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("delete branch success", func(t *testing.T) {
		ctx := context.Background()
		testutil.Must(t, deps.cataloger.CreateRepository(ctx, "my-new-repo", "s3://foo1", "master"))
		_, err := deps.cataloger.CreateBranch(ctx, "my-new-repo", "master2", "master")
		if err != nil {
			t.Fatal(err)
		}

		_, err = clt.Branches.DeleteBranch(&branches.DeleteBranchParams{
			Branch:     "master2",
			Repository: "my-new-repo",
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected error deleting branch: %s", err)
		}

		_, err = deps.cataloger.GetBranchReference(ctx, "my-new-repo", "master2")
		if !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected branch to be gone, instead got error: %s", err)
		}
	})

	t.Run("delete branch doesnt exist", func(t *testing.T) {
		_, err := clt.Branches.DeleteBranch(&branches.DeleteBranchParams{
			Branch:     "master5",
			Repository: "my-new-repo",
		}, bauth)

		if err == nil {
			t.Fatalf("expected error deleting branch that doesnt exist")
		}
	})
}

func TestHandler_ObjectsStatObjectHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	ctx := context.Background()
	err := deps.cataloger.CreateRepository(ctx, "repo1", "ns1", "master")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("get object stats", func(t *testing.T) {
		testutil.Must(t,
			deps.cataloger.CreateEntry(ctx, "repo1", "master", catalog.Entry{
				Path:            "foo/bar",
				PhysicalAddress: "this_is_bars_address",
				CreationDate:    time.Now(),
				Size:            666,
				Checksum:        "this_is_a_checksum",
				Metadata:        nil,
			}, catalog.CreateEntryParams{}))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := clt.Objects.StatObject(&objects.StatObjectParams{
			Ref:        "master",
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth)

		if err != nil {
			t.Fatalf("did not expect error for stat, got %s", err)
		}
		if resp.Payload.Path != "foo/bar" {
			t.Fatalf("expected to get back our path, got %s", resp.Payload.Path)
		}
		if resp.Payload.SizeBytes != 666 {
			t.Fatalf("expected correct size, got %d", resp.Payload.SizeBytes)
		}

		_, err = clt.Objects.StatObject(&objects.StatObjectParams{
			Ref:        "master:HEAD",
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth)

		if _, ok := err.(*objects.StatObjectNotFound); !ok {
			t.Fatalf("did expect object not found for stat, got %v", err)
		}
	})

	t.Run("get expired object stats", func(t *testing.T) {
		testutil.Must(t,
			deps.cataloger.CreateEntry(ctx, "repo1", "master", catalog.Entry{
				Path:            "foo/expired",
				PhysicalAddress: "this_address_is_expired",
				CreationDate:    time.Now(),
				Size:            999999,
				Checksum:        "eeee",
				Metadata:        nil,
				Expired:         true,
			}, catalog.CreateEntryParams{}))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := clt.Objects.StatObject(&objects.StatObjectParams{
			Ref:        "master",
			Path:       "foo/expired",
			Repository: "repo1",
		}, bauth)

		gone, ok := err.(*objects.StatObjectGone)
		if !ok {
			t.Fatalf("expected StatObjectGone error but got %#v (response %v)", err, resp)
		}
		if gone.Payload.Path != "foo/expired" {
			t.Fatalf("expected to get back our path, got %s", gone.Payload.Path)
		}
		if gone.Payload.SizeBytes != 999999 {
			t.Fatalf("expected correct size, got %d", gone.Payload.SizeBytes)
		}
	})
}

func TestHandler_ObjectsListObjectsHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	basicAuth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})
	ctx := context.Background()
	testutil.Must(t,
		deps.cataloger.CreateRepository(ctx, "repo1", "ns1", "master"))
	testutil.Must(t,
		deps.cataloger.CreateEntry(ctx, "repo1", "master", catalog.Entry{
			Path:            "foo/bar",
			PhysicalAddress: "this_is_bars_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "this_is_a_checksum",
		}, catalog.CreateEntryParams{}))
	testutil.Must(t,
		deps.cataloger.CreateEntry(ctx, "repo1", "master", catalog.Entry{
			Path:            "foo/quuux",
			PhysicalAddress: "this_is_quuxs_address_expired",
			CreationDate:    time.Now(),
			Size:            9999999,
			Checksum:        "quux_checksum",
			Expired:         true,
		}, catalog.CreateEntryParams{}))
	testutil.Must(t,
		deps.cataloger.CreateEntry(ctx, "repo1", "master", catalog.Entry{
			Path:            "foo/baz",
			PhysicalAddress: "this_is_bazs_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "baz_checksum",
		}, catalog.CreateEntryParams{}))
	testutil.Must(t,
		deps.cataloger.CreateEntry(ctx, "repo1", "master", catalog.Entry{
			Path:            "foo/a_dir/baz",
			PhysicalAddress: "this_is_bazs_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "baz_checksum",
		}, catalog.CreateEntryParams{}))

	t.Run("get object list", func(t *testing.T) {
		resp, err := clt.Objects.ListObjects(&objects.ListObjectsParams{
			Ref:        "master",
			Repository: "repo1",
			Prefix:     swag.String("foo/"),
		}, basicAuth)
		if err != nil {
			t.Fatal(err)
		}

		if len(resp.Payload.Results) != 4 {
			t.Fatalf("expected 4 entries, got back %d", len(resp.Payload.Results))
		}

		resp, err = clt.Objects.ListObjects(&objects.ListObjectsParams{
			Ref:        "master:HEAD",
			Repository: "repo1",
			Prefix:     swag.String("/"),
		}, basicAuth)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Payload.Results) != 0 {
			t.Fatalf("expected no entries, got back %d", len(resp.Payload.Results))
		}
	})

	t.Run("get object list paginated", func(t *testing.T) {
		resp, err := clt.Objects.ListObjects(&objects.ListObjectsParams{
			Amount:     swag.Int64(2),
			Ref:        "master",
			Repository: "repo1",
			Prefix:     swag.String("foo/"),
		}, basicAuth)
		if err != nil {
			t.Fatal(err)
		}

		if len(resp.Payload.Results) != 2 {
			t.Fatalf("expected 3 entries, got back %d", len(resp.Payload.Results))
		}
		if !swag.BoolValue(resp.Payload.Pagination.HasMore) {
			t.Fatalf("expected paginator.HasMore to be true")
		}

		if resp.Payload.Pagination.NextOffset != "foo/bar" {
			t.Fatalf("expected next offset to be foo/bar, got %s", resp.Payload.Pagination.NextOffset)
		}
	})
}

func TestHandler_ObjectsGetObjectHandler(t *testing.T) {
	handler, deps := getHandler(t)

	ctx := context.Background()
	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})
	err := deps.cataloger.CreateRepository(ctx, "repo1", "ns1", "master")
	if err != nil {
		t.Fatal(err)
	}

	expensiveString := "EXPENSIVE"

	buf := new(bytes.Buffer)
	buf.WriteString("this is file content made up of bytes")
	blob, err := upload.WriteBlob(deps.blocks, "ns1", buf, 37, block.PutOpts{StorageClass: &expensiveString})
	if err != nil {
		t.Fatal(err)
	}
	entry := catalog.Entry{
		Path:            "foo/bar",
		PhysicalAddress: blob.PhysicalAddress,
		CreationDate:    time.Now(),
		Size:            blob.Size,
		Checksum:        blob.Checksum,
	}
	testutil.Must(t,
		deps.cataloger.CreateEntry(ctx, "repo1", "master", entry, catalog.CreateEntryParams{}))

	expired := catalog.Entry{
		Path:            "foo/expired",
		PhysicalAddress: "an_expired_physical_address",
		CreationDate:    time.Now(),
		Size:            99999,
		Checksum:        "b10b",
		Expired:         true,
	}
	testutil.Must(t,
		deps.cataloger.CreateEntry(ctx, "repo1", "master", expired, catalog.CreateEntryParams{}))

	t.Run("get object", func(t *testing.T) {
		buf := new(bytes.Buffer)
		resp, err := clt.Objects.GetObject(&objects.GetObjectParams{
			Ref:        "master",
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth, buf)
		if err != nil {
			t.Fatal(err)
		}

		if resp.ContentLength != 37 {
			t.Fatalf("expected 37 bytes in content length, got back %d", resp.ContentLength)
		}
		if resp.ETag != `"3c4838fe975c762ee97cf39fbbe566f1"` {
			t.Fatalf("got unexpected etag: %s", resp.ETag)
		}

		body := buf.String()
		if !strings.EqualFold(body, "this is file content made up of bytes") {
			t.Fatalf("got unexpected body: '%s'", body)
		}

		_, err = clt.Objects.GetObject(&objects.GetObjectParams{
			Ref:        "master:HEAD",
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth, buf)
		if _, ok := err.(*objects.GetObjectNotFound); !ok {
			t.Fatalf("expected object not found error, got %v", err)
		}
	})

	t.Run("get properties", func(t *testing.T) {
		properties, err := clt.Objects.GetUnderlyingProperties(&objects.GetUnderlyingPropertiesParams{
			Ref:        "master",
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth)
		if err != nil {
			t.Fatalf("expected to get underlying properties, got %v", err)
		}
		if *properties.Payload.StorageClass != expensiveString {
			t.Errorf("expected to get \"%s\" storage class, got %#v", expensiveString, properties)
		}
	})

	t.Run("expired", func(t *testing.T) {
		buf := new(bytes.Buffer)
		_, err := clt.Objects.GetObject(&objects.GetObjectParams{
			Ref:        "master",
			Path:       "foo/expired",
			Repository: "repo1",
		}, bauth, buf)
		if err == nil {
			t.Errorf("expected an error, got none\n\t%s", buf.String())
		} else if !strings.Contains(err.Error(), "resource expired") {
			t.Errorf("expected \"resource expired\" error, got %s\n\t%s", err, buf.String())
		}
	})
}

func TestHandler_ObjectsUploadObjectHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})
	ctx := context.Background()
	err := deps.cataloger.CreateRepository(ctx, "repo1", "ns1", "master")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("upload object", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteString("hello world this is my awesome content")
		resp, err := clt.Objects.UploadObject(&objects.UploadObjectParams{
			Branch:     "master",
			Content:    runtime.NamedReader("content", buf),
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}

		if resp.Payload.SizeBytes != 38 {
			t.Fatalf("expected 38 bytes to be written, got back %d", resp.Payload.SizeBytes)
		}

		// download it
		rbuf := new(bytes.Buffer)
		rresp, err := clt.Objects.GetObject(&objects.GetObjectParams{
			Ref:        "master",
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth, rbuf)
		if err != nil {
			t.Fatal(err)
		}
		result := rbuf.String()
		if len(result) != 38 {
			t.Fatalf("expected 38 bytes to be read, got back %d", len(result))
		}
		if !strings.EqualFold(rresp.ETag, httputil.ETag(resp.Payload.Checksum)) {
			t.Fatalf("got unexpected etag: %s - expected %s", rresp.ETag, httputil.ETag(resp.Payload.Checksum))
		}
	})

	t.Run("upload object missing branch", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteString("hello world this is my awesome content")
		_, err := clt.Objects.UploadObject(&objects.UploadObjectParams{
			Branch:     "masterX",
			Content:    runtime.NamedReader("content", buf),
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth)
		if _, ok := err.(*objects.UploadObjectNotFound); !ok {
			t.Fatal("Missing branch should return not found")
		}
	})

	t.Run("upload objects dedup", func(t *testing.T) {
		t.Skip("api implements async dedup - consider removing the test code")
		const content = "They do not love that do not show their love"
		resp1, err := clt.Objects.UploadObject(&objects.UploadObjectParams{
			Branch:     "master",
			Content:    runtime.NamedReader("content", strings.NewReader(content)),
			Path:       "dd/bar1",
			Repository: "repo1",
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}

		resp2, err := clt.Objects.UploadObject(&objects.UploadObjectParams{
			Branch:     "master",
			Content:    runtime.NamedReader("content", strings.NewReader(content)),
			Path:       "dd/bar2",
			Repository: "repo1",
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}

		ent1, err := deps.cataloger.GetEntry(ctx, "repo1", "master", resp1.Payload.Path, catalog.GetEntryParams{})
		testutil.MustDo(t, "get first entry", err)
		ent2, err := deps.cataloger.GetEntry(ctx, "repo1", "master", resp2.Payload.Path, catalog.GetEntryParams{})
		testutil.MustDo(t, "get second entry", err)
		if ent1.PhysicalAddress != ent2.PhysicalAddress {
			t.Fatalf("First entry address '%s' should match the second '%s' - check dedup",
				ent1.PhysicalAddress, ent2.PhysicalAddress)
		}
	})
}

func TestHandler_ObjectsDeleteObjectHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})
	ctx := context.Background()
	err := deps.cataloger.CreateRepository(ctx, "repo1", "ns1", "master")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("delete object", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteString("hello world this is my awesome content")
		resp, err := clt.Objects.UploadObject(&objects.UploadObjectParams{
			Branch:     "master",
			Content:    runtime.NamedReader("content", buf),
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}

		if resp.Payload.SizeBytes != 38 {
			t.Fatalf("expected 38 bytes to be written, got back %d", resp.Payload.SizeBytes)
		}

		// download it
		rbuf := new(bytes.Buffer)
		rresp, err := clt.Objects.GetObject(&objects.GetObjectParams{
			Ref:        "master",
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth, rbuf)
		if err != nil {
			t.Fatal(err)
		}
		result := rbuf.String()
		if len(result) != 38 {
			t.Fatalf("expected 38 bytes to be read, got back %d", len(result))
		}
		if !strings.EqualFold(rresp.ETag, httputil.ETag(resp.Payload.Checksum)) {
			t.Fatalf("got unexpected etag: %s - expected %s", rresp.ETag, httputil.ETag(resp.Payload.Checksum))
		}

		// delete it
		_, err = clt.Objects.DeleteObject(&objects.DeleteObjectParams{
			Branch:     "master",
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}

		// get it
		_, err = clt.Objects.StatObject(&objects.StatObjectParams{
			Ref:        "master",
			Path:       "foo/bar",
			Repository: "repo1",
		}, bauth)
		if err == nil {
			t.Fatalf("expected file to be gone now")
		}
	})
}

func TestController_CreatePolicyHandler(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("valid_policy", func(t *testing.T) {
		ctx := context.Background()
		_, err := clt.Auth.CreatePolicy(&auth.CreatePolicyParams{
			Policy: &models.Policy{
				CreationDate: time.Now().Unix(),
				ID:           swag.String("ValidPolicyID"),
				Statement: []*models.Statement{
					{
						Action:   []string{"fs:ReadObject"},
						Effect:   swag.String("allow"),
						Resource: swag.String("arn:lakefs:fs:::repository/foo/object/*"),
					},
				},
			},
			Context: ctx,
		}, bauth)
		if err != nil {
			t.Fatalf("unexpected error creating valid policy: %v", err)
		}
	})

	t.Run("invalid_policy_action", func(t *testing.T) {
		ctx := context.Background()
		_, err := clt.Auth.CreatePolicy(&auth.CreatePolicyParams{
			Policy: &models.Policy{
				CreationDate: time.Now().Unix(),
				ID:           swag.String("ValidPolicyID"),
				Statement: []*models.Statement{
					{
						Action:   []string{"fsx:ReadObject"},
						Effect:   swag.String("allow"),
						Resource: swag.String("arn:lakefs:fs:::repository/foo/object/*"),
					},
				},
			},
			Context: ctx,
		}, bauth)
		if err == nil {
			t.Fatalf("expected error creating invalid policy: action")
		}
	})

	t.Run("invalid_policy_effect", func(t *testing.T) {
		ctx := context.Background()
		_, err := clt.Auth.CreatePolicy(&auth.CreatePolicyParams{
			Policy: &models.Policy{
				CreationDate: time.Now().Unix(),
				ID:           swag.String("ValidPolicyID"),
				Statement: []*models.Statement{
					{
						Action:   []string{"fs:ReadObject"},
						Effect:   swag.String("Allow"),
						Resource: swag.String("arn:lakefs:fs:::repository/foo/object/*"),
					},
				},
			},
			Context: ctx,
		}, bauth)
		if err == nil {
			t.Fatalf("expected error creating invalid policy: effect")
		}
	})

	t.Run("invalid_policy_arn", func(t *testing.T) {
		ctx := context.Background()
		_, err := clt.Auth.CreatePolicy(&auth.CreatePolicyParams{
			Policy: &models.Policy{
				CreationDate: time.Now().Unix(),
				ID:           swag.String("ValidPolicyID"),
				Statement: []*models.Statement{
					{
						Action:   []string{"fs:ReadObject"},
						Effect:   swag.String("Allow"),
						Resource: swag.String("arn:lakefs:fs:repository/foo/object/*"),
					},
				},
			},
			Context: ctx,
		}, bauth)
		if err == nil {
			t.Fatalf("expected error creating invalid policy: arn")
		}
	})

}

func TestHandler_RetentionPolicyHandlers(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.SecretAccessKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})
	ctx := context.Background()
	err := deps.cataloger.CreateRepository(ctx, "repo1", "ns1", "master")
	if err != nil {
		t.Fatal(err)
	}

	statusEnabled := "enabled"
	policy1 := models.RetentionPolicy{
		Description: "retention policy for API handler test",
		Rules: []*models.RetentionPolicyRule{
			{
				Status: &statusEnabled,
				Filter: &models.RetentionPolicyRuleFilter{
					Prefix: "master/logs/",
				},
				Expiration: &models.RetentionPolicyRuleExpiration{
					Uncommitted: &models.TimePeriod{Days: 6, Weeks: 2},
				},
			},
		},
	}

	// TODO(ariels): Verify initial state before any retention policy is set

	t.Run("Initialize a retention policy", func(t *testing.T) {
		_, err := clt.Retention.UpdateRetentionPolicy(&retention.UpdateRetentionPolicyParams{
			Repository: "repo1",
			Policy:     &policy1,
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := clt.Retention.GetRetentionPolicy(&retention.GetRetentionPolicyParams{
			Repository: "repo1",
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}
		got := resp.GetPayload()

		diff := deep.Equal(policy1, got.RetentionPolicy)
		if diff != nil {
			t.Errorf("expected to read back the same policy, got %s", diff)
		}
	})
}

func Test_setupLakeFSHandler(t *testing.T) {
	// get handler with DB without apply the DDL
	handler, deps := getHandler(t, testutil.WithGetDBApplyDDL(false))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	name := "admin"
	user := models.Setup{
		Username: &name,
	}
	req, err := json.Marshal(user)
	if err != nil {
		t.Fatal("JSON marshal request", err)
	}

	reqURI := srv.URL + client.DefaultBasePath + "/setup_lakefs"
	const contentType = "application/json"
	t.Run("fresh start", func(t *testing.T) {
		// request to setup
		res := mustSetup(t, reqURI, contentType, req)
		defer func() {
			_ = res.Body.Close()
		}()

		const expectedStatusCode = http.StatusOK
		if res.StatusCode != expectedStatusCode {
			t.Fatalf("setup request returned %d status, expected %d", res.StatusCode, expectedStatusCode)
		}

		// read response
		var credKeys *models.CredentialsWithSecret

		err = json.NewDecoder(res.Body).Decode(&credKeys)
		if err != nil {
			t.Fatal("Decode response", err)
		}

		if len(credKeys.AccessKeyID) == 0 {
			t.Fatal("Credential key id is missing")
		}

		foundCreds, err := deps.auth.GetCredentials(credKeys.AccessKeyID)
		if err != nil {
			t.Fatal("Get API credentials key id for created access key", err)
		}
		if foundCreds == nil {
			t.Fatal("Get API credentials secret key for created access key")
		}
		if foundCreds.SecretAccessKey != credKeys.SecretAccessKey {
			t.Fatalf("Access secret key '%s', expected '%s'", foundCreds.SecretAccessKey, credKeys.SecretAccessKey)
		}
	})

	// now we ask again - should get status conflict
	t.Run("existing setup", func(t *testing.T) {
		// request to setup
		res := mustSetup(t, reqURI, contentType, req)
		defer func() {
			_ = res.Body.Close()
		}()

		const expectedStatusCode = http.StatusConflict
		if res.StatusCode != expectedStatusCode {
			t.Fatalf("setup request returned %d status, expected %d", res.StatusCode, expectedStatusCode)
		}
	})
}

func mustSetup(t *testing.T, reqURI string, contentType string, req []byte) *http.Response {
	res, err := http.Post(reqURI, contentType, bytes.NewReader(req))
	if err != nil {
		t.Fatal("Post setup request to server", err)
	}
	return res
}
