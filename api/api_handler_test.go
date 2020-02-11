package api_test

import (
	"strings"
	"testing"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
	"golang.org/x/xerrors"
)

func TestHandler_ListRepositoriesHandler(t *testing.T) {

	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("list no repos", func(t *testing.T) {
		resp, err := clt.Repositories.ListRepositories(&repositories.ListRepositoriesParams{},
			httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

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
		testutil.Must(t, deps.meta.CreateRepo("foo1", "master"))
		testutil.Must(t, deps.meta.CreateRepo("foo2", "master"))
		testutil.Must(t, deps.meta.CreateRepo("foo3", "master"))

		resp, err := clt.Repositories.ListRepositories(&repositories.ListRepositoriesParams{},
			httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

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
		}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

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
		}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

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

	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("get missing repo", func(t *testing.T) {
		_, err := clt.Repositories.GetRepository(&repositories.GetRepositoryParams{
			RepositoryID: "foo1",
		}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

		if err == nil {
			t.Fatalf("expected err calling missing repo")
		}

		if _, ok := err.(*repositories.GetRepositoryNotFound); !ok {
			t.Fatalf("expected not found error getting missing repo")
		}
	})

	t.Run("get existing repo", func(t *testing.T) {
		deps.meta.CreateRepo("foo1", "some_non_default_branch")
		resp, err := clt.Repositories.GetRepository(&repositories.GetRepositoryParams{
			RepositoryID: "foo1",
		}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

		if err != nil {
			t.Fatalf("unexpected err calling get repo, %v", err)
		}

		if !strings.EqualFold(resp.GetPayload().DefaultBranch, "some_non_default_branch") {
			t.Fatalf("unexpected branch name %s, expected some_non_default_branch",
				resp.GetPayload().DefaultBranch)
		}
	})

}

func TestHandler_GetCommitHandler(t *testing.T) {

	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("get missing commit", func(t *testing.T) {
		_, err := clt.Commits.GetCommit(&commits.GetCommitParams{
			CommitID:     "b0a989d946dca26496b8280ca2bb0a96131a48b362e72f1789e498815992fffa",
			RepositoryID: "foo1",
		}, bauth)
		if err == nil {
			t.Fatalf("expected err calling missing commit")
		}

		if _, ok := err.(*commits.GetCommitNotFound); !ok {
			t.Fatalf("expected not found error getting missing commit")
		}
	})

	t.Run("get existing commit", func(t *testing.T) {
		deps.meta.CreateRepo("foo1", "master")
		deps.meta.Commit("foo1", "master", "some message", DefaultUserId, nil)
		b, err := deps.meta.GetBranch("foo1", "master")
		if err != nil {
			t.Fatal(err)
		}
		resp, err := clt.Commits.GetCommit(&commits.GetCommitParams{
			CommitID:     b.Commit,
			RepositoryID: "foo1",
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected err calling commit, %v", err)
		}

		if !strings.EqualFold(resp.GetPayload().Committer, DefaultUserId) {
			t.Fatalf("unexpected commit id %s, expected %s",
				resp.GetPayload().Committer, DefaultUserId)
		}
	})
}

func TestHandler_CommitHandler(t *testing.T) {

	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("commit non-existent commit", func(t *testing.T) {
		_, err := clt.Commits.Commit(&commits.CommitParams{
			BranchID: "master",
			Commit: &models.CommitCreation{
				Message:  swag.String("some message"),
				Metadata: nil,
			},
			RepositoryID: "foo1",
		}, bauth)

		if err == nil {
			t.Fatalf("expected err calling missing repo for commit")
		}

		if _, ok := err.(*commits.CommitDefault); !ok {
			t.Fatalf("expected not found error when missing commit repo, got %v", err)
		}
	})

	t.Run("commit success", func(t *testing.T) {
		deps.meta.CreateRepo("foo1", "master")
		_, err := clt.Commits.Commit(&commits.CommitParams{
			BranchID: "master",
			Commit: &models.CommitCreation{
				Message:  swag.String("some message"),
				Metadata: nil,
			},
			RepositoryID: "foo1",
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected err calling commit: %s", err.Error())
		}
	})
}

func TestHandler_CreateRepositoryHandler(t *testing.T) {
	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("create repo success", func(t *testing.T) {
		resp, err := clt.Repositories.CreateRepository(&repositories.CreateRepositoryParams{
			Repository: &models.RepositoryCreation{
				BucketName:    swag.String("foo-bucket"),
				ID:            swag.String("my-new-repo"),
				DefaultBranch: "master",
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
		err := deps.meta.CreateRepo("repo2", "master")
		if err != nil {
			t.Fatal(err)
		}
		_, err = clt.Repositories.CreateRepository(&repositories.CreateRepositoryParams{
			Repository: &models.RepositoryCreation{
				BucketName:    swag.String("foo-bucket"),
				ID:            swag.String("repo2"),
				DefaultBranch: "master",
			},
		}, bauth)

		if err == nil {
			t.Fatalf("expected error creating duplicate repo")
		}
	})
}

func TestHandler_DeleteRepositoryHandler(t *testing.T) {
	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("delete repo success", func(t *testing.T) {
		testutil.Must(t, deps.meta.CreateRepo("my-new-repo", "master"))

		_, err := clt.Repositories.DeleteRepository(&repositories.DeleteRepositoryParams{
			RepositoryID: "my-new-repo",
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected error deleting repo: %s", err)
		}

		_, err = deps.meta.GetRepo("my-new-repo")
		if !xerrors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected repo to be gone, instead got error: %s", err)
		}
	})

	t.Run("delete repo doesnt exist", func(t *testing.T) {
		_, err := clt.Repositories.DeleteRepository(&repositories.DeleteRepositoryParams{
			RepositoryID: "my-other-repo",
		}, bauth)

		if err == nil {
			t.Fatalf("expected error deleting repo that doesnt exist")
		}
	})

	t.Run("delete repo doesnt delete other repos", func(t *testing.T) {
		testutil.Must(t, deps.meta.CreateRepo("rr0", "master"))
		testutil.Must(t, deps.meta.CreateRepo("rr1", "master"))
		testutil.Must(t, deps.meta.CreateRepo("rr11", "master"))
		testutil.Must(t, deps.meta.CreateRepo("rr2", "master"))
		_, err := clt.Repositories.DeleteRepository(&repositories.DeleteRepositoryParams{
			RepositoryID: "rr1",
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected error deleting repo: %s", err)
		}

		_, err = deps.meta.GetRepo("rr0")
		if err != nil {
			t.Fatalf("unexpected error getting other repo: %s", err)
		}
		_, err = deps.meta.GetRepo("rr11")
		if err != nil {
			t.Fatalf("unexpected error getting other repo: %s", err)
		}
		_, err = deps.meta.GetRepo("rr2")
		if err != nil {
			t.Fatalf("unexpected error getting other repo: %s", err)
		}
	})
}

func TestHandler_ListBranchesHandler(t *testing.T) {
	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("list branches only default", func(t *testing.T) {
		testutil.Must(t, deps.meta.CreateRepo("repo1", "master"))
		resp, err := clt.Branches.ListBranches(&branches.ListBranchesParams{
			Amount:       swag.Int64(-1),
			RepositoryID: "repo1",
		}, bauth)
		if err != nil {
			t.Fatalf("unexpected error listing branches: %s", err)
		}
		if len(resp.GetPayload().Results) != 1 {
			t.Fatalf("expected 1 branch, got %d", len(resp.GetPayload().Results))
		}
	})

	t.Run("list branches pagination", func(t *testing.T) {
		branch, err := deps.meta.GetBranch("repo1", "master")
		if err != nil {
			t.Fatal(err)
		}
		testutil.Must(t, deps.meta.CreateBranch("repo1", "master1", branch.GetCommit()))
		testutil.Must(t, deps.meta.CreateBranch("repo1", "master2", branch.GetCommit()))
		testutil.Must(t, deps.meta.CreateBranch("repo1", "master3", branch.GetCommit()))
		testutil.Must(t, deps.meta.CreateBranch("repo1", "master4", branch.GetCommit()))
		testutil.Must(t, deps.meta.CreateBranch("repo1", "master5", branch.GetCommit()))
		testutil.Must(t, deps.meta.CreateBranch("repo1", "master6", branch.GetCommit()))
		testutil.Must(t, deps.meta.CreateBranch("repo1", "master7", branch.GetCommit()))

		resp, err := clt.Branches.ListBranches(&branches.ListBranchesParams{
			Amount:       swag.Int64(2),
			RepositoryID: "repo1",
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetPayload().Results) != 2 {
			t.Fatalf("expected 2 branches to return, got %d", len(resp.GetPayload().Results))
		}

		resp, err = clt.Branches.ListBranches(&branches.ListBranchesParams{
			Amount:       swag.Int64(2),
			After:        swag.String(resp.GetPayload().Pagination.NextOffset),
			RepositoryID: "repo1",
		}, bauth)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetPayload().Results) != 2 {
			t.Fatalf("expected 2 branches to return, got %d", len(resp.GetPayload().Results))
		}
		if !strings.EqualFold(swag.StringValue(resp.GetPayload().Results[0].ID), "master2") {
			t.Fatalf("expected master3 as the first result for the second page, got %s instead", swag.StringValue(resp.GetPayload().Results[0].ID))
		}
	})

	t.Run("list branches repo doesnt exist", func(t *testing.T) {
		_, err := clt.Branches.ListBranches(&branches.ListBranchesParams{
			Amount:       swag.Int64(2),
			RepositoryID: "repo2",
		}, bauth)
		if err == nil {
			t.Fatal("expected error calling list branches on repo that doesnt exist")
		}
	})
}

func TestHandler_GetBranchHandler(t *testing.T) {
	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("get default branch", func(t *testing.T) {
		testutil.Must(t, deps.meta.CreateRepo("repo1", "master"))
		resp, err := clt.Branches.GetBranch(&branches.GetBranchParams{
			BranchID:     "master",
			RepositoryID: "repo1",
		}, bauth)
		if err != nil {
			t.Fatalf("unexpected error getting branch: %s", err)
		}
		if !strings.EqualFold(swag.StringValue(resp.GetPayload().ID), "master") {
			t.Fatalf("got unexpected branch %s", swag.StringValue(resp.GetPayload().ID))
		}
	})

	t.Run("get missing branch", func(t *testing.T) {
		_, err := clt.Branches.GetBranch(&branches.GetBranchParams{
			BranchID:     "master333",
			RepositoryID: "repo1",
		}, bauth)
		if err == nil {
			t.Fatal("expected error getting branch that doesnt exist")
		}
	})

	t.Run("get branch for missing repo", func(t *testing.T) {
		_, err := clt.Branches.GetBranch(&branches.GetBranchParams{
			BranchID:     "master",
			RepositoryID: "repo3",
		}, bauth)
		if err == nil {
			t.Fatal("expected error getting branch for repo that doesnt exist")
		}
	})
}

func TestHandler_CreateBranchHandler(t *testing.T) {
	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("create branch success", func(t *testing.T) {
		testutil.Must(t, deps.meta.CreateRepo("repo1", "master"))
		branch, err := deps.meta.GetBranch("repo1", "master")
		if err != nil {
			t.Fatal(err)
		}
		resp, err := clt.Branches.CreateBranch(&branches.CreateBranchParams{
			Branch: &models.Refspec{
				CommitID: swag.String(branch.GetCommit()),
				ID:       swag.String("master2"),
			},
			RepositoryID: "repo1",
		}, bauth)
		if err != nil {
			t.Fatalf("unexpected error creating branch: %s", err)
		}
		if !strings.EqualFold(swag.StringValue(resp.GetPayload().ID), "master2") {
			t.Fatalf("got unexpected branch %s", swag.StringValue(resp.GetPayload().ID))
		}
	})

	t.Run("create branch missing commit", func(t *testing.T) {
		_, err := clt.Branches.CreateBranch(&branches.CreateBranchParams{
			Branch: &models.Refspec{
				CommitID: swag.String("a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"),
				ID:       swag.String("master3"),
			},
			RepositoryID: "repo1",
		}, bauth)
		if err == nil {
			t.Fatal("expected error creating branch with a commit that doesnt exist")
		}
	})

	t.Run("create branch missing repo", func(t *testing.T) {
		branch, err := deps.meta.GetBranch("repo1", "master")
		if err != nil {
			t.Fatal(err)
		}
		_, err = clt.Branches.CreateBranch(&branches.CreateBranchParams{
			Branch: &models.Refspec{
				CommitID: swag.String(branch.GetCommit()),
				ID:       swag.String("master8"),
			},
			RepositoryID: "repo5",
		}, bauth)
		if err == nil {
			t.Fatal("expected error creating branch with a repo that doesnt exist")
		}
	})
}

func TestHandler_DeleteBranchHandler(t *testing.T) {
	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("delete branch success", func(t *testing.T) {
		testutil.Must(t, deps.meta.CreateRepo("my-new-repo", "master"))
		branch, err := deps.meta.GetBranch("my-new-repo", "master")
		if err != nil {
			t.Fatal(err)
		}
		testutil.Must(t, deps.meta.CreateBranch("my-new-repo", "master2", branch.GetCommit()))

		_, err = clt.Branches.DeleteBranch(&branches.DeleteBranchParams{
			BranchID:     "master2",
			RepositoryID: "my-new-repo",
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected error deleting branch: %s", err)
		}

		_, err = deps.meta.GetBranch("my-new-repo", "master2")
		if !xerrors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected branch to be gone, instead got error: %s", err)
		}
	})

	t.Run("delete branch doesnt exist", func(t *testing.T) {
		_, err := clt.Branches.DeleteBranch(&branches.DeleteBranchParams{
			BranchID:     "master5",
			RepositoryID: "my-new-repo",
		}, bauth)

		if err == nil {
			t.Fatalf("expected error deleting branch that doesnt exist")
		}
	})
}
