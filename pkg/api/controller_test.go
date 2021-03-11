package api_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/api/gen/client/auth"
	"github.com/treeverse/lakefs/pkg/api/gen/client/branches"
	"github.com/treeverse/lakefs/pkg/api/gen/client/commits"
	"github.com/treeverse/lakefs/pkg/api/gen/client/config"
	"github.com/treeverse/lakefs/pkg/api/gen/client/objects"
	"github.com/treeverse/lakefs/pkg/api/gen/client/refs"
	"github.com/treeverse/lakefs/pkg/api/gen/client/repositories"
	"github.com/treeverse/lakefs/pkg/api/gen/client/setup"
	"github.com/treeverse/lakefs/pkg/api/gen/client/tags"
	"github.com/treeverse/lakefs/pkg/api/gen/models"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	timeout       = 10 * time.Second
	DefaultUserID = "example_user"
)

func TestController_ListRepositoriesHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("list no repos", func(t *testing.T) {
		resp, err := clt.Repositories.ListRepositories(
			repositories.NewListRepositoriesParamsWithTimeout(timeout),
			bauth,
		)

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
		_, err := deps.catalog.CreateRepository(ctx, "foo1", "s3://foo1", "master")
		testutil.Must(t, err)
		_, err = deps.catalog.CreateRepository(ctx, "foo2", "s3://foo1", "master")
		testutil.Must(t, err)
		_, err = deps.catalog.CreateRepository(ctx, "foo3", "s3://foo1", "master")
		testutil.Must(t, err)

		resp, err := clt.Repositories.ListRepositories(
			repositories.NewListRepositoriesParamsWithTimeout(timeout),
			bauth,
		)

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
		resp, err := clt.Repositories.ListRepositories(
			repositories.NewListRepositoriesParamsWithTimeout(timeout).
				WithAmount(swag.Int64(2)),
			bauth,
		)

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
		resp, err := clt.Repositories.ListRepositories(
			repositories.NewListRepositoriesParamsWithTimeout(timeout).
				WithAmount(swag.Int64(2)).WithAfter(swag.String("foo2")),
			bauth,
		)

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

func TestController_GetRepoHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("get missing repo", func(t *testing.T) {
		_, err := clt.Repositories.GetRepository(
			repositories.NewGetRepositoryParamsWithTimeout(timeout).
				WithRepository("foo1"),
			bauth,
		)

		if err == nil {
			t.Fatalf("expected err calling missing repo")
		}

		if _, ok := err.(*repositories.GetRepositoryNotFound); !ok {
			t.Fatalf("expected not found error getting missing repo")
		}
	})

	t.Run("get existing repo", func(t *testing.T) {
		const testBranchName = "non-default"
		_, err := deps.catalog.CreateRepository(context.Background(), "foo1", "s3://foo1", testBranchName)
		testutil.Must(t, err)

		resp, err := clt.Repositories.GetRepository(
			repositories.NewGetRepositoryParamsWithTimeout(timeout).
				WithRepository("foo1"),
			bauth)

		if err != nil {
			t.Fatalf("unexpected err calling get repo, %v", err)
		}

		if !strings.EqualFold(resp.GetPayload().DefaultBranch, testBranchName) {
			t.Fatalf("unexpected branch name %s, expected %s",
				resp.GetPayload().DefaultBranch, testBranchName)
		}
	})
}

func TestController_CommitsGetBranchCommitLogHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	ctx := context.Background()
	t.Run("get missing branch", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo1", "ns1", "master")
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
		_, err := deps.catalog.CreateRepository(ctx, "repo2", "ns1", "master")
		if err != nil {
			t.Fatal(err)
		}
		const commitsLen = 2
		for i := 0; i < commitsLen; i++ {
			n := strconv.Itoa(i + 1)
			p := "foo/bar" + n
			testutil.MustDo(t, "create entry bar"+n, deps.catalog.CreateEntry(ctx, "repo2", "master", catalog.DBEntry{Path: p, PhysicalAddress: "bar" + n + "addr", CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n}))
			if _, err := deps.catalog.Commit(ctx, "repo2", "master", "commit"+n, "some_user", nil); err != nil {
				t.Fatalf("failed to commit '%s': %s", p, err)
			}
		}
		resp, err := clt.Commits.GetBranchCommitLog(
			commits.NewGetBranchCommitLogParamsWithTimeout(timeout).
				WithBranch("master").
				WithRepository("repo2"),
			bauth)
		if err != nil {
			t.Fatalf("unexpected error getting log of commits: %s", err)
		}

		// repo is created with a commit
		const expectedCommits = commitsLen + 1
		commitsLog := resp.GetPayload().Results
		if len(commitsLog) != expectedCommits {
			t.Fatalf("Log %d commits, expected %d", len(commitsLog), expectedCommits)
		}
	})
}

func TestController_GetCommitHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("get missing commit", func(t *testing.T) {
		_, err := clt.Commits.GetCommit(
			commits.NewGetCommitParamsWithTimeout(timeout).
				WithCommitID("b0a989d946dca26496b8280ca2bb0a96131a48b362e72f1789e498815992fffa").
				WithRepository("foo1"),
			bauth)
		if err == nil {
			t.Fatalf("expected err calling missing commit")
		}

		if _, ok := err.(*commits.GetCommitNotFound); !ok {
			t.Fatalf("expected not found error getting missing commit")
		}
	})

	t.Run("get existing commit", func(t *testing.T) {
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "foo1", "s3://foo1", "master")
		testutil.Must(t, err)
		testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, "foo1", "master", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
		commit1, err := deps.catalog.Commit(ctx, "foo1", "master", "some message", DefaultUserID, nil)
		testutil.Must(t, err)
		reference1, err := deps.catalog.GetBranchReference(ctx, "foo1", "master")
		if err != nil {
			t.Fatal(err)
		}
		if reference1 != commit1.Reference {
			t.Fatalf("Commit reference %s, not equals to branch reference %s", commit1, reference1)
		}
		resp, err := clt.Commits.GetCommit(
			commits.NewGetCommitParamsWithTimeout(timeout).
				WithCommitID(commit1.Reference).
				WithRepository("foo1"),
			bauth)
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

func TestController_CommitHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("commit non-existent commit", func(t *testing.T) {
		_, err := clt.Commits.Commit(
			commits.NewCommitParamsWithTimeout(timeout).
				WithBranch("master").
				WithCommit(&models.CommitCreation{
					Message:  swag.String("some message"),
					Metadata: nil,
				}).
				WithRepository("foo1"),
			bauth)

		if err == nil {
			t.Fatalf("expected err calling missing repo for commit")
		}

		if _, ok := err.(*commits.CommitDefault); !ok {
			t.Fatalf("expected not found error when missing commit repo, got %v", err)
		}
	})

	t.Run("commit success", func(t *testing.T) {
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "foo1", "s3://foo1", "master")
		testutil.MustDo(t, "create repo foo1", err)
		testutil.MustDo(t, "commit bar on foo1", deps.catalog.CreateEntry(ctx, "foo1", "master", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		_, err = clt.Commits.Commit(
			commits.NewCommitParamsWithTimeout(timeout).
				WithBranch("master").
				WithCommit(&models.CommitCreation{
					Message:  swag.String("some message"),
					Metadata: nil,
				}).
				WithRepository("foo1"),
			bauth)
		if err != nil {
			t.Fatalf("unexpected error on commit: %s", err)
		}
	})
}

func TestController_CreateRepositoryHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("create repo success", func(t *testing.T) {
		resp, err := clt.Repositories.CreateRepository(
			repositories.NewCreateRepositoryParamsWithTimeout(timeout).
				WithRepository(&models.RepositoryCreation{
					StorageNamespace: swag.String("s3://foo-bucket/"),
					Name:             swag.String("my-new-repo"),
					DefaultBranch:    "master",
				}),
			bauth)

		if err != nil {
			t.Fatalf("unexpected error creating valid repo: %s", err)
		}

		if !strings.EqualFold(resp.GetPayload().ID, "my-new-repo") {
			t.Fatalf("got unexpected repo when creating my-new-repo: %s", resp.GetPayload().ID)
		}
	})

	t.Run("create repo duplicate", func(t *testing.T) {
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "repo2", "s3://foo1/", "master")
		if err != nil {
			t.Fatal(err)
		}
		_, err = clt.Repositories.CreateRepository(
			repositories.NewCreateRepositoryParamsWithTimeout(timeout).
				WithRepository(&models.RepositoryCreation{
					StorageNamespace: swag.String("s3://foo-bucket/"),
					Name:             swag.String("repo2"),
					DefaultBranch:    "master",
				}),
			bauth)

		if err == nil {
			t.Fatalf("expected error creating duplicate repo")
		}
	})
}

func TestController_DeleteRepositoryHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	ctx := context.Background()
	t.Run("delete repo success", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "my-new-repo", "s3://foo1/", "master")
		testutil.Must(t, err)

		_, err = clt.Repositories.DeleteRepository(
			repositories.NewDeleteRepositoryParamsWithTimeout(timeout).
				WithRepository("my-new-repo"),
			bauth)

		if err != nil {
			t.Fatalf("unexpected error deleting repo: %s", err)
		}

		_, err = deps.catalog.GetRepository(ctx, "my-new-repo")
		if !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("expected repo to be gone, instead got error: %s", err)
		}
	})

	t.Run("delete repo doesnt exist", func(t *testing.T) {
		_, err := clt.Repositories.DeleteRepository(
			repositories.NewDeleteRepositoryParamsWithTimeout(timeout).
				WithRepository("my-other-repo"),
			bauth)

		if err == nil {
			t.Fatalf("expected error deleting repo that doesnt exist")
		}
	})

	t.Run("delete repo doesnt delete other repos", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "rr0", "s3://foo1", "master")
		testutil.Must(t, err)
		_, err = deps.catalog.CreateRepository(ctx, "rr1", "s3://foo1", "master")
		testutil.Must(t, err)
		_, err = deps.catalog.CreateRepository(ctx, "rr11", "s3://foo1", "master")
		testutil.Must(t, err)
		_, err = deps.catalog.CreateRepository(ctx, "rr2", "s3://foo1", "master")
		testutil.Must(t, err)
		_, err = clt.Repositories.DeleteRepository(
			repositories.NewDeleteRepositoryParamsWithTimeout(timeout).
				WithRepository("rr1"),
			bauth)

		if err != nil {
			t.Fatalf("unexpected error deleting repo: %s", err)
		}

		_, err = deps.catalog.GetRepository(ctx, "rr0")
		if err != nil {
			t.Fatalf("unexpected error getting other repo: %s", err)
		}
		_, err = deps.catalog.GetRepository(ctx, "rr11")
		if err != nil {
			t.Fatalf("unexpected error getting other repo: %s", err)
		}
		_, err = deps.catalog.GetRepository(ctx, "rr2")
		if err != nil {
			t.Fatalf("unexpected error getting other repo: %s", err)
		}
	})
}

func TestController_ListBranchesHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	// setup repository

	t.Run("list branches only default", func(t *testing.T) {
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "repo1", "s3://foo1", "master")
		testutil.Must(t, err)
		resp, err := clt.Branches.ListBranches(
			branches.NewListBranchesParamsWithTimeout(timeout).
				WithAmount(swag.Int64(-1)).
				WithRepository("repo1"),
			bauth)
		if err != nil {
			t.Fatalf("unexpected error listing branches: %s", err)
		}
		const expectedBranchesLen = 1
		branchesLen := len(resp.GetPayload().Results)
		if branchesLen != expectedBranchesLen {
			t.Fatalf("ListBranches len=%d, expected %d", branchesLen, expectedBranchesLen)
		}
	})

	t.Run("list branches pagination", func(t *testing.T) {
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "repo2", "s3://foo2", "master")
		testutil.Must(t, err)

		// create first dummy commit on master so that we can create branches from it
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo2", "master", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "repo2", "master", "first commit", "test", nil)
		testutil.Must(t, err)

		for i := 0; i < 7; i++ {
			branchName := "master" + strconv.Itoa(i+1)
			_, err := deps.catalog.CreateBranch(ctx, "repo2", branchName, "master")
			testutil.MustDo(t, "create branch "+branchName, err)
		}
		resp, err := clt.Branches.ListBranches(
			branches.NewListBranchesParamsWithTimeout(timeout).
				WithAmount(swag.Int64(2)).
				WithRepository("repo2"),
			bauth)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetPayload().Results) != 2 {
			t.Fatalf("expected 2 branches to return, got %d", len(resp.GetPayload().Results))
		}

		resp, err = clt.Branches.ListBranches(
			branches.NewListBranchesParamsWithTimeout(timeout).
				WithAmount(swag.Int64(2)).
				WithAfter(swag.String("master1")).
				WithRepository("repo2"),
			bauth)
		if err != nil {
			t.Fatal(err)
		}
		results := resp.GetPayload().Results
		if len(results) != 2 {
			t.Fatalf("expected 2 branches to return, got %d", len(results))
		}
		retReference := results[0]
		const expectedID = "master2"
		if swag.StringValue(retReference.ID) != expectedID {
			t.Fatalf("expected '%s' as the first result for the second page, got '%s' instead",
				expectedID, swag.StringValue(retReference.ID))
		}
	})

	t.Run("list branches repo doesnt exist", func(t *testing.T) {
		_, err := clt.Branches.ListBranches(
			branches.NewListBranchesParamsWithTimeout(timeout).
				WithAmount(swag.Int64(2)).
				WithRepository("repoX"),
			bauth)
		if err == nil {
			t.Fatal("expected error calling list branches on repo that doesnt exist")
		}
	})
}

func TestController_ListTagsHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	// setup test data
	ctx := context.Background()
	_, err := deps.catalog.CreateRepository(ctx, "repo1", "local://foo1", "master")
	testutil.Must(t, err)
	testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", "master", catalog.DBEntry{Path: "obj1"}))
	commitLog, err := deps.catalog.Commit(ctx, "repo1", "master", "first commit", "test", nil)
	testutil.Must(t, err)
	const createTagLen = 7
	var createdTags []*models.Ref
	for i := 0; i < createTagLen; i++ {
		tagID := swag.String("tag" + strconv.Itoa(i))
		commitID := swag.String(commitLog.Reference)
		_, err := clt.Tags.CreateTag(
			tags.NewCreateTagParamsWithTimeout(timeout).
				WithRepository("repo1").
				WithTag(&models.TagCreation{
					ID:  tagID,
					Ref: commitID,
				}),
			bauth)
		testutil.Must(t, err)
		createdTags = append(createdTags, &models.Ref{
			ID:       tagID,
			CommitID: commitID,
		})
	}

	t.Run("default", func(t *testing.T) {
		resp, err := clt.Tags.ListTags(
			tags.NewListTagsParamsWithTimeout(timeout).
				WithRepository("repo1").
				WithAmount(swag.Int64(-1)),
			bauth)
		testutil.MustDo(t, "list tags", err)
		payload := resp.GetPayload()
		tagsLen := len(payload.Results)
		if tagsLen != createTagLen {
			t.Fatalf("ListTags len=%d, expected %d", tagsLen, createTagLen)
		}
		if diff := deep.Equal(payload.Results, createdTags); diff != nil {
			t.Fatal("ListTags results diff:", diff)
		}
	})

	t.Run("pagination", func(t *testing.T) {
		const pageSize = 2
		var results []*models.Ref
		var after string
		var calls int
		for {
			calls++
			resp, err := clt.Tags.ListTags(
				tags.NewListTagsParamsWithTimeout(timeout).
					WithRepository("repo1").
					WithAmount(swag.Int64(pageSize)).
					WithAfter(swag.String(after)),
				bauth)
			testutil.Must(t, err)
			payload := resp.GetPayload()
			results = append(results, payload.Results...)
			if !swag.BoolValue(payload.Pagination.HasMore) {
				break
			}
			after = payload.Pagination.NextOffset
		}
		expectedCalls := int(math.Ceil(float64(createTagLen) / pageSize))
		if calls != expectedCalls {
			t.Fatalf("ListTags pagination calls=%d, expected=%d", calls, expectedCalls)
		}
		if diff := deep.Equal(results, createdTags); diff != nil {
			t.Fatal("ListTags results diff:", diff)
		}
	})

	t.Run("no repository", func(t *testing.T) {
		_, err := clt.Tags.ListTags(
			tags.NewListTagsParamsWithTimeout(timeout).
				WithRepository("repoX"),
			bauth)
		var listTagsDefault *tags.ListTagsDefault
		if !errors.As(err, &listTagsDefault) {
			t.Fatal("expected error calling list tags on when repo doesnt exist")
		} else if listTagsDefault.Code() != http.StatusInternalServerError {
			t.Fatalf("expected error status code %d, expected %d", listTagsDefault.Code(), http.StatusInternalServerError)
		}
	})
}

func TestController_GetBranchHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("get default branch", func(t *testing.T) {
		ctx := context.Background()
		const testBranch = "master"
		_, err := deps.catalog.CreateRepository(ctx, "repo1", "s3://foo1", testBranch)
		// create first dummy commit on master so that we can create branches from it
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", testBranch, catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "repo1", testBranch, "first commit", "test", nil)
		testutil.Must(t, err)

		testutil.Must(t, err)
		resp, err := clt.Branches.GetBranch(
			branches.NewGetBranchParamsWithTimeout(timeout).
				WithBranch(testBranch).
				WithRepository("repo1"),
			bauth)
		if err != nil {
			t.Fatalf("unexpected error getting branch: %s", err)
		}
		reference := resp.GetPayload()
		if reference == nil || reference.CommitID == nil || *reference.CommitID == "" {
			t.Fatalf("Got no reference for branch '%s'", testBranch)
		}
	})

	t.Run("get missing branch", func(t *testing.T) {
		_, err := clt.Branches.GetBranch(
			branches.NewGetBranchParamsWithTimeout(timeout).
				WithBranch("master333").
				WithRepository("repo1"),
			bauth)
		if err == nil {
			t.Fatal("expected error getting branch that doesnt exist")
		}
	})

	t.Run("get branch for missing repo", func(t *testing.T) {
		_, err := clt.Branches.GetBranch(
			branches.NewGetBranchParamsWithTimeout(timeout).
				WithBranch("master").
				WithRepository("repo3"),
			bauth)
		if err == nil {
			t.Fatal("expected error getting branch for repo that doesnt exist")
		}
	})
}

func TestController_BranchesDiffBranchHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)
	ctx := context.Background()
	const testBranch = "master"
	_, err := deps.catalog.CreateRepository(ctx, "repo1", "s3://foo1", testBranch)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("diff branch no changes", func(t *testing.T) {
		diff, err := clt.Branches.DiffBranch(branches.NewDiffBranchParams().
			WithRepository("repo1").
			WithBranch(testBranch), bauth)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff.Payload.Results) != 0 {
			t.Fatalf("expected no diff results, got %d", len(diff.Payload.Results))
		}
	})

	t.Run("diff branch with writes", func(t *testing.T) {
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", testBranch, catalog.DBEntry{Path: "a/b"}))
		diff, err := clt.Branches.DiffBranch(branches.NewDiffBranchParams().
			WithRepository("repo1").
			WithBranch(testBranch), bauth)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff.Payload.Results) != 1 {
			t.Fatalf("expected no diff results, got %d", len(diff.Payload.Results))
		}

		if diff.Payload.Results[0].Path != "a/b" {
			t.Fatalf("got wrong diff object, expected a/b, got %s", diff.Payload.Results[0].Path)
		}
	})

	t.Run("diff branch that doesn't exist", func(t *testing.T) {
		_, err := clt.Branches.DiffBranch(branches.NewDiffBranchParams().
			WithRepository("repo1").
			WithBranch("some-other-missing-branch"), bauth)
		if _, ok := err.(*branches.DiffBranchNotFound); !ok {
			t.Fatalf("expected an 404, got %v", err)
		}

	})
}

func TestController_CreateBranchHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("create branch and diff refs success", func(t *testing.T) {
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "repo1", "s3://foo1", "master")
		testutil.Must(t, err)
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", "master", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "repo1", "master", "first commit", "test", nil)
		testutil.Must(t, err)

		const newBranchName = "master2"
		resp, err := clt.Branches.CreateBranch(
			branches.NewCreateBranchParamsWithTimeout(timeout).
				WithBranch(&models.BranchCreation{
					Name:   swag.String(newBranchName),
					Source: swag.String("master"),
				}).
				WithRepository("repo1"),
			bauth)
		if err != nil {
			t.Fatalf("unexpected error creating branch: %s", err)
		}
		reference := resp.GetPayload()
		if len(reference) == 0 {
			t.Fatalf("branch %s creation got no reference", newBranchName)
		}
		path := "some/path"
		buf := new(bytes.Buffer)
		buf.WriteString("hello world!")
		_, err = clt.Objects.UploadObject(
			objects.NewUploadObjectParamsWithTimeout(timeout).
				WithBranch(newBranchName).
				WithContent(runtime.NamedReader("content", buf)).
				WithPath(path).
				WithRepository("repo1"),
			bauth)
		if err != nil {
			t.Fatalf("unexpected error uploading object: %s", err)
		}
		if _, err := deps.catalog.Commit(ctx, "repo1", "master2", "commit 1", "some_user", nil); err != nil {
			t.Fatalf("failed to commit 'repo1': %s", err)
		}
		resp2, err := clt.Refs.DiffRefs(
			refs.NewDiffRefsParamsWithTimeout(timeout).
				WithLeftRef(newBranchName).
				WithRightRef("master").
				WithRepository("repo1"),
			bauth)
		if err != nil {
			t.Fatalf("unexpected error diffing refs: %s", err)
		}
		results := resp2.GetPayload().Results
		if len(results) != 1 {
			t.Fatalf("unexpected length of results: %d", len(results))
		}
		if results[0].Path != path {
			t.Fatalf("wrong result: %s", results[0].Path)
		}
	})

	t.Run("create branch missing commit", func(t *testing.T) {
		_, err := clt.Branches.CreateBranch(
			branches.NewCreateBranchParamsWithTimeout(timeout).
				WithBranch(&models.BranchCreation{
					Source: swag.String("a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"),
					Name:   swag.String("master3"),
				}).
				WithRepository("repo1"),
			bauth)
		if err == nil {
			t.Fatal("expected error creating branch with a commit that doesnt exist")
		}
	})

	t.Run("create branch missing repo", func(t *testing.T) {
		_, err := clt.Branches.CreateBranch(
			branches.NewCreateBranchParamsWithTimeout(timeout).
				WithBranch(&models.BranchCreation{
					Source: swag.String("master"),
					Name:   swag.String("master8"),
				}).
				WithRepository("repo5"),
			bauth)
		if err == nil {
			t.Fatal("expected error creating branch with a repo that doesnt exist")
		}
	})
}

func TestController_DeleteBranchHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("delete branch success", func(t *testing.T) {
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "my-new-repo", "s3://foo1", "master")
		testutil.Must(t, err)
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "my-new-repo", "master", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "my-new-repo", "master", "first commit", "test", nil)
		testutil.Must(t, err)

		_, err = deps.catalog.CreateBranch(ctx, "my-new-repo", "master2", "master")
		if err != nil {
			t.Fatal(err)
		}

		_, err = clt.Branches.DeleteBranch(
			branches.NewDeleteBranchParamsWithTimeout(timeout).
				WithBranch("master2").
				WithRepository("my-new-repo"),
			bauth)

		if err != nil {
			t.Fatalf("unexpected error deleting branch: %s", err)
		}

		_, err = deps.catalog.GetBranchReference(ctx, "my-new-repo", "master2")
		if !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("expected branch to be gone, instead got error: %s", err)
		}
	})

	t.Run("delete branch doesnt exist", func(t *testing.T) {
		_, err := clt.Branches.DeleteBranch(
			branches.NewDeleteBranchParamsWithTimeout(timeout).
				WithBranch("master5").
				WithRepository("my-new-repo"),
			bauth)

		if err == nil {
			t.Fatalf("expected error deleting branch that doesnt exist")
		}
	})
}

func TestController_ObjectsStatObjectHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	ctx := context.Background()
	_, err := deps.catalog.CreateRepository(ctx, "repo1", "s3://some-bucket", "master")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("get object stats", func(t *testing.T) {
		entry := catalog.DBEntry{
			Path:            "foo/bar",
			PhysicalAddress: "this_is_bars_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "this_is_a_checksum",
			Metadata:        nil,
		}
		testutil.Must(t,
			deps.catalog.CreateEntry(ctx, "repo1", "master", entry))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := clt.Objects.StatObject(
			objects.NewStatObjectParamsWithTimeout(timeout).
				WithRef("master").
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth)

		if err != nil {
			t.Fatalf("did not expect error for stat, got %s", err)
		}
		if resp.Payload.Path != entry.Path {
			t.Fatalf("expected to get back our path, got %s", resp.Payload.Path)
		}
		if swag.Int64Value(resp.Payload.SizeBytes) != entry.Size {
			t.Fatalf("expected correct size, got %d", resp.Payload.SizeBytes)
		}
		if resp.Payload.PhysicalAddress != "s3://some-bucket/"+entry.PhysicalAddress {
			t.Fatalf("expected correct PhysicalAddress, got %s", resp.Payload.PhysicalAddress)
		}

		_, err = clt.Objects.StatObject(
			objects.NewStatObjectParamsWithTimeout(timeout).
				WithRef("master:HEAD").
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth)

		if _, ok := err.(*objects.StatObjectNotFound); !ok {
			t.Fatalf("did expect object not found for stat, got %v", err)
		}
	})
}

func TestController_ObjectsListObjectsHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	ctx := context.Background()
	_, err := deps.catalog.CreateRepository(ctx, "repo1", "gs://bucket/prefix", "master")
	testutil.Must(t, err)
	testutil.Must(t,
		deps.catalog.CreateEntry(ctx, "repo1", "master", catalog.DBEntry{
			Path:            "foo/bar",
			PhysicalAddress: "this_is_bars_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "this_is_a_checksum",
		}))
	testutil.Must(t,
		deps.catalog.CreateEntry(ctx, "repo1", "master", catalog.DBEntry{
			Path:            "foo/quuux",
			PhysicalAddress: "this_is_quuxs_address_expired",
			CreationDate:    time.Now(),
			Size:            9999999,
			Checksum:        "quux_checksum",
			Expired:         true,
		}))
	testutil.Must(t,
		deps.catalog.CreateEntry(ctx, "repo1", "master", catalog.DBEntry{
			Path:            "foo/baz",
			PhysicalAddress: "this_is_bazs_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "baz_checksum",
		}))
	testutil.Must(t,
		deps.catalog.CreateEntry(ctx, "repo1", "master", catalog.DBEntry{
			Path:            "foo/a_dir/baz",
			PhysicalAddress: "this_is_bazs_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "baz_checksum",
		}))

	t.Run("get object list", func(t *testing.T) {
		resp, err := clt.Objects.ListObjects(
			objects.NewListObjectsParamsWithTimeout(timeout).
				WithRef("master").
				WithRepository("repo1").
				WithPrefix(swag.String("foo/")),
			bauth)
		if err != nil {
			t.Fatal(err)
		}

		if len(resp.Payload.Results) != 4 {
			t.Fatalf("expected 4 entries, got back %d", len(resp.Payload.Results))
		}

	})

	t.Run("get object list paginated", func(t *testing.T) {
		resp, err := clt.Objects.ListObjects(
			objects.NewListObjectsParamsWithTimeout(timeout).
				WithAmount(swag.Int64(2)).
				WithRef("master").
				WithRepository("repo1").
				WithPrefix(swag.String("foo/")),
			bauth)
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

func TestController_ObjectsGetObjectHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", "ns1", "master")
	if err != nil {
		t.Fatal(err)
	}

	expensiveString := "EXPENSIVE"

	buf := new(bytes.Buffer)
	buf.WriteString("this is file content made up of bytes")
	blob, err := upload.WriteBlob(context.Background(), deps.blocks, "ns1", buf, 37, block.PutOpts{StorageClass: &expensiveString})
	if err != nil {
		t.Fatal(err)
	}
	entry := catalog.DBEntry{
		Path:            "foo/bar",
		PhysicalAddress: blob.PhysicalAddress,
		CreationDate:    time.Now(),
		Size:            blob.Size,
		Checksum:        blob.Checksum,
	}
	testutil.Must(t,
		deps.catalog.CreateEntry(ctx, "repo1", "master", entry))

	expired := catalog.DBEntry{
		Path:            "foo/expired",
		PhysicalAddress: "an_expired_physical_address",
		CreationDate:    time.Now(),
		Size:            99999,
		Checksum:        "b10b",
		Expired:         true,
	}
	testutil.Must(t,
		deps.catalog.CreateEntry(ctx, "repo1", "master", expired))

	t.Run("get object", func(t *testing.T) {
		buf := new(bytes.Buffer)
		resp, err := clt.Objects.GetObject(
			objects.NewGetObjectParamsWithTimeout(timeout).
				WithRef("master").
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth, buf)
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

		_, err = clt.Objects.GetObject(
			objects.NewGetObjectParamsWithTimeout(timeout).
				WithRef("master:HEAD").
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth, buf)
		if _, ok := err.(*objects.GetObjectNotFound); !ok {
			t.Fatalf("expected object not found error, got %v", err)
		}
	})

	t.Run("get properties", func(t *testing.T) {
		properties, err := clt.Objects.GetUnderlyingProperties(
			objects.NewGetUnderlyingPropertiesParamsWithTimeout(timeout).
				WithRef("master").
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth)
		if err != nil {
			t.Fatalf("expected to get underlying properties, got %v", err)
		}
		if *properties.Payload.StorageClass != expensiveString {
			t.Errorf("expected to get \"%s\" storage class, got %#v", expensiveString, properties)
		}
	})
}

func TestController_ObjectsUploadObjectHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	ctx := context.Background()
	_, err := deps.catalog.CreateRepository(ctx, "repo1", "gs://bucket/prefix", "master")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("upload object", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteString("hello world this is my awesome content")
		resp, err := clt.Objects.UploadObject(
			objects.NewUploadObjectParamsWithTimeout(timeout).
				WithBranch("master").
				WithContent(runtime.NamedReader("content", buf)).
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth)
		if err != nil {
			t.Fatal(err)
		}

		if swag.Int64Value(resp.Payload.SizeBytes) != 38 {
			t.Fatalf("expected 38 bytes to be written, got back %d", resp.Payload.SizeBytes)
		}

		// download it
		rbuf := new(bytes.Buffer)
		rresp, err := clt.Objects.GetObject(
			objects.NewGetObjectParamsWithTimeout(timeout).
				WithRef("master").
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth, rbuf)
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
		_, err := clt.Objects.UploadObject(
			objects.NewUploadObjectParamsWithTimeout(timeout).
				WithBranch("masterX").
				WithContent(runtime.NamedReader("content", buf)).
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth)
		if _, ok := err.(*objects.UploadObjectNotFound); !ok {
			t.Fatal("Missing branch should return not found")
		}
	})

	t.Run("upload object missing repo", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteString("hello world this is my awesome content")
		_, err := clt.Objects.UploadObject(
			objects.NewUploadObjectParamsWithTimeout(timeout).
				WithBranch("master").
				WithContent(runtime.NamedReader("content", buf)).
				WithPath("foo/bar").
				WithRepository("repo55555"),
			bauth)
		if _, ok := err.(*objects.UploadObjectNotFound); !ok {
			t.Fatal("Missing branch should return not found")
		}
	})

}

func TestController_ObjectsStageObjectHandler(t *testing.T) {
	clt, deps := setupClient(t, "s3")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	ctx := context.Background()
	_, err := deps.catalog.CreateRepository(ctx, "repo1", "s3://bucket/prefix", "master")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("stage object", func(t *testing.T) {
		resp, err := clt.Objects.StageObject(objects.NewStageObjectParams().
			WithRepository("repo1").
			WithBranch("master").
			WithPath("foo/bar").
			WithObject(&models.ObjectStageCreation{
				Checksum:        swag.String("afb0689fe58b82c5f762991453edbbec"),
				PhysicalAddress: swag.String("s3://another-bucket/some/location"),
				SizeBytes:       swag.Int64(38),
			}), bauth)

		if err != nil {
			t.Fatal(err)
		}

		if swag.Int64Value(resp.Payload.SizeBytes) != 38 {
			t.Fatalf("expected 38 bytes to be written, got back %d", resp.Payload.SizeBytes)
		}

		// get back info
		got, err := clt.Objects.StatObject(objects.NewStatObjectParams().
			WithRepository("repo1").
			WithRef("master").
			WithPath("foo/bar"), bauth)
		if err != nil {
			t.Fatal(err)
		}

		if got.Payload.PhysicalAddress != "s3://another-bucket/some/location" {
			t.Fatalf("unexpected physical address: %s", got.Payload.PhysicalAddress)
		}
	})

	t.Run("upload object missing branch", func(t *testing.T) {
		_, err := clt.Objects.StageObject(objects.NewStageObjectParams().
			WithRepository("repo1").
			WithBranch("master1234").
			WithPath("foo/bar").
			WithObject(&models.ObjectStageCreation{
				Checksum:        swag.String("afb0689fe58b82c5f762991453edbbec"),
				PhysicalAddress: swag.String("s3://another-bucket/some/location"),
				SizeBytes:       swag.Int64(38),
			}), bauth)
		if _, ok := err.(*objects.StageObjectNotFound); !ok {
			t.Fatal("Missing branch should return not found")
		}
	})

	t.Run("wrong storage adapter", func(t *testing.T) {
		_, err := clt.Objects.StageObject(objects.NewStageObjectParams().
			WithRepository("repo1").
			WithBranch("master1234").
			WithPath("foo/bar").
			WithObject(&models.ObjectStageCreation{
				Checksum:        swag.String("afb0689fe58b82c5f762991453edbbec"),
				PhysicalAddress: swag.String("gs://another-bucket/some/location"),
				SizeBytes:       swag.Int64(38),
			}), bauth)
		if _, ok := err.(*objects.StageObjectBadRequest); !ok {
			t.Fatal("Wrong storage adapter should return 400")
		}
	})
}

func TestController_ObjectsDeleteObjectHandler(t *testing.T) {
	clt, deps := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	ctx := context.Background()
	_, err := deps.catalog.CreateRepository(ctx, "repo1", "s3://some-bucket/prefix", "master")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("delete object", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteString("hello world this is my awesome content")
		resp, err := clt.Objects.UploadObject(
			objects.NewUploadObjectParamsWithTimeout(timeout).
				WithBranch("master").
				WithContent(runtime.NamedReader("content", buf)).
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth)
		if err != nil {
			t.Fatal(err)
		}

		if swag.Int64Value(resp.Payload.SizeBytes) != 38 {
			t.Fatalf("expected 38 bytes to be written, got back %d", resp.Payload.SizeBytes)
		}

		// download it
		rbuf := new(bytes.Buffer)
		rresp, err := clt.Objects.GetObject(
			objects.NewGetObjectParamsWithTimeout(timeout).
				WithRef("master").
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth, rbuf)
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
		_, err = clt.Objects.DeleteObject(
			objects.NewDeleteObjectParamsWithTimeout(timeout).
				WithBranch("master").
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth)
		if err != nil {
			t.Fatal(err)
		}

		// get it
		_, err = clt.Objects.StatObject(
			objects.NewStatObjectParamsWithTimeout(timeout).
				WithRef("master").
				WithPath("foo/bar").
				WithRepository("repo1"),
			bauth)
		if err == nil {
			t.Fatalf("expected file to be gone now")
		}
	})
}

func TestController_CreatePolicyHandler(t *testing.T) {
	clt, _ := setupClient(t, "")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("valid_policy", func(t *testing.T) {
		_, err := clt.Auth.CreatePolicy(
			auth.NewCreatePolicyParamsWithTimeout(timeout).
				WithPolicy(&models.Policy{
					CreationDate: time.Now().Unix(),
					ID:           swag.String("ValidPolicyID"),
					Statement: []*models.Statement{
						{
							Action:   []string{"fs:ReadObject"},
							Effect:   swag.String("allow"),
							Resource: swag.String("arn:lakefs:fs:::repository/foo/object/*"),
						},
					},
				}),
			bauth)
		if err != nil {
			t.Fatalf("unexpected error creating valid policy: %v", err)
		}
	})

	t.Run("invalid_policy_action", func(t *testing.T) {
		_, err := clt.Auth.CreatePolicy(
			auth.NewCreatePolicyParamsWithTimeout(timeout).
				WithPolicy(&models.Policy{
					CreationDate: time.Now().Unix(),
					ID:           swag.String("ValidPolicyID"),
					Statement: []*models.Statement{
						{
							Action:   []string{"fsx:ReadObject"},
							Effect:   swag.String("allow"),
							Resource: swag.String("arn:lakefs:fs:::repository/foo/object/*"),
						},
					},
				}),
			bauth)
		if err == nil {
			t.Fatalf("expected error creating invalid policy: action")
		}
	})

	t.Run("invalid_policy_effect", func(t *testing.T) {
		_, err := clt.Auth.CreatePolicy(
			auth.NewCreatePolicyParamsWithTimeout(timeout).
				WithPolicy(&models.Policy{
					CreationDate: time.Now().Unix(),
					ID:           swag.String("ValidPolicyID"),
					Statement: []*models.Statement{
						{
							Action:   []string{"fs:ReadObject"},
							Effect:   swag.String("Allow"),
							Resource: swag.String("arn:lakefs:fs:::repository/foo/object/*"),
						},
					},
				}),
			bauth)
		if err == nil {
			t.Fatalf("expected error creating invalid policy: effect")
		}
	})

	t.Run("invalid_policy_arn", func(t *testing.T) {
		_, err := clt.Auth.CreatePolicy(
			auth.NewCreatePolicyParamsWithTimeout(timeout).
				WithPolicy(&models.Policy{
					CreationDate: time.Now().Unix(),
					ID:           swag.String("ValidPolicyID"),
					Statement: []*models.Statement{
						{
							Action:   []string{"fs:ReadObject"},
							Effect:   swag.String("Allow"),
							Resource: swag.String("arn:lakefs:fs:repository/foo/object/*"),
						},
					},
				}),
			bauth)
		if err == nil {
			t.Fatalf("expected error creating invalid policy: arn")
		}
	})

}

func TestController_ConfigHandlers(t *testing.T) {
	const ExpectedExample = "s3://example-bucket/"
	clt, _ := setupClient(t, "s3")

	// create user
	creds := createDefaultAdminUser(t, clt)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("Get config (currently only block store type)", func(t *testing.T) {
		resp, err := clt.Config.GetConfig(
			config.NewGetConfigParamsWithTimeout(timeout),
			bauth)
		if err != nil {
			t.Fatal(err)
		}

		got := resp.GetPayload()

		if got.BlockstoreNamespaceExample != ExpectedExample {
			t.Errorf("expected to get %s, got %s", ExpectedExample, got.BlockstoreNamespaceExample)
		}
	})
}

func TestController_SetupLakeFSHandler(t *testing.T) {
	name := "admin"
	cases := []struct {
		name string
		user models.Setup
		// Currently only test failure with SetupLakeFSDefault, further testing is by
		// HTTP status code
		errorDefaultCode int
	}{
		{name: "simple", user: models.Setup{Username: &name}},
		{
			name: "accessKeyAndSecret",
			user: models.Setup{
				Username: &name,
				Key: &models.SetupKey{
					AccessKeyID:     swag.String("IKEAsneakers"),
					SecretAccessKey: swag.String("cetec astronomy"),
				},
			},
		},
		{
			name: "emptyAccessKeyId",
			user: models.Setup{
				Username: &name,
				Key:      &models.SetupKey{SecretAccessKey: swag.String("cetec astronomy")},
			},
			errorDefaultCode: 422,
		},
		{
			name: "emptySecretKey", user: models.Setup{
				Username: &name,
				Key: &models.SetupKey{
					AccessKeyID: swag.String("IKEAsneakers"),
				},
			},
			errorDefaultCode: 422,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clt, deps := setupClient(t, "", testutil.WithGetDBApplyDDL(false))
			t.Run("fresh start", func(t *testing.T) {
				res, err := clt.Setup.SetupLakeFS(setup.NewSetupLakeFSParamsWithTimeout(timeout).WithUser(&c.user))
				if c.errorDefaultCode != 0 {
					var defaultErr *setup.SetupLakeFSDefault
					if errors.As(err, &defaultErr) {
						if defaultErr.Code() != c.errorDefaultCode {
							t.Errorf("got default error with code %d, expected %d", defaultErr.Code(), c.errorDefaultCode)
						}
					} else {
						t.Errorf("got %s instead of default error", err)
					}
					return
				}
				if err != nil {
					t.Fatal("setup lakeFS:", err)
				}

				creds := res.Payload
				bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

				if len(creds.AccessKeyID) == 0 {
					t.Fatal("Credential key id is missing")
				}

				if c.user.Key != nil {
					if accessKeyID := swag.StringValue(c.user.Key.AccessKeyID); accessKeyID != creds.AccessKeyID {
						t.Errorf("got access key ID %s != %s", creds.AccessKeyID, accessKeyID)
					}
					if secretAccessKey := swag.StringValue(c.user.Key.SecretAccessKey); secretAccessKey != creds.AccessSecretKey {
						t.Errorf("got secret key %s != %s", creds.AccessSecretKey, secretAccessKey)
					}
				}

				foundCreds, err := clt.Auth.GetCredentials(
					auth.NewGetCredentialsParamsWithTimeout(timeout).
						WithAccessKeyID(creds.AccessKeyID).
						WithUserID(swag.StringValue(c.user.Username)),
					bauth)
				if err != nil {
					t.Fatal("Get API credentials key id for created access key", err)
				}
				if foundCreds == nil {
					t.Fatal("Get API credentials secret key for created access key")
				}
				if foundCreds.Payload.AccessKeyID != creds.AccessKeyID {
					t.Fatalf("Access key ID '%s', expected '%s'", foundCreds.Payload.AccessKeyID, creds.AccessKeyID)
				}
				if len(deps.collector.metadata) != 1 {
					t.Fatal("Failed to collect metadata")
				}
				if deps.collector.metadata[0].InstallationID == "" {
					t.Fatal("Empty installationID")
				}
				if len(deps.collector.metadata[0].Entries) < 5 {
					t.Fatalf("There should be at least 5 metadata entries: %s", deps.collector.metadata[0].Entries)
				}

				hasBlockStoreType := false
				for _, ent := range deps.collector.metadata[0].Entries {
					if ent.Name == stats.BlockstoreTypeKey {
						hasBlockStoreType = true
						if ent.Value == "" {
							t.Fatalf("Blockstorage key exists but with empty value: %s", deps.collector.metadata[0].Entries)
						}
						break
					}
				}
				if !hasBlockStoreType {
					t.Fatalf("missing blockstorage key: %s", deps.collector.metadata[0].Entries)
				}
			})

			if c.errorDefaultCode == 0 {
				// now we ask again - should get status conflict
				t.Run("existing setup", func(t *testing.T) {
					// request to setup
					res, err := clt.Setup.SetupLakeFS(setup.NewSetupLakeFSParamsWithTimeout(timeout).WithUser(&c.user))
					var fsConflict *setup.SetupLakeFSConflict
					if !errors.As(err, &fsConflict) {
						t.Errorf("repeated setup got %+v, %v instead of \"setupLakeFSConflict\"", res, err)
					}
				})
			}
		})
	}
}
