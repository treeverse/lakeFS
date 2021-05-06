package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	DefaultUserID = "example_user"
)

type Statuser interface {
	StatusCode() int
}

func verifyResponseOK(t testing.TB, resp Statuser, err error) {
	t.Helper()
	if err != nil {
		t.Fatal("request failed with error:", err)
	}
	if resp == nil {
		t.Fatal("request's response is missing")
	}
	statusCode := resp.StatusCode()
	if !api.IsStatusCodeOK(statusCode) {
		t.Fatal("request response failed with code", statusCode)
	}
}

func onBlock(deps *dependencies, path string) string {
	return fmt.Sprintf("%s://%s", deps.blocks.BlockstoreType(), path)
}

func TestController_ListRepositoriesHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	t.Run("list no repos", func(t *testing.T) {
		resp, err := clt.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})

		if err != nil {
			t.Fatal(err)
		}

		if resp.JSON200 == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.JSON200.Results) != 0 {
			t.Fatalf("expected 0 repositories, got %d", len(resp.JSON200.Results))
		}
	})

	t.Run("list some repos", func(t *testing.T) {
		// write some repos
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "foo1", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)
		_, err = deps.catalog.CreateRepository(ctx, "foo2", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)
		_, err = deps.catalog.CreateRepository(ctx, "foo3", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)

		resp, err := clt.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
		if err != nil {
			t.Fatal(err)
		}

		if resp.JSON200 == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.JSON200.Results) != 3 {
			t.Fatalf("expected 3 repositories, got %d", len(resp.JSON200.Results))
		}
	})

	t.Run("paginate repos", func(t *testing.T) {
		// write some repos
		resp, err := clt.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{
			Amount: api.PaginationAmountPtr(2),
		})
		if err != nil {
			t.Fatal(err)
		}

		if resp.JSON200 == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.JSON200.Results) != 2 {
			t.Fatalf("expected 3 repositories, got %d", len(resp.JSON200.Results))
		}

		if !resp.JSON200.Pagination.HasMore {
			t.Fatalf("expected more results from paginator, got none")
		}
	})

	t.Run("paginate repos after", func(t *testing.T) {
		// write some repos
		resp, err := clt.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{
			After:  api.PaginationAfterPtr("foo2"),
			Amount: api.PaginationAmountPtr(2),
		})
		if err != nil {
			t.Fatal(err)
		}

		if resp.JSON200 == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.JSON200.Results) != 1 {
			t.Fatalf("expected 1 repository, got %d", len(resp.JSON200.Results))
		}

		if resp.JSON200.Pagination.HasMore {
			t.Fatalf("expected no more results from paginator")
		}

		if resp.JSON200.Results[0].Id != "foo3" {
			t.Fatalf("expected last pagination result to be foo3, got %s instead",
				resp.JSON200.Results[0].Id)
		}
	})
}

func TestController_GetRepoHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	t.Run("get missing repo", func(t *testing.T) {
		resp, err := clt.GetRepositoryWithResponse(ctx, "foo1")
		testutil.Must(t, err)
		if resp == nil {
			t.Fatal("GetRepository missing response")
		}
		if resp.JSON404 == nil {
			t.Fatal("get missing repository should return 404, got:", resp.HTTPResponse)
		}
	})

	t.Run("get existing repo", func(t *testing.T) {
		const testBranchName = "non-default"
		_, err := deps.catalog.CreateRepository(context.Background(), "foo1", onBlock(deps, "foo1"), testBranchName)
		testutil.Must(t, err)

		resp, err := clt.GetRepositoryWithResponse(ctx, "foo1")
		verifyResponseOK(t, resp, err)

		repository := resp.JSON200
		if repository.DefaultBranch != testBranchName {
			t.Fatalf("unexpected branch name %s, expected %s", repository.DefaultBranch, testBranchName)
		}
	})
}

func TestController_CommitsGetBranchCommitLogHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	t.Run("get missing branch", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "ns1"), "main")
		testutil.Must(t, err)

		resp, err := clt.LogCommitsWithResponse(ctx, "repo1", "otherbranch", &api.LogCommitsParams{})
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatalf("expected error getting a branch that doesn't exist")
		}
	})

	t.Run("get branch log", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo2", onBlock(deps, "ns1"), "main")
		testutil.Must(t, err)

		const commitsLen = 2
		for i := 0; i < commitsLen; i++ {
			n := strconv.Itoa(i + 1)
			p := "foo/bar" + n
			err := deps.catalog.CreateEntry(ctx, "repo2", "main", catalog.DBEntry{Path: p, PhysicalAddress: onBlock(deps, "bar"+n+"addr"), CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n})
			testutil.MustDo(t, "create entry "+p, err)
			_, err = deps.catalog.Commit(ctx, "repo2", "main", "commit"+n, "some_user", nil)
			testutil.MustDo(t, "commit "+p, err)
		}
		resp, err := clt.LogCommitsWithResponse(ctx, "repo2", "main", &api.LogCommitsParams{})
		verifyResponseOK(t, resp, err)

		// repo is created with a commit
		const expectedCommits = commitsLen + 1
		commitsLog := resp.JSON200.Results
		if len(commitsLog) != expectedCommits {
			t.Fatalf("Log %d commits, expected %d", len(commitsLog), expectedCommits)
		}
	})
}

func TestController_GetCommitHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	t.Run("get missing commit", func(t *testing.T) {
		resp, err := clt.GetCommitWithResponse(ctx, "foo1", "b0a989d946dca26496b8280ca2bb0a96131a48b362e72f1789e498815992fffa")
		testutil.Must(t, err)
		if resp == nil {
			t.Fatal("expect get missing commit response")
		}
		notFoundResp := resp.JSON404
		if notFoundResp == nil {
			t.Fatalf("expected not found error getting missing commit")
		}
	})

	t.Run("get existing commit", func(t *testing.T) {
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "foo1", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)
		testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, "foo1", "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
		commit1, err := deps.catalog.Commit(ctx, "foo1", "main", "some message", DefaultUserID, nil)
		testutil.Must(t, err)
		reference1, err := deps.catalog.GetBranchReference(ctx, "foo1", "main")
		if err != nil {
			t.Fatal(err)
		}
		if reference1 != commit1.Reference {
			t.Fatalf("Commit reference %s, not equals to branch reference %s", commit1, reference1)
		}
		resp, err := clt.GetCommitWithResponse(ctx, "foo1", commit1.Reference)
		verifyResponseOK(t, resp, err)

		committer := resp.JSON200.Committer
		if committer != DefaultUserID {
			t.Fatalf("unexpected commit id %s, expected %s", committer, DefaultUserID)
		}
	})
}

func TestController_CommitHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	t.Run("commit non-existent commit", func(t *testing.T) {
		resp, err := clt.CommitWithResponse(ctx, "foo1", "main", api.CommitJSONRequestBody{
			Message: "some message",
		})
		testutil.Must(t, err)
		if resp == nil {
			t.Fatal("Commit() expected response")
		}
		notFoundResp := resp.JSON404
		if notFoundResp == nil {
			t.Fatal("expected not found response on missing commit repo")
		}
	})

	t.Run("commit success", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "foo1", onBlock(deps, "foo1"), "main")
		testutil.MustDo(t, "create repo foo1", err)
		testutil.MustDo(t, "commit bar on foo1", deps.catalog.CreateEntry(ctx, "foo1", "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		resp, err := clt.CommitWithResponse(ctx, "foo1", "main", api.CommitJSONRequestBody{
			Message: "some message",
		})
		verifyResponseOK(t, resp, err)
	})
}

func TestController_CreateRepositoryHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()
	t.Run("create repo success", func(t *testing.T) {
		resp, err := clt.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
			DefaultBranch:    api.StringPtr("main"),
			Name:             "my-new-repo",
			StorageNamespace: onBlock(deps, "foo-bucket"),
		})
		verifyResponseOK(t, resp, err)

		repository := resp.JSON201
		if repository.Id != "my-new-repo" {
			t.Fatalf("got unexpected repo when creating my-new-repo: %s", repository.Id)
		}
	})

	t.Run("create repo duplicate", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo2", onBlock(deps, "foo1"), "main")
		if err != nil {
			t.Fatal(err)
		}
		resp, err := clt.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
			DefaultBranch:    api.StringPtr("main"),
			Name:             "repo2",
			StorageNamespace: onBlock(deps, "foo-bucket"),
		})
		if resp == nil {
			t.Fatal("CreateRepository missing response")
		}
		validationErrResp := resp.JSON409
		if validationErrResp == nil {
			t.Fatalf("expected error creating duplicate repo")
		}
	})
}

func TestController_DeleteRepositoryHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	t.Run("delete repo success", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "my-new-repo", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)

		resp, err := clt.DeleteRepositoryWithResponse(ctx, "my-new-repo")
		verifyResponseOK(t, resp, err)

		_, err = deps.catalog.GetRepository(ctx, "my-new-repo")
		if !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("expected repo to be gone, instead got error: %s", err)
		}
	})

	t.Run("delete repo doesnt exist", func(t *testing.T) {
		resp, err := clt.DeleteRepositoryWithResponse(ctx, "my-other-repo")
		testutil.Must(t, err)
		if resp.StatusCode() == http.StatusOK {
			t.Fatalf("DeleteRepository should fail on non existing repository, got %d", resp.StatusCode())
		}
	})

	t.Run("delete repo doesnt delete other repos", func(t *testing.T) {
		names := []string{"rr0", "rr1", "rr11", "rr2"}
		for _, name := range names {
			_, err := deps.catalog.CreateRepository(ctx, name, onBlock(deps, "foo1"), "main")
			testutil.Must(t, err)
		}

		// delete one repository and check that all rest are there
		resp, err := clt.DeleteRepositoryWithResponse(ctx, "rr1")
		verifyResponseOK(t, resp, err)
		for _, name := range names {
			if name == "rr1" {
				continue
			}
			_, err = deps.catalog.GetRepository(ctx, name)
			if err != nil {
				t.Fatalf("unexpected error getting other repo: %s", err)
			}
		}
	})
}

func TestController_ListBranchesHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	t.Run("list branches only default", func(t *testing.T) {
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)
		resp, err := clt.ListBranchesWithResponse(ctx, "repo1", &api.ListBranchesParams{
			Amount: api.PaginationAmountPtr(-1),
		})
		verifyResponseOK(t, resp, err)

		const expectedBranchesLen = 1
		branchesLen := len(resp.JSON200.Results)
		if branchesLen != expectedBranchesLen {
			t.Fatalf("ListBranches len=%d, expected %d", branchesLen, expectedBranchesLen)
		}
	})

	t.Run("list branches pagination", func(t *testing.T) {
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "repo2", onBlock(deps, "foo2"), "main")
		testutil.Must(t, err)

		// create first dummy commit on main so that we can create branches from it
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo2", "main", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "repo2", "main", "first commit", "test", nil)
		testutil.Must(t, err)

		for i := 0; i < 7; i++ {
			branchName := "main" + strconv.Itoa(i+1)
			_, err := deps.catalog.CreateBranch(ctx, "repo2", branchName, "main")
			testutil.MustDo(t, "create branch "+branchName, err)
		}
		resp, err := clt.ListBranchesWithResponse(ctx, "repo2", &api.ListBranchesParams{
			Amount: api.PaginationAmountPtr(2),
		})
		verifyResponseOK(t, resp, err)
		if len(resp.JSON200.Results) != 2 {
			t.Fatalf("expected 2 branches to return, got %d", len(resp.JSON200.Results))
		}

		resp, err = clt.ListBranchesWithResponse(ctx, "repo2", &api.ListBranchesParams{
			After:  api.PaginationAfterPtr("main1"),
			Amount: api.PaginationAmountPtr(2),
		})
		verifyResponseOK(t, resp, err)
		results := resp.JSON200.Results
		if len(results) != 2 {
			t.Fatalf("expected 2 branches to return, got %d", len(results))
		}
		retReference := results[0]
		const expectedID = "main2"
		if retReference.Id != expectedID {
			t.Fatalf("expected '%s' as the first result for the second page, got '%s' instead", expectedID, retReference.Id)
		}
	})

	t.Run("list branches repo doesnt exist", func(t *testing.T) {
		resp, err := clt.ListBranchesWithResponse(ctx, "repo666", &api.ListBranchesParams{
			Amount: api.PaginationAmountPtr(2),
		})
		testutil.Must(t, err)
		if resp == nil {
			t.Fatal("ListBranches() missing response")
		}
		if resp.JSON404 == nil {
			t.Fatal("expected error calling list branches on repo that doesnt exist")
		}
	})
}

func TestController_ListTagsHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	// setup test data
	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), "main")
	testutil.Must(t, err)
	testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", "main", catalog.DBEntry{Path: "obj1"}))
	commitLog, err := deps.catalog.Commit(ctx, "repo1", "main", "first commit", "test", nil)
	testutil.Must(t, err)
	const createTagLen = 7
	var createdTags []api.Ref
	for i := 0; i < createTagLen; i++ {
		tagID := "tag" + strconv.Itoa(i)
		commitID := commitLog.Reference
		_, err := clt.CreateTagWithResponse(ctx, "repo1", api.CreateTagJSONRequestBody{
			Id:  tagID,
			Ref: commitID,
		})
		testutil.Must(t, err)
		createdTags = append(createdTags, api.Ref{
			Id:       tagID,
			CommitId: commitID,
		})
	}

	t.Run("default", func(t *testing.T) {
		resp, err := clt.ListTagsWithResponse(ctx, "repo1", &api.ListTagsParams{
			Amount: api.PaginationAmountPtr(-1),
		})
		verifyResponseOK(t, resp, err)
		payload := resp.JSON200
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
		var results []api.Ref
		var after string
		var calls int
		for {
			calls++
			resp, err := clt.ListTagsWithResponse(ctx, "repo1", &api.ListTagsParams{
				After:  api.PaginationAfterPtr(after),
				Amount: api.PaginationAmountPtr(pageSize),
			})
			testutil.Must(t, err)
			payload := resp.JSON200
			results = append(results, payload.Results...)
			if !payload.Pagination.HasMore {
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
		resp, err := clt.ListTagsWithResponse(ctx, "repo666", &api.ListTagsParams{})
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatal("ListTags should return not found error")
		}
	})
}

func TestController_GetBranchHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	t.Run("get default branch", func(t *testing.T) {
		const testBranch = "main"
		_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), testBranch)
		testutil.Must(t, err)
		// create first dummy commit on main so that we can create branches from it
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", testBranch, catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "repo1", testBranch, "first commit", "test", nil)
		testutil.Must(t, err)

		resp, err := clt.GetBranchWithResponse(ctx, "repo1", testBranch)
		verifyResponseOK(t, resp, err)
		reference := resp.JSON200
		if reference == nil || reference.CommitId == "" {
			t.Fatalf("Got no reference for branch '%s'", testBranch)
		}
	})

	t.Run("get missing branch", func(t *testing.T) {
		resp, err := clt.GetBranchWithResponse(ctx, "repo1", "main333")
		if err != nil {
			t.Fatal("GetBranch error", err)
		}
		if resp.JSON404 == nil {
			t.Fatal("GetBranch expected not found error")
		}
	})

	t.Run("get branch for missing repo", func(t *testing.T) {
		resp, err := clt.GetBranchWithResponse(ctx, "repo3", "main")
		if err != nil {
			t.Fatal("GetBranch error", err)
		}
		if resp.JSON404 == nil {
			t.Fatal("GetBranch expected not found error")
		}
	})
}

func TestController_BranchesDiffBranchHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")

	ctx := context.Background()
	const testBranch = "main"
	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), testBranch)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("diff branch no changes", func(t *testing.T) {
		resp, err := clt.DiffBranchWithResponse(ctx, "repo1", testBranch, &api.DiffBranchParams{})
		verifyResponseOK(t, resp, err)
		changes := len(resp.JSON200.Results)
		if changes != 0 {
			t.Fatalf("expected no diff results, got %d", changes)
		}
	})

	t.Run("diff branch with writes", func(t *testing.T) {
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", testBranch, catalog.DBEntry{Path: "a/b"}))
		resp, err := clt.DiffBranchWithResponse(ctx, "repo1", testBranch, &api.DiffBranchParams{})
		verifyResponseOK(t, resp, err)
		results := resp.JSON200.Results
		if len(results) != 1 {
			t.Fatalf("expected no resp results, got %d", len(results))
		}

		if results[0].Path != "a/b" {
			t.Fatalf("got wrong diff object, expected a/b, got %s", results[0].Path)
		}
	})

	t.Run("diff branch that doesn't exist", func(t *testing.T) {
		resp, err := clt.DiffBranchWithResponse(ctx, "repo1", "some-other-missing-branch", &api.DiffBranchParams{})
		if err != nil {
			t.Fatal("DiffBranch failed:", err)
		}
		if resp.JSON404 == nil {
			t.Fatalf("expected an not found")
		}
	})
}

func TestController_CreateBranchHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()
	t.Run("create branch and diff refs success", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", "main", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "repo1", "main", "first commit", "test", nil)
		testutil.Must(t, err)

		const newBranchName = "main2"
		resp, err := clt.CreateBranchWithResponse(ctx, "repo1", api.CreateBranchJSONRequestBody{
			Name:   newBranchName,
			Source: "main",
		})
		verifyResponseOK(t, resp, err)
		reference := string(resp.Body)
		if len(reference) == 0 {
			t.Fatalf("branch %s creation got no reference", newBranchName)
		}
		const path = "some/path"
		const content = "hello world!"

		uploadResp, _ := uploadObjectHelper(t, ctx, clt, path, strings.NewReader(content), "repo1", newBranchName)
		verifyResponseOK(t, uploadResp, err)

		if _, err := deps.catalog.Commit(ctx, "repo1", "main2", "commit 1", "some_user", nil); err != nil {
			t.Fatalf("failed to commit 'repo1': %s", err)
		}
		resp2, err := clt.DiffRefsWithResponse(ctx, "repo1", newBranchName, "main", &api.DiffRefsParams{})
		verifyResponseOK(t, resp2, err)
		results := resp2.JSON200.Results
		if len(results) != 1 {
			t.Fatalf("unexpected length of results: %d", len(results))
		}
		if results[0].Path != path {
			t.Fatalf("wrong result: %s", results[0].Path)
		}
	})

	t.Run("create branch missing commit", func(t *testing.T) {
		resp, err := clt.CreateBranchWithResponse(ctx, "repo1", api.CreateBranchJSONRequestBody{
			Name:   "main3",
			Source: "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447",
		})
		if err != nil {
			t.Fatal("CreateBranch failed with error:", err)
		}
		if resp.JSON404 == nil {
			t.Fatal("CreateBranch expected to fail with not found")
		}
	})

	t.Run("create branch missing repo", func(t *testing.T) {
		resp, err := clt.CreateBranchWithResponse(ctx, "repo5", api.CreateBranchJSONRequestBody{
			Name:   "main8",
			Source: "main",
		})
		if err != nil {
			t.Fatal("CreateBranch failed with error:", err)
		}
		if resp.JSON404 == nil {
			t.Fatal("CreateBranch expected not found")
		}
	})

	t.Run("create branch conflict", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo6", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)

		resp, err := clt.CreateBranchWithResponse(ctx, "repo6", api.CreateBranchJSONRequestBody{
			Name:   "main",
			Source: "main",
		})
		if err != nil {
			t.Fatal("CreateBranch failed with error:", err)
		}
		if resp.JSON409 == nil {
			t.Fatal("CreateBranch expected conflict")
		}
	})
}

func uploadObjectHelper(t testing.TB, ctx context.Context, clt api.ClientWithResponsesInterface, path string, reader io.Reader, repo, branch string) (*api.UploadObjectResponse, error) {
	t.Helper()

	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	contentWriter, err := w.CreateFormFile("content", filepath.Base(path))
	if err != nil {
		t.Fatal("CreateFormFile:", err)
	}
	if _, err := io.Copy(contentWriter, reader); err != nil {
		t.Fatal("CreateFormFile write content:", err)
	}
	w.Close()

	return clt.UploadObjectWithBodyWithResponse(ctx, repo, branch, &api.UploadObjectParams{
		Path: path,
	}, w.FormDataContentType(), &b)
}

func writeMultipart(fieldName, filename, content string) (string, io.Reader) {
	var buf bytes.Buffer
	mpw := multipart.NewWriter(&buf)
	w, _ := mpw.CreateFormFile("content", "bar")
	_, _ = w.Write([]byte("hello world!"))
	_ = mpw.Close()
	return mpw.FormDataContentType(), &buf
}

func TestController_UploadObject(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "my-new-repo", onBlock(deps, "foo1"), "main")
	testutil.Must(t, err)

	t.Run("upload object", func(t *testing.T) {
		// write
		contentType, buf := writeMultipart("content", "bar", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &api.UploadObjectParams{
			Path: "foo/bar",
		}, contentType, buf)

		testutil.Must(t, err)
		if b.StatusCode() == 500 {
			t.Fatalf("got 500 while uploading: %v", b.JSONDefault)
		}
		if b.StatusCode() != 201 {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}
	})

	t.Run("overwrite", func(t *testing.T) {
		// write first
		contentType, buf := writeMultipart("content", "baz1", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &api.UploadObjectParams{
			Path: "foo/baz1",
		}, contentType, buf)
		testutil.Must(t, err)
		if b.StatusCode() != 201 {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}
		// overwrite
		contentType, buf = writeMultipart("content", "baz1", "something else!")
		b, err = clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &api.UploadObjectParams{
			Path: "foo/baz1",
		}, contentType, buf)

		testutil.Must(t, err)
		if b.StatusCode() != 201 {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}
	})

	t.Run("disable overwrite with if-none-match (uncommitted entry)", func(t *testing.T) {
		// write first
		contentType, buf := writeMultipart("content", "baz2", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &api.UploadObjectParams{
			Path: "foo/baz2",
		}, contentType, buf)
		testutil.Must(t, err)
		if b.StatusCode() != 201 {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}
		// overwrite
		contentType, buf = writeMultipart("content", "baz2", "something else!")
		all := "*"
		b, err = clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &api.UploadObjectParams{
			Path:        "foo/baz2",
			IfNoneMatch: &all,
		}, contentType, buf)

		testutil.Must(t, err)
		if b.StatusCode() != 412 {
			t.Fatalf("expected 412 for UploadObject, got %d", b.StatusCode())
		}
	})

	t.Run("disable overwrite with if-none-match (committed entry)", func(t *testing.T) {
		_, err := deps.catalog.CreateBranch(ctx, "my-new-repo", "another-branch", "main")
		testutil.Must(t, err)

		// write first
		contentType, buf := writeMultipart("content", "baz3", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "another-branch", &api.UploadObjectParams{
			Path: "foo/baz3",
		}, contentType, buf)
		testutil.Must(t, err)
		if b.StatusCode() != 201 {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}

		// commit
		_, err = deps.catalog.Commit(ctx, "my-new-repo", "another-branch", "a commit!", "user1", nil)
		testutil.Must(t, err)

		// overwrite after commit
		all := "*"
		contentType, buf = writeMultipart("content", "baz3", "something else!")
		b, err = clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "another-branch", &api.UploadObjectParams{
			Path:        "foo/baz3",
			IfNoneMatch: &all,
		}, contentType, buf)

		testutil.Must(t, err)
		if b.StatusCode() != 412 {
			t.Fatalf("expected 412 for UploadObject, got %d", b.StatusCode())
		}
	})
}

func TestController_DeleteBranchHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	t.Run("delete branch success", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "my-new-repo", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "my-new-repo", "main", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "my-new-repo", "main", "first commit", "test", nil)
		testutil.Must(t, err)

		_, err = deps.catalog.CreateBranch(ctx, "my-new-repo", "main2", "main")
		if err != nil {
			t.Fatal(err)
		}

		delResp, err := clt.DeleteBranchWithResponse(ctx, "my-new-repo", "main2")
		verifyResponseOK(t, delResp, err)

		_, err = deps.catalog.GetBranchReference(ctx, "my-new-repo", "main2")
		if !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("expected branch to be gone, instead got error: %s", err)
		}
	})

	t.Run("delete branch doesnt exist", func(t *testing.T) {
		resp, err := clt.DeleteBranchWithResponse(ctx, "my-new-repo", "main5")
		if err != nil {
			t.Fatal("DeleteBranch error:", err)
		}
		if resp.JSON404 == nil {
			t.Fatal("DeleteBranch expected not found")
		}
	})
}

func TestController_ObjectsStatObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "some-bucket"), "main")
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
			deps.catalog.CreateEntry(ctx, "repo1", "main", entry))
		if err != nil {
			t.Fatal(err)
		}

		resp, err := clt.StatObjectWithResponse(ctx, "repo1", "main", &api.StatObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, resp, err)
		objectStats := resp.JSON200
		if objectStats.Path != entry.Path {
			t.Fatalf("expected to get back our path, got %s", objectStats.Path)
		}
		if api.Int64Value(objectStats.SizeBytes) != entry.Size {
			t.Fatalf("expected correct size, got %d", objectStats.SizeBytes)
		}
		if objectStats.PhysicalAddress != onBlock(deps, "some-bucket/")+entry.PhysicalAddress {
			t.Fatalf("expected correct PhysicalAddress, got %s", objectStats.PhysicalAddress)
		}
	})
}

func TestController_ObjectsListObjectsHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "bucket/prefix"), "main")
	testutil.Must(t, err)
	dbEntries := []catalog.DBEntry{
		{
			Path:            "foo/bar",
			PhysicalAddress: "this_is_bars_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "this_is_a_checksum",
		},
		{
			Path:            "foo/quuux",
			PhysicalAddress: "this_is_quuxs_address_expired",
			CreationDate:    time.Now(),
			Size:            9999999,
			Checksum:        "quux_checksum",
			Expired:         true,
		},
		{
			Path:            "foo/baz",
			PhysicalAddress: "this_is_bazs_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "baz_checksum",
		},
		{
			Path:            "foo/a_dir/baz",
			PhysicalAddress: "this_is_bazs_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "baz_checksum",
		},
	}
	for _, entry := range dbEntries {
		err := deps.catalog.CreateEntry(ctx, "repo1", "main", entry)
		testutil.Must(t, err)
	}

	t.Run("get object list", func(t *testing.T) {
		resp, err := clt.ListObjectsWithResponse(ctx, "repo1", "main", &api.ListObjectsParams{
			Prefix: api.StringPtr("foo/"),
		})
		verifyResponseOK(t, resp, err)
		results := resp.JSON200.Results
		if len(results) != 4 {
			t.Fatalf("expected 4 entries, got back %d", len(results))
		}
	})

	t.Run("get object list paginated", func(t *testing.T) {
		resp, err := clt.ListObjectsWithResponse(ctx, "repo1", "main", &api.ListObjectsParams{
			Prefix: api.StringPtr("foo/"),
			Amount: api.PaginationAmountPtr(2),
		})
		verifyResponseOK(t, resp, err)
		if len(resp.JSON200.Results) != 2 {
			t.Fatalf("expected 3 entries, got back %d", len(resp.JSON200.Results))
		}
		if !resp.JSON200.Pagination.HasMore {
			t.Fatalf("expected paginator.HasMore to be true")
		}

		if resp.JSON200.Pagination.NextOffset != "foo/bar" {
			t.Fatalf("expected next offset to be foo/bar, got %s", resp.JSON200.Pagination.NextOffset)
		}
	})
}

func TestController_ObjectsGetObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", "ns1", "main")
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
		deps.catalog.CreateEntry(ctx, "repo1", "main", entry))

	expired := catalog.DBEntry{
		Path:            "foo/expired",
		PhysicalAddress: "an_expired_physical_address",
		CreationDate:    time.Now(),
		Size:            99999,
		Checksum:        "b10b",
		Expired:         true,
	}
	testutil.Must(t,
		deps.catalog.CreateEntry(ctx, "repo1", "main", expired))

	t.Run("get object", func(t *testing.T) {
		resp, err := clt.GetObjectWithResponse(ctx, "repo1", "main", &api.GetObjectParams{Path: "foo/bar"})
		if err != nil {
			t.Fatal(err)
		}
		if resp.HTTPResponse.StatusCode != http.StatusOK {
			t.Fatalf("GetObject() status code %d, expected %d", resp.HTTPResponse.StatusCode, http.StatusOK)
		}

		if resp.HTTPResponse.ContentLength != 37 {
			t.Fatalf("expected 37 bytes in content length, got back %d", resp.HTTPResponse.ContentLength)
		}
		etag := resp.HTTPResponse.Header.Get("ETag")
		if etag != `"3c4838fe975c762ee97cf39fbbe566f1"` {
			t.Fatalf("got unexpected etag: %s", etag)
		}

		body := string(resp.Body)
		if body != "this is file content made up of bytes" {
			t.Fatalf("got unexpected body: '%s'", body)
		}
	})

	t.Run("get properties", func(t *testing.T) {
		resp, err := clt.GetUnderlyingPropertiesWithResponse(ctx, "repo1", "main", &api.GetUnderlyingPropertiesParams{Path: "foo/bar"})
		if err != nil {
			t.Fatalf("expected to get underlying properties, got %v", err)
		}
		properties := resp.JSON200
		if properties == nil {
			t.Fatalf("expected to get underlying properties, status code %d", resp.HTTPResponse.StatusCode)
		}

		if api.StringValue(properties.StorageClass) != expensiveString {
			t.Errorf("expected to get \"%s\" storage class, got %#v", expensiveString, properties)
		}
	})
}

func TestController_ObjectsUploadObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "bucket/prefix"), "main")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("upload object", func(t *testing.T) {
		const content = "hello world this is my awesome content"
		resp, err := uploadObjectHelper(t, ctx, clt, "foo/bar", strings.NewReader(content), "repo1", "main")
		verifyResponseOK(t, resp, err)

		sizeBytes := api.Int64Value(resp.JSON201.SizeBytes)
		const expectedSize = 38
		if sizeBytes != expectedSize {
			t.Fatalf("expected %d bytes to be written, got back %d", expectedSize, sizeBytes)
		}

		// download it
		rresp, err := clt.GetObjectWithResponse(ctx, "repo1", "main", &api.GetObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, rresp, err)
		result := string(rresp.Body)
		if len(result) != expectedSize {
			t.Fatalf("expected %d bytes to be read, got back %d", expectedSize, len(result))
		}
		etag := rresp.HTTPResponse.Header.Get("ETag")
		const expectedEtag = "7e70ed4aa82063dd88ca47e91a8c6e09"
		if etag != httputil.ETag(expectedEtag) {
			t.Fatalf("got unexpected etag: %s - expected %s", etag, httputil.ETag(expectedEtag))
		}
	})

	t.Run("upload object missing branch", func(t *testing.T) {
		const content = "hello world this is my awesome content"
		resp, err := uploadObjectHelper(t, ctx, clt, "foo/bar", strings.NewReader(content), "repo1", "mainX")
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatal("Missing branch should return not found")
		}
	})

	t.Run("upload object missing repo", func(t *testing.T) {
		const content = "hello world this is my awesome content"
		resp, err := uploadObjectHelper(t, ctx, clt, "foo/bar", strings.NewReader(content), "repo55555", "main")
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatal("Missing repository should return not found")
		}
	})
}

func TestController_ObjectsStageObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "s3")
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "bucket/prefix"), "main")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("stage object", func(t *testing.T) {
		const expectedSizeBytes = 38
		resp, err := clt.StageObjectWithResponse(ctx, "repo1", "main", &api.StageObjectParams{Path: "foo/bar"}, api.StageObjectJSONRequestBody{
			Checksum:        "afb0689fe58b82c5f762991453edbbec",
			PhysicalAddress: onBlock(deps, "another-bucket/some/location"),
			SizeBytes:       expectedSizeBytes,
		})
		verifyResponseOK(t, resp, err)

		sizeBytes := api.Int64Value(resp.JSON201.SizeBytes)
		if sizeBytes != expectedSizeBytes {
			t.Fatalf("expected %d bytes to be written, got back %d", expectedSizeBytes, sizeBytes)
		}

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, "repo1", "main", &api.StatObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, statResp, err)
		objectStat := statResp.JSON200
		if objectStat.PhysicalAddress != onBlock(deps, "another-bucket/some/location") {
			t.Fatalf("unexpected physical address: %s", objectStat.PhysicalAddress)
		}
	})

	t.Run("upload object missing branch", func(t *testing.T) {
		resp, err := clt.StageObjectWithResponse(ctx, "repo1", "main1234", &api.StageObjectParams{Path: "foo/bar"},
			api.StageObjectJSONRequestBody{
				Checksum:        "afb0689fe58b82c5f762991453edbbec",
				PhysicalAddress: onBlock(deps, "another-bucket/some/location"),
				SizeBytes:       38,
			})
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatal("Missing branch should return not found")
		}
	})

	t.Run("wrong storage adapter", func(t *testing.T) {
		resp, err := clt.StageObjectWithResponse(ctx, "repo1", "main1234", &api.StageObjectParams{
			Path: "foo/bar",
		}, api.StageObjectJSONRequestBody{
			Checksum:        "afb0689fe58b82c5f762991453edbbec",
			PhysicalAddress: "gs://another-bucket/some/location",
			SizeBytes:       38,
		})
		testutil.Must(t, err)
		if resp.JSON400 == nil {
			t.Fatalf("Wrong storage adapter should return 400, got status %s [%d]\n\tbody: %s", resp.Status(), resp.StatusCode(), string(resp.Body))
		}
	})
}

func TestController_ObjectsDeleteObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "")
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "some-bucket/prefix"), "main")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("delete object", func(t *testing.T) {
		const content = "hello world this is my awesome content"
		resp, err := uploadObjectHelper(t, ctx, clt, "foo/bar", strings.NewReader(content), "repo1", "main")
		verifyResponseOK(t, resp, err)

		sizeBytes := api.Int64Value(resp.JSON201.SizeBytes)
		if sizeBytes != 38 {
			t.Fatalf("expected 38 bytes to be written, got back %d", sizeBytes)
		}

		// download it
		rresp, err := clt.GetObjectWithResponse(ctx, "repo1", "main", &api.GetObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, rresp, err)
		result := string(rresp.Body)
		if len(result) != 38 {
			t.Fatalf("expected 38 bytes to be read, got back %d", len(result))
		}
		etag := rresp.HTTPResponse.Header.Get("ETag")
		const expectedEtag = "7e70ed4aa82063dd88ca47e91a8c6e09"
		if etag != httputil.ETag(expectedEtag) {
			t.Fatalf("got unexpected etag: %s - expected %s", etag, httputil.ETag(expectedEtag))
		}

		// delete it
		delResp, err := clt.DeleteObjectWithResponse(ctx, "repo1", "main", &api.DeleteObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, delResp, err)

		// get it
		statResp, err := clt.StatObjectWithResponse(ctx, "repo1", "main", &api.StatObjectParams{Path: "foo/bar"})
		testutil.Must(t, err)
		if statResp == nil {
			t.Fatal("StatObject missing response")
		}
		if statResp.JSON404 == nil {
			t.Fatalf("expected file to be gone now")
		}
	})
}

func TestController_CreatePolicyHandler(t *testing.T) {
	clt, _ := setupClientWithAdmin(t, "")
	ctx := context.Background()
	t.Run("valid_policy", func(t *testing.T) {
		resp, err := clt.CreatePolicyWithResponse(ctx, api.CreatePolicyJSONRequestBody{
			CreationDate: api.Int64Ptr(time.Now().Unix()),
			Id:           "ValidPolicyID",
			Statement: []api.Statement{
				{
					Action:   []string{"fs:ReadObject"},
					Effect:   "allow",
					Resource: "arn:lakefs:fs:::repository/foo/object/*",
				},
			},
		})
		verifyResponseOK(t, resp, err)
	})

	t.Run("invalid_policy_action", func(t *testing.T) {
		resp, err := clt.CreatePolicyWithResponse(ctx, api.CreatePolicyJSONRequestBody{
			CreationDate: api.Int64Ptr(time.Now().Unix()),
			Id:           "ValidPolicyID",
			Statement: []api.Statement{
				{
					Action:   []string{"fsx:ReadObject"},
					Effect:   "allow",
					Resource: "arn:lakefs:fs:::repository/foo/object/*",
				},
			},
		})
		testutil.Must(t, err)
		if resp.HTTPResponse.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected error creating invalid policy: action")
		}
	})

	t.Run("invalid_policy_effect", func(t *testing.T) {
		resp, err := clt.CreatePolicyWithResponse(ctx, api.CreatePolicyJSONRequestBody{
			CreationDate: api.Int64Ptr(time.Now().Unix()),
			Id:           "ValidPolicyID",
			Statement: []api.Statement{
				{
					Action:   []string{"fs:ReadObject"},
					Effect:   "Allow",
					Resource: "arn:lakefs:fs:::repository/foo/object/*",
				},
			},
		})
		testutil.Must(t, err)
		// middleware validates possible values so we match the http response
		if resp.HTTPResponse.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected error creating invalid policy: effect")
		}
	})

	t.Run("invalid_policy_arn", func(t *testing.T) {
		resp, err := clt.CreatePolicyWithResponse(ctx, api.CreatePolicyJSONRequestBody{
			CreationDate: api.Int64Ptr(time.Now().Unix()),
			Id:           "ValidPolicyID",
			Statement: []api.Statement{
				{
					Action:   []string{"fs:ReadObject"},
					Effect:   "Allow",
					Resource: "arn:lakefs:fs:repository/foo/object/*",
				},
			},
		})
		testutil.Must(t, err)
		if resp.HTTPResponse.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected error creating invalid policy: arn")
		}
	})
}

func TestController_ConfigHandlers(t *testing.T) {
	clt, deps := setupClientWithAdmin(t, "s3")
	ctx := context.Background()

	var ExpectedExample = onBlock(deps, "example-bucket/")

	t.Run("Get config (currently only block store type)", func(t *testing.T) {
		resp, err := clt.GetConfigWithResponse(ctx)
		verifyResponseOK(t, resp, err)

		example := resp.JSON200.BlockstoreNamespaceExample
		if example != ExpectedExample {
			t.Errorf("expected to get %s, got %s", ExpectedExample, example)
		}
	})
}

func TestController_SetupLakeFSHandler(t *testing.T) {
	cases := []struct {
		name               string
		key                *api.AccessKeyCredentials
		expectedStatusCode int
	}{
		{
			name:               "simple",
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "accessKeyAndSecret",
			key: &api.AccessKeyCredentials{
				AccessKeyId:     "IKEAsneakers",
				SecretAccessKey: "cetec astronomy",
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "emptyAccessKeyId",
			key: &api.AccessKeyCredentials{
				SecretAccessKey: "cetec astronomy",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "emptySecretKey",
			key: &api.AccessKeyCredentials{
				AccessKeyId: "IKEAsneakers",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			handler, deps := setupHandler(t, "")
			server := setupServer(t, handler)
			clt := setupClientByEndpoint(t, server.URL, "", "")

			t.Run("fresh start", func(t *testing.T) {
				ctx := context.Background()
				resp, err := clt.SetupWithResponse(ctx, api.SetupJSONRequestBody{
					Username: "admin",
					Key:      c.key,
				})
				testutil.Must(t, err)
				if resp.HTTPResponse.StatusCode != c.expectedStatusCode {
					t.Fatalf("got status code %d, expected %d", resp.HTTPResponse.StatusCode, c.expectedStatusCode)
				}
				if resp.JSON200 == nil {
					return
				}

				creds := resp.JSON200
				if len(creds.AccessKeyId) == 0 {
					t.Fatal("Credential key id is missing")
				}

				if c.key != nil && c.key.AccessKeyId != creds.AccessKeyId {
					t.Errorf("got access key ID %s != %s", creds.AccessKeyId, c.key.AccessKeyId)
				}
				if c.key != nil && c.key.SecretAccessKey != creds.SecretAccessKey {
					t.Errorf("got secret access key %s != %s", creds.SecretAccessKey, c.key.SecretAccessKey)
				}

				clt = setupClientByEndpoint(t, server.URL, creds.AccessKeyId, creds.SecretAccessKey)
				getCredResp, err := clt.GetCredentialsWithResponse(ctx, "admin", creds.AccessKeyId)
				verifyResponseOK(t, getCredResp, err)
				if err != nil {
					t.Fatal("Get API credentials key id for created access key", err)
				}
				foundCreds := getCredResp.JSON200
				if foundCreds == nil {
					t.Fatal("Get API credentials secret key for created access key")
				}
				if foundCreds.AccessKeyId != creds.AccessKeyId {
					t.Fatalf("Access key ID '%s', expected '%s'", foundCreds.AccessKeyId, creds.AccessKeyId)
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

			if c.expectedStatusCode == http.StatusOK {
				// now we ask again - should get status conflict
				t.Run("existing setup", func(t *testing.T) {
					// request to setup
					ctx := context.Background()
					res, err := clt.SetupWithResponse(ctx, api.SetupJSONRequestBody{
						Username: "admin",
					})
					testutil.Must(t, err)
					if res.JSON409 == nil {
						t.Error("repeated setup didn't got conflict response")
					}
				})
			}
		})
	}
}

var listRepositoryRunsActionTemplate = template.Must(template.New("").Parse(`---
name: CommitAction
on:
  pre-commit:
    branches:
      - "*"
hooks:
  - id: hook1
    type: webhook
    properties:
      url: {{.URL}}
`))

func TestController_ListRepositoryRuns(t *testing.T) {
	clt, _ := setupClientWithAdmin(t, "")
	ctx := context.Background()
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer httpServer.Close()
	// create repository
	resp, err := clt.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
		DefaultBranch:    api.StringPtr("main"),
		Name:             "repo9",
		StorageNamespace: "mem://repo9",
	})
	verifyResponseOK(t, resp, err)
	// upload action for pre-commit
	var b bytes.Buffer
	testutil.MustDo(t, "execute action template", listRepositoryRunsActionTemplate.Execute(&b, httpServer))
	actionContent := b.String()
	uploadResp, err := uploadObjectHelper(t, ctx, clt, "_lakefs_actions/pre_commit.yaml", strings.NewReader(actionContent), "repo9", "main")
	verifyResponseOK(t, uploadResp, err)
	// commit
	respCommit, err := clt.CommitWithResponse(ctx, "repo9", "main", api.CommitJSONRequestBody{
		Message: "pre-commit action",
	})
	verifyResponseOK(t, respCommit, err)
	// work branch
	branchResp, err := clt.CreateBranchWithResponse(ctx, "repo9", api.CreateBranchJSONRequestBody{
		Name:   "work",
		Source: "main",
	})
	verifyResponseOK(t, branchResp, err)
	// upload and commit content on branch
	commitIDs := []string{respCommit.JSON201.Id}
	const contentCount = 5
	for i := 0; i < contentCount; i++ {
		content := fmt.Sprintf("content-%d", i)
		uploadResp, err := uploadObjectHelper(t, ctx, clt, content, strings.NewReader(content), "repo9", "work")
		verifyResponseOK(t, uploadResp, err)
		respCommit, err := clt.CommitWithResponse(ctx, "repo9", "work", api.CommitJSONRequestBody{Message: content})
		verifyResponseOK(t, respCommit, err)
		commitIDs = append(commitIDs, respCommit.JSON201.Id)
	}

	t.Run("total", func(t *testing.T) {
		respList, err := clt.ListRepositoryRunsWithResponse(ctx, "repo9", &api.ListRepositoryRunsParams{
			Amount: api.PaginationAmountPtr(100),
		})
		verifyResponseOK(t, respList, err)
		runsCount := len(respList.JSON200.Results)
		if runsCount != contentCount+1 {
			t.Fatalf("ListRepositoryRuns() got %d results, expected %d+1", runsCount, contentCount)
		}
	})

	t.Run("on branch", func(t *testing.T) {
		respList, err := clt.ListRepositoryRunsWithResponse(ctx, "repo9", &api.ListRepositoryRunsParams{
			Branch: api.StringPtr("work"),
			Amount: api.PaginationAmountPtr(100),
		})
		verifyResponseOK(t, respList, err)
		runsCount := len(respList.JSON200.Results)
		if runsCount != contentCount {
			t.Fatalf("ListRepositoryRuns() on `work` branch got %d results, expected %d", runsCount, contentCount)
		}
	})

	t.Run("on deleted branch", func(t *testing.T) {
		// delete work branch and list them again
		delResp, err := clt.DeleteBranchWithResponse(ctx, "repo9", "work")
		verifyResponseOK(t, delResp, err)

		respList, err := clt.ListRepositoryRunsWithResponse(ctx, "repo9", &api.ListRepositoryRunsParams{
			Branch: api.StringPtr("work"),
			Amount: api.PaginationAmountPtr(100),
		})
		verifyResponseOK(t, respList, err)
		runsCount := len(respList.JSON200.Results)
		if runsCount != contentCount {
			t.Fatalf("ListRepositoryRuns() got %d results, expected %d (after delete repository)", runsCount, contentCount)
		}
	})
}

func TestController_MergeDiffWithParent(t *testing.T) {
	clt, _ := setupClientWithAdmin(t, "")
	ctx := context.Background()

	const repoName = "repo7"
	repoResp, err := clt.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
		DefaultBranch:    api.StringPtr("main"),
		Name:             repoName,
		StorageNamespace: "mem://",
	})
	verifyResponseOK(t, repoResp, err)

	branchResp, err := clt.CreateBranchWithResponse(ctx, repoName, api.CreateBranchJSONRequestBody{Name: "work", Source: "main"})
	verifyResponseOK(t, branchResp, err)

	const content = "awesome content"
	resp, err := uploadObjectHelper(t, ctx, clt, "file1", strings.NewReader(content), repoName, "work")
	verifyResponseOK(t, resp, err)

	commitResp, err := clt.CommitWithResponse(ctx, repoName, "work", api.CommitJSONRequestBody{Message: "file 1 commit to work"})
	verifyResponseOK(t, commitResp, err)

	mergeResp, err := clt.MergeIntoBranchWithResponse(ctx, repoName, "work", "main", api.MergeIntoBranchJSONRequestBody{
		Message: api.StringPtr("merge work to main"),
	})
	verifyResponseOK(t, mergeResp, err)

	diffResp, err := clt.DiffRefsWithResponse(ctx, repoName, "main", "main~1", &api.DiffRefsParams{})
	verifyResponseOK(t, diffResp, err)
	expectedResults := []api.Diff{
		{Path: "file1", PathType: "object", Type: "added"},
	}
	if diff := deep.Equal(diffResp.JSON200.Results, expectedResults); diff != nil {
		t.Fatal("Diff results not as expected:", diff)
	}
}
