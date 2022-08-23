package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/go-test/deep"
	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
	"github.com/treeverse/lakefs/pkg/graveler"
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

type commitEntriesParams struct {
	repo         string
	branch       string
	filesVersion int64
	paths        []string
	user         string
	commitName   string
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

func testControllerWithKV(t *testing.T, kvEnabled bool) {
	testController_ListRepositoriesHandler(t, kvEnabled)
	testController_GetRepoHandler(t, kvEnabled)
	testController_LogCommitsHandler(t, kvEnabled)
	testController_CommitsGetBranchCommitLogByPath(t, kvEnabled)
	testController_GetCommitHandler(t, kvEnabled)
	testController_CommitHandler(t, kvEnabled)
	testController_CreateRepositoryHandler(t, kvEnabled)
	testController_DeleteRepositoryHandler(t, kvEnabled)
	testController_ListBranchesHandler(t, kvEnabled)
	testController_ListTagsHandler(t, kvEnabled)
	testController_GetBranchHandler(t, kvEnabled)
	testController_BranchesDiffBranchHandler(t, kvEnabled)
	testController_CreateBranchHandler(t, kvEnabled)
	testController_UploadObject(t, kvEnabled)
	testController_DeleteBranchHandler(t, kvEnabled)
	testController_IngestRangeHandler(t, kvEnabled)
	testController_WriteMetaRangeHandler(t, kvEnabled)
	testController_ObjectsStatObjectHandler(t, kvEnabled)
	testController_ObjectsListObjectsHandler(t, kvEnabled)
	testController_ObjectsGetObjectHandler(t, kvEnabled)
	testController_ObjectsUploadObjectHandler(t, kvEnabled)
	testController_ObjectsStageObjectHandler(t, kvEnabled)
	testController_ObjectsDeleteObjectHandler(t, kvEnabled)
	testController_CreatePolicyHandler(t, kvEnabled)
	testController_ConfigHandlers(t, kvEnabled)
	testController_SetupLakeFSHandler(t, kvEnabled)
	testController_ListRepositoryRuns(t, kvEnabled)
	testController_MergeDiffWithParent(t, kvEnabled)
	testController_MergeIntoExplicitBranch(t, kvEnabled)
	testController_CreateTag(t, kvEnabled)
	testController_Revert(t, kvEnabled)
	testController_RevertConflict(t, kvEnabled)
	testController_ExpandTemplate(t, kvEnabled)
}

func TestKVEnabled(t *testing.T) {
	testControllerWithKV(t, true)
}

func TestKVDisabled(t *testing.T) {
	testControllerWithKV(t, false)
}

func testController_ListRepositoriesHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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

func testController_GetRepoHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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

	t.Run("use same storage namespace twice", func(t *testing.T) {
		name := testUniqueRepoName()
		resp, err := clt.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
			Name:             name,
			StorageNamespace: onBlock(deps, name),
		})
		verifyResponseOK(t, resp, err)

		resp, err = clt.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
			Name:             name + "_2",
			StorageNamespace: onBlock(deps, name),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode())
	})
}

func testCommitEntries(t *testing.T, ctx context.Context, cat catalog.Interface, deps *dependencies, params commitEntriesParams) string {
	t.Helper()
	for _, p := range params.paths {
		err := cat.CreateEntry(ctx, params.repo, params.branch,
			catalog.DBEntry{
				Path:            p,
				PhysicalAddress: onBlock(deps, fmt.Sprintf("add_num_%v_of_%v", params.filesVersion, p)),
				CreationDate:    time.Now(),
				Size:            params.filesVersion,
				Checksum:        fmt.Sprintf("cksum%v", params.filesVersion),
			})
		testutil.MustDo(t, "create entry "+p, err)
	}
	commit, err := cat.Commit(ctx, params.repo, params.branch, "commit"+params.commitName, params.user, nil, nil, nil)
	testutil.MustDo(t, "commit", err)
	return commit.Reference
}

func testController_LogCommitsHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()

	t.Run("missing branch", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "ns1"), "main")
		testutil.Must(t, err)

		resp, err := clt.LogCommitsWithResponse(ctx, "repo1", "otherbranch", &api.LogCommitsParams{})
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatalf("expected error getting a branch that doesn't exist")
		}
	})

	t.Run("log", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo2", onBlock(deps, "ns1"), "main")
		testutil.Must(t, err)

		const commitsLen = 2
		for i := 0; i < commitsLen; i++ {
			n := strconv.Itoa(i + 1)
			p := "foo/bar" + n
			err := deps.catalog.CreateEntry(ctx, "repo2", "main", catalog.DBEntry{Path: p, PhysicalAddress: onBlock(deps, "bar"+n+"addr"), CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n})
			testutil.MustDo(t, "create entry "+p, err)
			_, err = deps.catalog.Commit(ctx, "repo2", "main", "commit"+n, "some_user", nil, nil, nil)
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

	t.Run("limit", func(t *testing.T) {
		const repoID = "repo3"
		_, err := deps.catalog.CreateRepository(ctx, repoID, onBlock(deps, "ns2"), "main")
		testutil.Must(t, err)

		const commitsLen = 2
		for i := 0; i < commitsLen; i++ {
			n := strconv.Itoa(i + 1)
			p := "foo/bar" + n
			err := deps.catalog.CreateEntry(ctx, repoID, "main", catalog.DBEntry{Path: p, PhysicalAddress: onBlock(deps, "bar"+n+"addr"), CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n})
			testutil.MustDo(t, "create entry "+p, err)
			_, err = deps.catalog.Commit(ctx, repoID, "main", "commit"+n, "some_user", nil, nil, nil)
			testutil.MustDo(t, "commit "+p, err)
		}
		limit := true
		resp, err := clt.LogCommitsWithResponse(ctx, repoID, "main", &api.LogCommitsParams{
			Amount: api.PaginationAmountPtr(commitsLen - 1),
			Limit:  &limit,
		})
		verifyResponseOK(t, resp, err)

		// repo is created with a commit
		if resp.JSON200 == nil {
			t.Fatal("LogCommits expected valid response")
		}
		commitsLog := resp.JSON200.Results
		// expect exact number of commits without more
		const expectedCommits = commitsLen - 1
		if len(commitsLog) != expectedCommits {
			t.Fatalf("Log %d commits, expected %d", len(commitsLog), expectedCommits)
		}
		if resp.JSON200.Pagination.HasMore {
			t.Fatalf("Log HasMore %t, expected false", resp.JSON200.Pagination.HasMore)
		}
	})
}

func testController_CommitsGetBranchCommitLogByPath(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()
	/*
			This is the tree we test on:

		                      P              branch "branch-b"
		                     / \
		                   	/   \
		 .---A---B---C-----D-----R---N---X-  branch "main"
		              \             /
		               \           /
		                ---L---M---          branch "branch-a"

				Commits content:
				•	commit A
					⁃	added data/a/a.txt
					⁃	added data/a/b.txt
					⁃	added data/a/c.txt
					⁃	added data/b/b.txt
				•	commit B
					⁃	added data/a/foo.txt
					⁃	added data/b/bar.txt
				•	commit C
					⁃	changed data/a/a.txt
				•	commit D
					⁃	changed data/b/b.txt
				•	commit P
					⁃	added data/c/banana.txt
				•	commit R
					⁃	merged branch-b to main
				•	commit L
					⁃	changed data/a/foo.txt
					⁃	changed data/b/bar.txt
				•	commit M
					⁃	added data/a/d.txt
				•	commit N
					⁃	merged branch-a to main
				•	commit x
					⁃	changed data/a/a.txt
					⁃	added data/c/zebra.txt
	*/

	_, err := deps.catalog.CreateRepository(ctx, "repo3", onBlock(deps, "ns1"), "main")
	testutil.Must(t, err)

	commitsMap := make(map[string]string)
	commitsMap["commitA"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         "repo3",
		branch:       "main",
		filesVersion: 1,
		paths:        []string{"data/a/a.txt", "data/a/b.txt", "data/a/c.txt", "data/b/b.txt"},
		user:         "user1",
		commitName:   "A",
	})
	commitsMap["commitB"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         "repo3",
		branch:       "main",
		filesVersion: 1,
		paths:        []string{"data/a/foo.txt", "data/b/bar.txt"},
		user:         "user1",
		commitName:   "B",
	})
	commitsMap["commitC"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         "repo3",
		branch:       "main",
		filesVersion: 2,
		paths:        []string{"data/a/a.txt"},
		user:         "user1",
		commitName:   "C",
	})
	_, err = deps.catalog.CreateBranch(ctx, "repo3", "branch-a", "main")
	testutil.Must(t, err)
	commitsMap["commitL"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         "repo3",
		branch:       "branch-a",
		filesVersion: 2,
		paths:        []string{"data/a/foo.txt", "data/b/bar.txt"},
		user:         "user2",
		commitName:   "L",
	})
	commitsMap["commitD"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         "repo3",
		branch:       "main",
		filesVersion: 2,
		paths:        []string{"data/b/b.txt"},
		user:         "user1",
		commitName:   "D",
	})
	_, err = deps.catalog.CreateBranch(ctx, "repo3", "branch-b", "main")
	testutil.Must(t, err)
	commitsMap["commitP"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         "repo3",
		branch:       "branch-b",
		filesVersion: 1,
		paths:        []string{"data/c/banana.txt"},
		user:         "user3",
		commitName:   "P",
	})
	mergeCommit, err := deps.catalog.Merge(ctx, "repo3", "main", "branch-b", "user3", "commitR", nil, "")
	testutil.Must(t, err)
	commitsMap["commitR"] = mergeCommit
	commitsMap["commitM"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         "repo3",
		branch:       "branch-a",
		filesVersion: 1,
		paths:        []string{"data/a/d.txt"},
		user:         "user2",
		commitName:   "M",
	})
	mergeCommit, err = deps.catalog.Merge(ctx, "repo3", "main", "branch-a", "user2", "commitN", nil, "")
	testutil.Must(t, err)
	commitsMap["commitN"] = mergeCommit
	commitsMap["commitX"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         "repo3",
		branch:       "main",
		filesVersion: 3,
		paths:        []string{"data/a/a.txt", "data/c/zebra.txt"},
		user:         "user1",
		commitName:   "X",
	})

	cases := []struct {
		name            string
		objectList      *[]string
		prefixList      *[]string
		expectedCommits []string
	}{
		{
			name:            "singleObjectWithOneCommit",
			objectList:      &[]string{"data/a/d.txt"},
			expectedCommits: []string{commitsMap["commitM"]},
		},
		{
			name:            "simpleObjectWithMultipleCommits",
			objectList:      &[]string{"data/a/a.txt"},
			expectedCommits: []string{commitsMap["commitX"], commitsMap["commitC"], commitsMap["commitA"]},
		},
		{
			name:            "simpleObjectLastFile",
			objectList:      &[]string{"data/c/zebra.txt"},
			expectedCommits: []string{commitsMap["commitX"]},
		},
		{
			name:            "multipleObjects",
			objectList:      &[]string{"data/a/foo.txt", "data/b/bar.txt", "data/b/b.txt", "data/c/banana.txt"},
			expectedCommits: []string{commitsMap["commitP"], commitsMap["commitD"], commitsMap["commitL"], commitsMap["commitB"], commitsMap["commitA"]},
		},
		{
			name:            "oneObjectOnePrefix",
			objectList:      &[]string{"data/a/c.txt"},
			prefixList:      &[]string{"data/b/"},
			expectedCommits: []string{commitsMap["commitD"], commitsMap["commitL"], commitsMap["commitB"], commitsMap["commitA"]},
		},
		{
			name:            "simplePrefix",
			prefixList:      &[]string{"data/b/"},
			expectedCommits: []string{commitsMap["commitD"], commitsMap["commitL"], commitsMap["commitB"], commitsMap["commitA"]},
		},
		{
			name:            "twoPrefixs",
			prefixList:      &[]string{"data/a/", "data/c/"},
			expectedCommits: []string{commitsMap["commitX"], commitsMap["commitM"], commitsMap["commitP"], commitsMap["commitL"], commitsMap["commitC"], commitsMap["commitB"], commitsMap["commitA"]},
		},
		{
			name:            "mainPrefix",
			prefixList:      &[]string{"data/"},
			expectedCommits: []string{commitsMap["commitX"], commitsMap["commitM"], commitsMap["commitP"], commitsMap["commitD"], commitsMap["commitL"], commitsMap["commitC"], commitsMap["commitB"], commitsMap["commitA"]},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, err := clt.LogCommitsWithResponse(ctx, "repo3", "main", &api.LogCommitsParams{Objects: c.objectList, Prefixes: c.prefixList})
			verifyResponseOK(t, resp, err)

			commitsLog := resp.JSON200.Results
			commitsIds := make([]string, 0, len(commitsLog))
			for _, commit := range commitsLog {
				commitsIds = append(commitsIds, commit.Id)
			}
			if !reflect.DeepEqual(c.expectedCommits, commitsIds) {
				t.Fatalf("Log commits is: %v, expected %v", commitsIds, c.expectedCommits)
			}
		})
	}
}

func testController_GetCommitHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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
		commit1, err := deps.catalog.Commit(ctx, "foo1", "main", "some message", DefaultUserID, nil, nil, nil)
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

func testController_CommitHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()

	t.Run("commit non-existent commit", func(t *testing.T) {
		repo := testUniqueRepoName()
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &api.CommitParams{}, api.CommitJSONRequestBody{
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
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
		testutil.MustDo(t, fmt.Sprintf("create repo %s", repo), err)
		testutil.MustDo(t, fmt.Sprintf("commit bar on %s", repo), deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &api.CommitParams{}, api.CommitJSONRequestBody{
			Message: "some message",
		})
		verifyResponseOK(t, resp, err)
	})

	t.Run("commit success with source metarange", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
		testutil.MustDo(t, fmt.Sprintf("create repo %s", repo), err)

		_, err = deps.catalog.CreateBranch(ctx, repo, "foo-branch", "main")
		testutil.Must(t, err)
		testutil.MustDo(t, fmt.Sprintf("commit bar on %s", repo), deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &api.CommitParams{}, api.CommitJSONRequestBody{
			Message: "some message",
		})
		verifyResponseOK(t, resp, err)

		resp, err = clt.CommitWithResponse(ctx, repo, "foo-branch", &api.CommitParams{SourceMetarange: &resp.JSON201.MetaRangeId}, api.CommitJSONRequestBody{
			Message: "some message",
		})
		verifyResponseOK(t, resp, err)
	})

	t.Run("commit failure with source metarange and dirty branch", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
		testutil.MustDo(t, fmt.Sprintf("create repo %s", repo), err)

		_, err = deps.catalog.CreateBranch(ctx, repo, "foo-branch", "main")
		testutil.Must(t, err)

		testutil.MustDo(t, fmt.Sprintf("commit bar on %s", repo), deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &api.CommitParams{}, api.CommitJSONRequestBody{
			Message: "some message",
		})
		verifyResponseOK(t, resp, err)

		testutil.MustDo(t, fmt.Sprintf("commit bar on %s", repo), deps.catalog.CreateEntry(ctx, repo, "foo-branch", catalog.DBEntry{Path: "foo/bar/2", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		resp, err = clt.CommitWithResponse(ctx, repo, "foo-branch", &api.CommitParams{SourceMetarange: &resp.JSON201.MetaRangeId}, api.CommitJSONRequestBody{
			Message: "some message",
		})

		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, graveler.ErrCommitMetaRangeDirtyBranch.Error())
	})

	t.Run("commit failure empty branch", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
		testutil.MustDo(t, fmt.Sprintf("create repo %s", repo), err)

		_, err = deps.catalog.CreateBranch(ctx, repo, "foo-branch", "main")
		testutil.Must(t, err)

		resp, err := clt.CommitWithResponse(ctx, repo, "foo-branch", &api.CommitParams{}, api.CommitJSONRequestBody{
			Message: "some message",
		})

		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, graveler.ErrNoChanges.Error())
	})

	t.Run("commit success - with creation date", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
		testutil.MustDo(t, fmt.Sprintf("create repo %s", repo), err)
		testutil.MustDo(t, fmt.Sprintf("commit bar on %s", repo), deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		date := int64(1642626109)
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &api.CommitParams{}, api.CommitJSONRequestBody{
			Message: "some message",
			Date:    &date,
		})
		verifyResponseOK(t, resp, err)
		if resp.JSON201.CreationDate != date {
			t.Errorf("creation date expected %d, got: %d", date, resp.JSON201.CreationDate)
		}
	})
}

func testController_CreateRepositoryHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()
	t.Run("create repo success", func(t *testing.T) {
		resp, err := clt.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
			DefaultBranch:    api.StringPtr("main"),
			Name:             "my-new-repo",
			StorageNamespace: onBlock(deps, "foo-bucket-1"),
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
			StorageNamespace: onBlock(deps, "foo-bucket-2"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp == nil {
			t.Fatal("CreateRepository missing response")
		}
		validationErrResp := resp.JSON409
		if validationErrResp == nil {
			t.Fatalf("expected error creating duplicate repo")
		}
	})

	t.Run("create repo with conflicting storage type", func(t *testing.T) {
		resp, _ := clt.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
			DefaultBranch:    api.StringPtr("main"),
			Name:             "repo2",
			StorageNamespace: "s3://foo-bucket",
		})
		if resp == nil {
			t.Fatal("CreateRepository missing response")
		}
		validationErrResp := resp.JSON409
		if validationErrResp == nil {
			t.Fatal("expected error creating repo with conflicting storage type")
		}
	})
}

func testController_DeleteRepositoryHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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

func testController_ListBranchesHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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
		_, err = deps.catalog.Commit(ctx, "repo2", "main", "first commit", "test", nil, nil, nil)
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

func testController_ListTagsHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()

	// setup test data
	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), "main")
	testutil.Must(t, err)
	testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", "main", catalog.DBEntry{Path: "obj1"}))
	commitLog, err := deps.catalog.Commit(ctx, "repo1", "main", "first commit", "test", nil, nil, nil)
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

func testController_GetBranchHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()

	t.Run("get default branch", func(t *testing.T) {
		const testBranch = "main"
		_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), testBranch)
		testutil.Must(t, err)
		// create first dummy commit on main so that we can create branches from it
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", testBranch, catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "repo1", testBranch, "first commit", "test", nil, nil, nil)
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

func testController_BranchesDiffBranchHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()
	const testBranch = "main"
	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), testBranch)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("diff branch no changes", func(t *testing.T) {
		// create an entry and remove it
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", testBranch, catalog.DBEntry{Path: "a/b/c"}))
		testutil.Must(t, deps.catalog.DeleteEntry(ctx, "repo1", testBranch, "a/b/c"))

		resp, err := clt.DiffBranchWithResponse(ctx, "repo1", testBranch, &api.DiffBranchParams{})
		verifyResponseOK(t, resp, err)
		changes := len(resp.JSON200.Results)
		if changes != 0 {
			t.Fatalf("expected no diff results, got %d", changes)
		}
	})

	t.Run("diff branch with writes", func(t *testing.T) {
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", testBranch, catalog.DBEntry{Path: "a/b"}))
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", testBranch, catalog.DBEntry{Path: "a/b/c"}))
		testutil.Must(t, deps.catalog.DeleteEntry(ctx, "repo1", testBranch, "a/b/c"))
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

func testController_CreateBranchHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()
	t.Run("create branch and diff refs success", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", "main", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "repo1", "main", "first commit", "test", nil, nil, nil)
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

		uploadResp, err := uploadObjectHelper(t, ctx, clt, path, strings.NewReader(content), "repo1", newBranchName)
		verifyResponseOK(t, uploadResp, err)

		if _, err := deps.catalog.Commit(ctx, "repo1", "main2", "commit 1", "some_user", nil, nil, nil); err != nil {
			t.Fatalf("failed to commit 'repo1': %s", err)
		}
		resp2, err := clt.DiffRefsWithResponse(ctx, "repo1", "main", newBranchName, &api.DiffRefsParams{})
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
	if err := w.Close(); err != nil {
		t.Fatal("Close multipart writer:", err)
	}

	return clt.UploadObjectWithBodyWithResponse(ctx, repo, branch, &api.UploadObjectParams{
		Path: path,
	}, w.FormDataContentType(), &b)
}

func writeMultipart(fieldName, filename, content string) (string, io.Reader) {
	var buf bytes.Buffer
	mpw := multipart.NewWriter(&buf)
	w, _ := mpw.CreateFormFile(fieldName, filename)
	_, _ = w.Write([]byte(content))
	_ = mpw.Close()
	return mpw.FormDataContentType(), &buf
}

func testController_UploadObject(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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
		_, err = deps.catalog.Commit(ctx, "my-new-repo", "another-branch", "a commit!", "user1", nil, nil, nil)
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

	t.Run("upload object missing 'content' key", func(t *testing.T) {
		// write
		contentType, buf := writeMultipart("this-is-not-content", "bar", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &api.UploadObjectParams{
			Path: "foo/bar",
		}, contentType, buf)

		testutil.Must(t, err)
		if b.StatusCode() != 500 {
			t.Fatalf("expected 500 for UploadObject, got %d", b.StatusCode())
		}
		if !strings.Contains(b.JSONDefault.Message, "missing key 'content'") {
			t.Fatalf("error message should state missing 'content' key")
		}
	})
}

func testController_DeleteBranchHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()

	t.Run("delete branch success", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "my-new-repo", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "my-new-repo", "main", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "my-new-repo", "main", "first commit", "test", nil, nil, nil)
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

	t.Run("delete default branch", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "my-new-repo2", onBlock(deps, "foo2"), "main")
		testutil.Must(t, err)
		resp, err := clt.DeleteBranchWithResponse(ctx, "my-new-repo2", "main")
		if err != nil {
			t.Fatal("DeleteBranch error:", err)
		}
		if resp.JSONDefault == nil {
			t.Fatal("DeleteBranch expected error while trying to delete default branch")
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

func testController_IngestRangeHandler(t *testing.T, kvEnabled bool) {
	const (
		fromSourceURI           = "https://valid.uri"
		uriPrefix               = "take/from/here"
		fromSourceURIWithPrefix = fromSourceURI + "/" + uriPrefix
		after                   = "some/key/to/start/after"
		prepend                 = "some/logical/prefix"
	)

	continuationToken := "opaque"

	setup := func(t *testing.T, count int, expectedErr error) (api.ClientWithResponsesInterface, *testutils.FakeWalker) {
		t.Helper()
		ctx := context.Background()

		w := testutils.NewFakeWalker(count, count, uriPrefix, after, continuationToken, fromSourceURIWithPrefix, expectedErr)
		clt, deps := setupClientWithAdminAndWalkerFactory(t, testutils.FakeFactory{Walker: w}, kvEnabled)

		// setup test data
		_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)

		return clt, w
	}

	t.Run("successful ingestion no pagination", func(t *testing.T) {
		ctx := context.Background()
		count := 1000
		clt, w := setup(t, count, nil)

		resp, err := clt.IngestRangeWithResponse(ctx, "repo1", api.IngestRangeJSONRequestBody{
			After:             after,
			FromSourceURI:     fromSourceURIWithPrefix,
			Prepend:           prepend,
			ContinuationToken: &continuationToken,
		})

		verifyResponseOK(t, resp, err)
		require.NotNil(t, resp.JSON201.Range)
		require.NotNil(t, resp.JSON201.Pagination)
		require.Equal(t, count, resp.JSON201.Range.Count)
		require.Equal(t, strings.Replace(w.Entries[0].FullKey, uriPrefix, prepend, 1), resp.JSON201.Range.MinKey)
		require.Equal(t, strings.Replace(w.Entries[count-1].FullKey, uriPrefix, prepend, 1), resp.JSON201.Range.MaxKey)
		require.False(t, resp.JSON201.Pagination.HasMore)
		require.Empty(t, resp.JSON201.Pagination.LastKey)
		require.Empty(t, resp.JSON201.Pagination.ContinuationToken)
	})

	t.Run("successful ingestion with pagination", func(t *testing.T) {
		// force splitting the range before
		ctx := context.Background()
		count := 200_000
		clt, w := setup(t, count, nil)

		resp, err := clt.IngestRangeWithResponse(ctx, "repo1", api.IngestRangeJSONRequestBody{
			After:             after,
			FromSourceURI:     fromSourceURIWithPrefix,
			Prepend:           prepend,
			ContinuationToken: &continuationToken,
		})

		verifyResponseOK(t, resp, err)
		require.NotNil(t, resp.JSON201.Range)
		require.NotNil(t, resp.JSON201.Pagination)
		require.Less(t, resp.JSON201.Range.Count, count)
		require.Equal(t, strings.Replace(w.Entries[0].FullKey, uriPrefix, prepend, 1), resp.JSON201.Range.MinKey)
		require.Equal(t, strings.Replace(w.Entries[resp.JSON201.Range.Count-1].FullKey, uriPrefix, prepend, 1), resp.JSON201.Range.MaxKey)
		require.True(t, resp.JSON201.Pagination.HasMore)
		require.Equal(t, w.Entries[resp.JSON201.Range.Count-1].FullKey, resp.JSON201.Pagination.LastKey)
		require.Equal(t, testutils.ContinuationTokenOpaque, *resp.JSON201.Pagination.ContinuationToken)
	})

	t.Run("error during walk", func(t *testing.T) {
		// force splitting the range before
		ctx := context.Background()
		count := 10
		expectedErr := errors.New("failed reading for object store")
		clt, _ := setup(t, count, expectedErr)

		resp, err := clt.IngestRangeWithResponse(ctx, "repo1", api.IngestRangeJSONRequestBody{
			After:             after,
			FromSourceURI:     fromSourceURIWithPrefix,
			Prepend:           prepend,
			ContinuationToken: &continuationToken,
		})

		require.NoError(t, err)
		require.Equal(t, http.StatusInternalServerError, resp.StatusCode())
		require.Contains(t, string(resp.Body), expectedErr.Error())
	})
}

func testController_WriteMetaRangeHandler(t *testing.T, kvEnabled bool) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	repo := testUniqueRepoName()
	// setup test data
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
	testutil.Must(t, err)

	t.Run("successful metarange creation", func(t *testing.T) {
		resp, err := clt.CreateMetaRangeWithResponse(ctx, repo, api.CreateMetaRangeJSONRequestBody{
			Ranges: []api.RangeMetadata{
				{Count: 11355, EstimatedSize: 123465897, Id: "FirstRangeID", MaxKey: "1", MinKey: "2"},
				{Count: 13123, EstimatedSize: 123465897, Id: "SecondRangeID", MaxKey: "3", MinKey: "4"},
				{Count: 10123, EstimatedSize: 123465897, Id: "ThirdRangeID", MaxKey: "5", MinKey: "6"},
			},
		})

		verifyResponseOK(t, resp, err)
		require.NotNil(t, resp.JSON201)
		require.NotNil(t, resp.JSON201.Id)
		require.NotEmpty(t, *resp.JSON201.Id)

		respMR, err := clt.GetMetaRangeWithResponse(ctx, repo, *resp.JSON201.Id)
		verifyResponseOK(t, respMR, err)
		require.NotNil(t, respMR.JSON200)
		require.NotEmpty(t, respMR.JSON200.Location)
	})

	t.Run("missing ranges", func(t *testing.T) {
		resp, err := clt.CreateMetaRangeWithResponse(ctx, repo, api.CreateMetaRangeJSONRequestBody{
			Ranges: []api.RangeMetadata{},
		})

		require.NoError(t, err)
		require.NotNil(t, resp.JSON400)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode())
	})
}

func testController_ObjectsStatObjectHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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
			Metadata:        catalog.Metadata{"additionalProperty1": "testing get object stats"},
		}
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", "main", entry))

		resp, err := clt.StatObjectWithResponse(ctx, "repo1", "main", &api.StatObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, resp, err)
		objectStats := resp.JSON200

		// verify bar stat info
		if objectStats.Path != entry.Path {
			t.Fatalf("expected to get back our path, got %s", objectStats.Path)
		}
		if api.Int64Value(objectStats.SizeBytes) != entry.Size {
			t.Fatalf("expected correct size, got %d", objectStats.SizeBytes)
		}
		if objectStats.PhysicalAddress != onBlock(deps, "some-bucket/")+entry.PhysicalAddress {
			t.Fatalf("expected correct PhysicalAddress, got %s", objectStats.PhysicalAddress)
		}
		if diff := deep.Equal(objectStats.Metadata.AdditionalProperties, map[string]string(entry.Metadata)); diff != nil {
			t.Fatalf("expected to get back user-defined metadata: %s", diff)
		}
		contentType := api.StringValue(objectStats.ContentType)
		if contentType != catalog.DefaultContentType {
			t.Fatalf("expected to get default content type, got: %s", contentType)
		}

		// verify get stat without metadata works
		getUserMetadata := false
		resp, err = clt.StatObjectWithResponse(ctx, "repo1", "main", &api.StatObjectParams{Path: "foo/bar", UserMetadata: &getUserMetadata})
		verifyResponseOK(t, resp, err)
		objectStatsNoMetadata := resp.JSON200
		if objectStatsNoMetadata.Metadata != nil {
			t.Fatalf("expected to not get back user-defined metadata, got %+v", objectStatsNoMetadata.Metadata.AdditionalProperties)
		}
	})

	t.Run("get object stats content-type", func(t *testing.T) {
		entry := catalog.DBEntry{
			Path:            "foo/bar2",
			PhysicalAddress: "this_is_bar2_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "this_is_a_checksum",
			ContentType:     "example/content",
		}
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "repo1", "main", entry))

		resp, err := clt.StatObjectWithResponse(ctx, "repo1", "main", &api.StatObjectParams{Path: "foo/bar2"})
		verifyResponseOK(t, resp, err)
		objectStats := resp.JSON200

		// verify stat custom content-type
		contentType := api.StringValue(objectStats.ContentType)
		if contentType != entry.ContentType {
			t.Fatalf("expected to get entry content type, got: %s, expected: %s", contentType, entry.ContentType)
		}
	})
}

func testController_ObjectsListObjectsHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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
			Metadata:        catalog.Metadata{"additionalProperty1": "1"},
		},
		{
			Path:            "foo/quuux",
			PhysicalAddress: "this_is_quuxs_address_expired",
			CreationDate:    time.Now(),
			Size:            9999999,
			Checksum:        "quux_checksum",
			Expired:         true,
			Metadata:        catalog.Metadata{"additionalProperty1": "2"},
		},
		{
			Path:            "foo/baz",
			PhysicalAddress: "this_is_bazs_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "baz_checksum",
			Metadata:        catalog.Metadata{"additionalProperty1": "3"},
		},
		{
			Path:            "foo/a_dir/baz",
			PhysicalAddress: "this_is_bazs_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "baz_checksum",
			Metadata:        catalog.Metadata{"additionalProperty1": "4"},
		},
	}
	for _, entry := range dbEntries {
		err := deps.catalog.CreateEntry(ctx, "repo1", "main", entry)
		testutil.Must(t, err)
	}

	t.Run("get object list", func(t *testing.T) {
		prefix := api.PaginationPrefix("foo/")
		resp, err := clt.ListObjectsWithResponse(ctx, "repo1", "main", &api.ListObjectsParams{
			Prefix: &prefix,
		})
		verifyResponseOK(t, resp, err)
		results := resp.JSON200.Results
		if len(results) != 4 {
			t.Fatalf("expected 4 entries, got back %d", len(results))
		}
	})

	t.Run("get object list without user-defined metadata", func(t *testing.T) {
		prefix := api.PaginationPrefix("foo/")
		getUserMetadata := false
		resp, err := clt.ListObjectsWithResponse(ctx, "repo1", "main", &api.ListObjectsParams{
			Prefix:       &prefix,
			UserMetadata: &getUserMetadata,
		})
		verifyResponseOK(t, resp, err)
		results := resp.JSON200.Results
		for _, obj := range results {
			if obj.Metadata != nil {
				t.Fatalf("expected no user-defined metadata, got %+v", obj.Metadata.AdditionalProperties)
			}
		}
	})

	t.Run("get object list paginated", func(t *testing.T) {
		prefix := api.PaginationPrefix("foo/")
		resp, err := clt.ListObjectsWithResponse(ctx, "repo1", "main", &api.ListObjectsParams{
			Prefix: &prefix,
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

func testController_ObjectsGetObjectHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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

func testController_ObjectsUploadObjectHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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

func testController_ObjectsStageObjectHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
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

func testController_ObjectsDeleteObjectHandler(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()

	const repo = "repo1"
	const branch = "main"
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "some-bucket/prefix"), branch)
	if err != nil {
		t.Fatal(err)
	}

	const content = "hello world this is my awesome content"

	t.Run("delete object", func(t *testing.T) {
		resp, err := uploadObjectHelper(t, ctx, clt, "foo/bar", strings.NewReader(content), repo, branch)
		verifyResponseOK(t, resp, err)

		sizeBytes := api.Int64Value(resp.JSON201.SizeBytes)
		if sizeBytes != 38 {
			t.Fatalf("expected 38 bytes to be written, got back %d", sizeBytes)
		}

		// download it
		rresp, err := clt.GetObjectWithResponse(ctx, repo, branch, &api.GetObjectParams{Path: "foo/bar"})
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
		delResp, err := clt.DeleteObjectWithResponse(ctx, repo, branch, &api.DeleteObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, delResp, err)

		// get it
		statResp, err := clt.StatObjectWithResponse(ctx, repo, branch, &api.StatObjectParams{Path: "foo/bar"})
		testutil.Must(t, err)
		if statResp == nil {
			t.Fatal("StatObject missing response")
		}
		if statResp.JSON404 == nil {
			t.Fatalf("expected file to be gone now")
		}
	})

	t.Run("delete objects", func(t *testing.T) {
		// setup content to delete
		const namePrefix = "foo2/bar"
		const numOfObjs = 3
		var paths []string
		for i := 0; i < numOfObjs; i++ {
			objPath := namePrefix + strconv.Itoa(i)
			paths = append(paths, objPath)
			resp, err := uploadObjectHelper(t, ctx, clt, objPath, strings.NewReader(content), repo, branch)
			verifyResponseOK(t, resp, err)
		}

		// delete objects
		delResp, err := clt.DeleteObjectsWithResponse(ctx, repo, branch, api.DeleteObjectsJSONRequestBody{Paths: paths})
		verifyResponseOK(t, delResp, err)
		if delResp.StatusCode() != http.StatusNoContent {
			t.Errorf("DeleteObjects should return 204 (no content) for successful delete, got %d", delResp.StatusCode())
		}

		// check objects no longer there
		paths = append(paths, "not-there") // include missing one
		for _, p := range paths {
			statResp, err := clt.StatObjectWithResponse(ctx, repo, branch, &api.StatObjectParams{Path: p})
			testutil.Must(t, err)
			if statResp == nil {
				t.Fatalf("StatObject missing response for '%s'", p)
			}
			if statResp.JSON404 == nil {
				t.Fatalf("expected file to be gone now for '%s'", p)
			}
		}
	})

	t.Run("delete objects request size", func(t *testing.T) {
		// setup content to delete
		const namePrefix = "foo3/bar"
		const numOfObjs = api.DefaultMaxDeleteObjects + 1
		var paths []string
		for i := 0; i < numOfObjs; i++ {
			paths = append(paths, namePrefix+strconv.Itoa(i))
		}

		// delete objects
		delResp, err := clt.DeleteObjectsWithResponse(ctx, repo, branch, api.DeleteObjectsJSONRequestBody{Paths: paths})
		testutil.Must(t, err)
		const expectedStatusCode = http.StatusInternalServerError
		if delResp.StatusCode() != expectedStatusCode {
			t.Fatalf("DeleteObjects status code %d, expected %d", delResp.StatusCode(), expectedStatusCode)
		}
		if delResp.JSONDefault == nil {
			t.Fatal("DeleteObjects expected default error")
		}
		if !strings.Contains(delResp.JSONDefault.Message, api.ErrRequestSizeExceeded.Error()) {
			t.Fatalf("DeleteObjects size exceeded error: '%s', expected '%s'", delResp.JSONDefault.Message, api.ErrRequestSizeExceeded)
		}
	})

	t.Run("delete objects protected", func(t *testing.T) {
		// create a branch with branch protection to test delete files
		_, err = deps.catalog.CreateBranch(ctx, repo, "protected", branch)
		testutil.Must(t, err)
		// some content
		paths := []string{"a", "b", "c"}
		for _, p := range paths {
			resp, err := uploadObjectHelper(t, ctx, clt, p, strings.NewReader(content), repo, branch)
			verifyResponseOK(t, resp, err)
		}
		err = deps.catalog.CreateBranchProtectionRule(ctx, repo, "*", []graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_STAGING_WRITE})
		testutil.Must(t, err)

		// delete objects
		delResp, err := clt.DeleteObjectsWithResponse(ctx, repo, "protected", api.DeleteObjectsJSONRequestBody{Paths: paths})
		verifyResponseOK(t, delResp, err)
		if delResp.StatusCode() != http.StatusOK {
			t.Fatalf("DeleteObjects status code %d, expected %d", delResp.StatusCode(), http.StatusOK)
		}
		if delResp.JSON200 == nil {
			t.Fatal("DeleteObjects response is missing")
		}
		if len(delResp.JSON200.Errors) != len(paths) {
			t.Fatalf("DeleteObjects")
		}
		var errPaths []string
		for _, item := range delResp.JSON200.Errors {
			errPaths = append(errPaths, api.StringValue(item.Path))
		}
		// sort both lists to match
		sort.Strings(errPaths)
		sort.Strings(paths)
		if diff := deep.Equal(paths, errPaths); diff != nil {
			t.Fatalf("DeleteObjects errors path difference: %s", diff)
		}
	})
}

func testController_CreatePolicyHandler(t *testing.T, kvEnabled bool) {
	clt, _ := setupClientWithAdmin(t, kvEnabled)
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

func testController_ConfigHandlers(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()

	ExpectedExample := onBlock(deps, "example-bucket/")

	t.Run("Get storage config", func(t *testing.T) {
		resp, err := clt.GetStorageConfigWithResponse(ctx)
		verifyResponseOK(t, resp, err)

		example := resp.JSON200.BlockstoreNamespaceExample
		if example != ExpectedExample {
			t.Errorf("expected to get %s, got %s", ExpectedExample, example)
		}
	})
}

func testController_SetupLakeFSHandler(t *testing.T, kvEnabled bool) {
	const validAccessKeyID = "AKIAIOSFODNN7EXAMPLE"
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
				AccessKeyId:     validAccessKeyID,
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
				AccessKeyId: validAccessKeyID,
			},
			expectedStatusCode: http.StatusBadRequest,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			handler, deps := setupHandler(t, kvEnabled)
			server := setupServer(t, handler)
			clt := setupClientByEndpoint(t, server.URL, "", "")

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
			if len(deps.collector.Metadata) != 1 {
				t.Fatal("Failed to collect metadata")
			}
			if deps.collector.Metadata[0].InstallationID == "" {
				t.Fatal("Empty installationID")
			}
			if len(deps.collector.Metadata[0].Entries) < 5 {
				t.Fatalf("There should be at least 5 metadata entries: %s", deps.collector.Metadata[0].Entries)
			}

			hasBlockStoreType := false
			for _, ent := range deps.collector.Metadata[0].Entries {
				if ent.Name == stats.BlockstoreTypeKey {
					hasBlockStoreType = true
					if ent.Value == "" {
						t.Fatalf("Blockstorage key exists but with empty value: %s", deps.collector.Metadata[0].Entries)
					}
					break
				}
			}
			if !hasBlockStoreType {
				t.Fatalf("missing blockstorage key: %s", deps.collector.Metadata[0].Entries)
			}

			// on successful setup - make sure we can't re-setup
			if c.expectedStatusCode == http.StatusOK {
				ctx := context.Background()
				res, err := clt.SetupWithResponse(ctx, api.SetupJSONRequestBody{
					Username: "admin",
				})
				testutil.Must(t, err)
				if res.JSON409 == nil {
					t.Error("re-setup didn't got conflict response")
				}
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

func testController_ListRepositoryRuns(t *testing.T, kvEnabled bool) {
	clt, _ := setupClientWithAdmin(t, kvEnabled)
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
	respCommit, err := clt.CommitWithResponse(ctx, "repo9", "main", &api.CommitParams{}, api.CommitJSONRequestBody{
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
		respCommit, err := clt.CommitWithResponse(ctx, "repo9", "work", &api.CommitParams{}, api.CommitJSONRequestBody{Message: content})
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

	t.Run("on branch and commit", func(t *testing.T) {
		respList, err := clt.ListRepositoryRunsWithResponse(ctx, "repo9", &api.ListRepositoryRunsParams{
			Branch: api.StringPtr("someBranch"),
			Commit: api.StringPtr("someCommit"),
			Amount: api.PaginationAmountPtr(100),
		})
		require.NoError(t, err)
		require.NotNil(t, respList)
		require.Equal(t, http.StatusBadRequest, respList.StatusCode())
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

func testController_MergeDiffWithParent(t *testing.T, kvEnabled bool) {
	clt, _ := setupClientWithAdmin(t, kvEnabled)
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

	commitResp, err := clt.CommitWithResponse(ctx, repoName, "work", &api.CommitParams{}, api.CommitJSONRequestBody{Message: "file 1 commit to work"})
	verifyResponseOK(t, commitResp, err)

	mergeResp, err := clt.MergeIntoBranchWithResponse(ctx, repoName, "work", "main", api.MergeIntoBranchJSONRequestBody{
		Message: api.StringPtr("merge work to main"),
	})
	verifyResponseOK(t, mergeResp, err)

	diffResp, err := clt.DiffRefsWithResponse(ctx, repoName, "main~1", "main", &api.DiffRefsParams{})
	verifyResponseOK(t, diffResp, err)
	expectedSize := int64(len(content))
	expectedResults := []api.Diff{
		{Path: "file1", PathType: "object", Type: "added", SizeBytes: &expectedSize},
	}
	if diff := deep.Equal(diffResp.JSON200.Results, expectedResults); diff != nil {
		t.Fatal("Diff results not as expected:", diff)
	}
}

func testController_MergeIntoExplicitBranch(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()

	// setup env
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
	testutil.Must(t, err)
	_, err = deps.catalog.CreateBranch(ctx, repo, "branch1", "main")
	testutil.Must(t, err)
	err = deps.catalog.CreateEntry(ctx, repo, "branch1", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"})
	testutil.Must(t, err)
	_, err = deps.catalog.Commit(ctx, repo, "branch1", "some message", DefaultUserID, nil, nil, nil)
	testutil.Must(t, err)

	// test branch with mods
	table := []struct {
		Name string
		Mod  graveler.RefModType
	}{
		{Name: "staging", Mod: graveler.RefModTypeDollar},
		{Name: "committed", Mod: graveler.RefModTypeAt},
	}
	for _, tt := range table {
		t.Run(tt.Name, func(t *testing.T) {
			destinationBranch := "main" + string(tt.Mod)
			resp, err := clt.MergeIntoBranchWithResponse(ctx, repo, "branch1", destinationBranch, api.MergeIntoBranchJSONRequestBody{})
			testutil.MustDo(t, "perform merge into branch", err)
			if resp.StatusCode() != http.StatusBadRequest {
				t.Fatalf("merge to branch with modifier should fail with status %d, got code: %v", http.StatusBadRequest, resp.StatusCode())
			}
		})
	}
}

func testController_CreateTag(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()
	// setup env
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
	testutil.Must(t, err)
	testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
	commit1, err := deps.catalog.Commit(ctx, repo, "main", "some message", DefaultUserID, nil, nil, nil)
	testutil.Must(t, err)

	t.Run("ref", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  "tag1",
			Ref: commit1.Reference,
		})
		verifyResponseOK(t, tagResp, err)
	})

	t.Run("branch", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  "tag2",
			Ref: "main",
		})
		verifyResponseOK(t, tagResp, err)
	})

	t.Run("branch_with_latest_modifier", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  "tag3",
			Ref: "main@",
		})
		verifyResponseOK(t, tagResp, err)
	})

	t.Run("branch_with_staging_modifier", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  "tag4",
			Ref: "main$",
		})
		testutil.Must(t, err)
		if tagResp.JSONDefault == nil {
			t.Errorf("Create tag to explicit stage should fail with error, got %v", tagResp)
		}
	})

	t.Run("tag_tag", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  "tag5",
			Ref: "main",
		})
		verifyResponseOK(t, tagResp, err)
		tagTagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  "tag6",
			Ref: "tag5",
		})
		verifyResponseOK(t, tagTagResp, err)
	})

	t.Run("not_exists", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  "tag6",
			Ref: "unknown",
		})
		testutil.Must(t, err)
		if tagResp.JSON404 == nil {
			t.Errorf("Create tag to unknown ref expected 404, got %v", tagResp)
		}
	})
}

func testUniqueRepoName() string {
	return "repo-" + nanoid.MustGenerate("abcdef1234567890", 8)
}

func testController_Revert(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()
	// setup env
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
	testutil.Must(t, err)
	testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
	_, err = deps.catalog.Commit(ctx, repo, "main", "some message", DefaultUserID, nil, nil, nil)
	testutil.Must(t, err)

	t.Run("ref", func(t *testing.T) {
		branchResp, err := clt.GetBranchWithResponse(ctx, repo, "main")
		verifyResponseOK(t, branchResp, err)
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "main", api.RevertBranchJSONRequestBody{Ref: branchResp.JSON200.CommitId})
		verifyResponseOK(t, revertResp, err)
	})

	t.Run("branch", func(t *testing.T) {
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "main", api.RevertBranchJSONRequestBody{Ref: "main"})
		verifyResponseOK(t, revertResp, err)
	})

	t.Run("committed", func(t *testing.T) {
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "main", api.RevertBranchJSONRequestBody{Ref: "main@"})
		verifyResponseOK(t, revertResp, err)
	})

	t.Run("staging", func(t *testing.T) {
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "main", api.RevertBranchJSONRequestBody{Ref: "main$"})
		testutil.Must(t, err)
		if revertResp.JSONDefault == nil {
			t.Errorf("Revert to explicit staging should fail with error, got %v", revertResp)
		}
	})
}

func testController_RevertConflict(t *testing.T, kvEnabled bool) {
	clt, deps := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()
	// setup env
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
	testutil.Must(t, err)
	testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
	firstCommit, err := deps.catalog.Commit(ctx, repo, "main", "some message", DefaultUserID, nil, nil, nil)
	testutil.Must(t, err)
	testutil.MustDo(t, "overriding entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum2"}))
	_, err = deps.catalog.Commit(ctx, repo, "main", "some other message", DefaultUserID, nil, nil, nil)
	testutil.Must(t, err)

	resp, err := clt.RevertBranchWithResponse(ctx, repo, "main", api.RevertBranchJSONRequestBody{Ref: firstCommit.Reference})
	testutil.Must(t, err)
	if resp.HTTPResponse.StatusCode != http.StatusConflict {
		t.Errorf("Revert with a conflict should fail with status %d got %d", http.StatusConflict, resp.HTTPResponse.StatusCode)
	}
}

func testController_ExpandTemplate(t *testing.T, kvEnabled bool) {
	clt, _ := setupClientWithAdmin(t, kvEnabled)
	ctx := context.Background()

	t.Run("not-found", func(t *testing.T) {
		resp, err := clt.ExpandTemplateWithResponse(ctx, "no/template/here", &api.ExpandTemplateParams{})
		testutil.Must(t, err)
		if resp.HTTPResponse.StatusCode != http.StatusNotFound {
			t.Errorf("Expanding a nonexistent template should fail with status %d got %d\n\t%s\n\t%+v", http.StatusNotFound, resp.HTTPResponse.StatusCode, string(resp.Body), resp)
		}
	})

	t.Run("spark.conf", func(t *testing.T) {
		const lfsURL = "https://lakefs.example.test"
		expected := []struct {
			name    string
			pattern string
		}{
			{"impl", `spark\.hadoop\.fs\.s3a\.impl=org\.apache\.hadoop\.fs\.s3a\.S3AFileSystem`},
			{"access.key", `spark\.hadoop\.fs\.s3a\.access.key=AKIA.*`},
			{"secret.key", `spark\.hadoop\.fs\.s3a\.secret.key=`},
			{"s3a_endpoint", `spark\.hadoop\.fs\.s3a\.endpoint=` + lfsURL},
		}

		// OpenAPI places additional query params in the wrong
		// place.  Use a request editor to place them directly as a
		// query string.
		resp, err := clt.ExpandTemplateWithResponse(ctx, "spark.submit.conf.tt", &api.ExpandTemplateParams{},
			api.RequestEditorFn(func(_ context.Context, req *http.Request) error {
				values := req.URL.Query()
				values.Add("lakefs_url", lfsURL)
				req.URL.RawQuery = values.Encode()
				return nil
			}))
		testutil.Must(t, err)
		if resp.HTTPResponse.StatusCode != http.StatusOK {
			t.Errorf("Expansion failed with status %d\n\t%s\n\t%+v", resp.HTTPResponse.StatusCode, string(resp.Body), resp)
		}

		contentType := resp.HTTPResponse.Header.Values("Content-Type")
		if len(contentType) != 1 {
			t.Errorf("Expansion returned %d content types: %v", len(contentType), contentType)
		}
		if contentType[0] != "application/x-conf" {
			t.Errorf("Expansion returned content type %s not application/x-conf", contentType[0])
		}

		for _, e := range expected {
			re := regexp.MustCompile(e.pattern)
			if !re.Match(resp.Body) {
				t.Errorf("Expansion result has no %s: /%s/\n\t%s", e.name, e.pattern, string(resp.Body))
			}
		}
	})

	t.Run("fail", func(t *testing.T) {
		resp, err := clt.ExpandTemplateWithResponse(ctx, "fail.tt", &api.ExpandTemplateParams{})
		testutil.Must(t, err)
		if resp.HTTPResponse.StatusCode != http.StatusInternalServerError {
			t.Errorf("Expansion should fail with status %d got %d\n\t%s\n\t%+v", http.StatusInternalServerError, resp.HTTPResponse.StatusCode, string(resp.Body), resp)
		}

		parsed := make(map[string]string, 0)
		err = json.Unmarshal(resp.Body, &parsed)
		if err != nil {
			t.Errorf("Unmarshal body: %s", err)
		}
		if parsed["message"] != "expansion failed" {
			t.Errorf("Expected \"expansion failed\" message, got %+v", parsed)
		}
	})
}
