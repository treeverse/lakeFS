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
	"net"
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

	"github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/hashicorp/go-multierror"
	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/rs/xid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/ingest/store"
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

func TestController_ListRepositoriesHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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
	clt, deps := setupClientWithAdmin(t)
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

func TestController_LogCommitsMissingBranch(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "ns1"), "main")
	testutil.Must(t, err)

	resp, err := clt.LogCommitsWithResponse(ctx, "repo1", "otherbranch", &api.LogCommitsParams{})
	testutil.Must(t, err)
	if resp.JSON404 == nil {
		t.Fatalf("expected error getting a branch that doesn't exist")
	}
}

func TestController_LogCommitsHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	tests := []struct {
		name            string
		commits         int
		limit           bool
		expectedCommits int
		objects         []string
		prefixes        []string
	}{
		{
			name:            "log",
			commits:         2,
			expectedCommits: 3,
		},
		{
			name:            "limit",
			commits:         2,
			expectedCommits: 1,
			limit:           true,
		},
		{
			name:            "log-with-objects",
			commits:         10,
			expectedCommits: 3,
			objects:         []string{"foo/bar7", "foo/bar3", "foo/bar9"},
		},
		{
			name:            "log-with-objects-no-commits",
			commits:         5,
			expectedCommits: 0,
			objects:         []string{"you-won't-find-me"},
		},
		{
			name:            "log-with-prefixes",
			commits:         10,
			expectedCommits: 10,
			prefixes:        []string{"foo/bar"},
		},
	}

	for _, ttt := range tests {
		tt := ttt
		t.Run(tt.name, func(t *testing.T) {
			_, err := deps.catalog.CreateRepository(ctx, tt.name, onBlock(deps, tt.name), "main")
			testutil.Must(t, err)

			const prefix = "foo/bar"
			for i := 0; i < tt.commits; i++ {
				n := strconv.Itoa(i + 1)
				p := prefix + n
				err := deps.catalog.CreateEntry(ctx, tt.name, "main", catalog.DBEntry{Path: p, PhysicalAddress: onBlock(deps, "bar"+n+"addr"), CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n})
				testutil.MustDo(t, "create entry "+p, err)
				_, err = deps.catalog.Commit(ctx, tt.name, "main", "commit"+n, "some_user", nil, nil, nil)
				testutil.MustDo(t, "commit "+p, err)
			}
			params := &api.LogCommitsParams{}
			if tt.objects != nil {
				params.Objects = &tt.objects
			}
			if tt.prefixes != nil {
				params.Prefixes = &tt.prefixes
			}
			if tt.limit {
				params.Limit = &tt.limit
				params.Amount = api.PaginationAmountPtr(1)
			}
			resp, err := clt.LogCommitsWithResponse(ctx, tt.name, "main", params)
			verifyResponseOK(t, resp, err)

			commitsLog := resp.JSON200.Results
			if len(commitsLog) != tt.expectedCommits {
				t.Fatalf("Log %d commits, expected %d", len(commitsLog), tt.expectedCommits)
			}
		})
	}
}

// TestController_LogCommitsParallelHandler sends concurrent requests to LogCommits.
// LogCommits uses shared work pool, checking correctness for concurrent work is important.
func TestController_LogCommitsParallelHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	repo := "repo-log-commits-parallel"
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
	testutil.Must(t, err)

	commits := 100
	const prefix = "foo/bar"
	commitsToLook := map[string]*catalog.CommitLog{}
	for i := 0; i < commits; i++ {
		n := strconv.Itoa(i + 1)
		p := prefix + n
		err := deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: p, PhysicalAddress: onBlock(deps, "bar"+n+"addr"), CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n})
		testutil.MustDo(t, "create entry "+p, err)
		log, err := deps.catalog.Commit(ctx, repo, "main", t.Name()+" commit"+n, "some_user", nil, nil, nil)
		testutil.MustDo(t, "commit "+p, err)
		if i%4 == 0 {
			commitsToLook[p] = log
		}
	}

	var g multierror.Group
	for path, logRef := range commitsToLook {
		objects := []string{path}
		params := &api.LogCommitsParams{Objects: &objects}
		log := logRef
		g.Go(func() error {
			resp, err := clt.LogCommitsWithResponse(ctx, repo, "main", params)
			verifyResponseOK(t, resp, err)

			commitsLog := resp.JSON200.Results
			if len(commitsLog) != 1 {
				t.Fatalf("Log %d commits, expected %d", len(commitsLog), 1)
			}
			if commitsLog[0].Id != log.Reference {
				t.Fatalf("Found commit %s, expected %s", commitsLog[0].Id, log.Reference)
			}
			return nil
		})
	}

	err = g.Wait()
	require.Nil(t, err)
}

func TestController_LogCommitsPredefinedData(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	// prepare test data
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
	testutil.Must(t, err)
	const prefix = "foo/bar"
	const totalCommits = 10
	for i := 0; i < totalCommits; i++ {
		n := strconv.Itoa(i + 1)
		p := prefix + n
		err := deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: p, PhysicalAddress: onBlock(deps, "bar"+n+"addr"), CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n})
		testutil.MustDo(t, "create entry "+p, err)
		_, err = deps.catalog.Commit(ctx, repo, "main", "commit"+n, "some_user", nil, nil, nil)
		testutil.MustDo(t, "commit "+p, err)
	}

	tests := []struct {
		name            string
		amount          int
		limit           bool
		objects         []string
		prefixes        []string
		expectedCommits []string
		expectedMore    bool
	}{
		{
			name:            "log",
			expectedCommits: []string{"commit10", "commit9", "commit8", "commit7", "commit6", "commit5", "commit4", "commit3", "commit2", "commit1", "Repository created"},
			expectedMore:    false,
		},
		{
			name:            "limit",
			limit:           true,
			expectedCommits: []string{"commit10"},
			expectedMore:    false,
		},
		{
			name:            "amount",
			amount:          3,
			expectedCommits: []string{"commit10", "commit9", "commit8"},
			expectedMore:    true,
		},
		{
			name:            "limit-object",
			limit:           true,
			objects:         []string{"foo/bar7"},
			expectedCommits: []string{"commit7"},
			expectedMore:    false,
		},
		{
			name:            "limit-object-not-found",
			limit:           true,
			objects:         []string{"foo/bar99"},
			expectedCommits: []string{},
			expectedMore:    false,
		},
		{
			name:            "log-with-objects",
			objects:         []string{"foo/bar7", "foo/bar3", "foo/bar9"},
			expectedCommits: []string{"commit9", "commit7", "commit3"},
			expectedMore:    false,
		},
		{
			name:            "log-with-objects-amount",
			amount:          2,
			objects:         []string{"foo/bar7", "foo/bar3", "foo/bar9"},
			expectedCommits: []string{"commit9", "commit7"},
			expectedMore:    true,
		},
		{
			name:            "log-with-objects-no-commits",
			objects:         []string{"you-won't-find-me"},
			expectedCommits: []string{},
			expectedMore:    false,
		},
		{
			name:            "log-single-object",
			amount:          1,
			objects:         []string{"foo/bar10"},
			expectedCommits: []string{"commit10"},
			expectedMore:    true,
		},
		{
			name:            "log-with-prefixes",
			prefixes:        []string{"foo/bar1"},
			expectedCommits: []string{"commit10", "commit1"},
			expectedMore:    false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			params := &api.LogCommitsParams{}
			if tt.objects != nil {
				params.Objects = &tt.objects
			}
			if tt.prefixes != nil {
				params.Prefixes = &tt.prefixes
			}
			if tt.limit {
				params.Limit = &tt.limit
				params.Amount = api.PaginationAmountPtr(1)
			}
			if tt.amount > 0 {
				params.Amount = api.PaginationAmountPtr(tt.amount)
			}

			resp, err := clt.LogCommitsWithResponse(ctx, repo, "main", params)
			verifyResponseOK(t, resp, err)
			commitsLog := resp.JSON200.Results
			// compare by commit messages
			commitMessages := make([]string, 0)
			for _, commit := range commitsLog {
				commitMessages = append(commitMessages, commit.Message)
			}
			if diff := deep.Equal(commitMessages, tt.expectedCommits); diff != nil {
				t.Fatalf("Commit log expected messages: %s", diff)
			}
		})
	}
}

func TestController_CommitsGetBranchCommitLogByPath(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

func TestController_GetCommitHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

	t.Run("initial commit", func(t *testing.T) {
		// validate a new repository's initial commit existence and structure
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "foo2", onBlock(deps, "foo2"), "main")
		testutil.Must(t, err)
		resp, err := clt.GetCommitWithResponse(ctx, "foo2", "main")
		verifyResponseOK(t, resp, err)

		commit := resp.JSON200
		if commit == nil {
			t.Fatalf("GetCommit failed - status code: %d", resp.StatusCode())
		}
		if len(commit.Id) == 0 {
			t.Errorf("GetCommit initial commit missing ID: %+v", commit)
		}
		metadata := api.Commit_Metadata{}
		expectedCommit := &api.Commit{
			Committer:    "",
			CreationDate: commit.CreationDate,
			Id:           commit.Id,
			Message:      graveler.FirstCommitMsg,
			MetaRangeId:  "",
			Metadata:     &metadata,
			Parents:      []string{},
		}
		if diff := deep.Equal(commit, expectedCommit); diff != nil {
			t.Fatalf("GetCommit initial commit diff: %v", diff)
		}
	})
}

func TestController_CommitHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

func TestController_CreateRepositoryHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("create repo success", func(t *testing.T) {
		const repoName = "my-new-repo"
		resp, err := clt.CreateRepositoryWithResponse(ctx, &api.CreateRepositoryParams{}, api.CreateRepositoryJSONRequestBody{
			DefaultBranch:    api.StringPtr("main"),
			Name:             repoName,
			StorageNamespace: onBlock(deps, "foo-bucket-1"),
		})
		verifyResponseOK(t, resp, err)

		response := resp.JSON201
		if response == nil {
			t.Fatal("CreateRepository got bad response")
		}
		if response.Id != repoName {
			t.Fatalf("CreateRepository id=%s, expected=%s", response.Id, repoName)
		}
	})

	t.Run("create bare repo success", func(t *testing.T) {
		const repoName = "my-new-repo-bare"
		bareRepo := true
		resp, err := clt.CreateRepositoryWithResponse(ctx,
			&api.CreateRepositoryParams{
				Bare: &bareRepo,
			}, api.CreateRepositoryJSONRequestBody{
				DefaultBranch:    api.StringPtr("main"),
				Name:             repoName,
				StorageNamespace: onBlock(deps, "foo-bucket-1"),
			})
		verifyResponseOK(t, resp, err)

		response := resp.JSON201
		if response == nil {
			t.Fatal("CreateRepository (bare) got bad response")
		}
		if response.Id != repoName {
			t.Fatalf("CreateRepository bare id=%s, expected=%s", response.Id, repoName)
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

func TestController_DeleteRepositoryHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("delete repo success", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)

		resp, err := clt.DeleteRepositoryWithResponse(ctx, repo)
		verifyResponseOK(t, resp, err)

		// delete again to expect repository not found
		resp, err = clt.DeleteRepositoryWithResponse(ctx, repo)
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatalf("expected repository to be gone (404), instead got status: %s", resp.Status())
		}
	})

	t.Run("delete repo doesnt exist", func(t *testing.T) {
		repo := testUniqueRepoName()
		resp, err := clt.DeleteRepositoryWithResponse(ctx, repo)
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
	clt, deps := setupClientWithAdmin(t)
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

func TestController_ListTagsHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

func TestController_GetBranchHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

func TestController_BranchesDiffBranchHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

func TestController_CreateBranchHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

	t.Run("create branch conflict with branch", func(t *testing.T) {
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

	t.Run("create branch conflict with tag", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo7", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)

		name := "tag123"
		_, err = deps.catalog.CreateTag(ctx, "repo7", name, "main")
		testutil.Must(t, err)

		resp, err := clt.CreateBranchWithResponse(ctx, "repo7", api.CreateBranchJSONRequestBody{
			Name:   name,
			Source: "main",
		})
		if err != nil {
			t.Fatal("CreateBranch failed with error:", err)
		}
		if resp.JSON409 == nil {
			t.Fatal("CreateBranch expected conflict")
		}
	})

	t.Run("create branch conflict with commit", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "repo8", onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)

		log, err := deps.catalog.GetCommit(ctx, "repo8", "main")
		testutil.Must(t, err)

		resp, err := clt.CreateBranchWithResponse(ctx, "repo6", api.CreateBranchJSONRequestBody{
			Name:   log.Reference,
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

func TestController_DiffRefs(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("diff prefix with and without delimiter", func(t *testing.T) {
		const repoName = "repo7"
		const newBranchName = "main2"
		_, err := deps.catalog.CreateRepository(ctx, repoName, onBlock(deps, "foo1"), "main")
		testutil.Must(t, err)

		resp, err := clt.CreateBranchWithResponse(ctx, repoName, api.CreateBranchJSONRequestBody{
			Name:   newBranchName,
			Source: "main",
		})
		verifyResponseOK(t, resp, err)
		reference := string(resp.Body)
		if len(reference) == 0 {
			t.Fatalf("branch %s creation got no reference", newBranchName)
		}
		const prefix = "some/"
		const path = "path"
		const fullPath = prefix + path
		const content = "hello world!"

		uploadResp, err := uploadObjectHelper(t, ctx, clt, fullPath, strings.NewReader(content), repoName, newBranchName)
		verifyResponseOK(t, uploadResp, err)

		if _, err := deps.catalog.Commit(ctx, repoName, newBranchName, "commit 1", "some_user", nil, nil, nil); err != nil {
			t.Fatalf("failed to commit 'repo1': %s", err)
		}
		resp2, err := clt.DiffRefsWithResponse(ctx, repoName, "main", newBranchName, &api.DiffRefsParams{})
		verifyResponseOK(t, resp2, err)
		results := resp2.JSON200.Results
		if len(results) != 1 {
			t.Fatalf("unexpected length of results: %d", len(results))
		}
		if results[0].Path != fullPath {
			t.Fatalf("wrong result: %s", results[0].Path)
		}
		if results[0].Type != "added" {
			t.Fatalf("wrong diff type: %s", results[0].Type)
		}

		delimiter := api.PaginationDelimiter("/")
		resp2, err = clt.DiffRefsWithResponse(ctx, repoName, "main", newBranchName, &api.DiffRefsParams{Delimiter: &delimiter})
		verifyResponseOK(t, resp2, err)
		results = resp2.JSON200.Results
		if len(results) != 1 {
			t.Fatalf("unexpected length of results: %d", len(results))
		}
		if results[0].Path != prefix {
			t.Fatalf("wrong result: %s", results[0].Path)
		}
		if results[0].Type != "prefix_changed" {
			t.Fatalf("wrong diff type: %s", results[0].Type)
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

func TestController_UploadObject(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

	t.Run("disable overwrite with if-none-match (no entry)", func(t *testing.T) {
		ifNoneMatch := api.StringPtr("*")
		contentType, buf := writeMultipart("content", "baz4", "something else!")
		resp, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &api.UploadObjectParams{
			Path:        "foo/baz4",
			IfNoneMatch: ifNoneMatch,
		}, contentType, buf)
		if err != nil {
			t.Fatalf("UploadObject err=%s, expected no error", err)
		}
		if resp.JSON201 == nil {
			t.Fatalf("UploadObject status code=%d, expected 201", resp.StatusCode())
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

func TestController_DeleteBranchHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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
		if !errors.Is(err, graveler.ErrNotFound) {
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

func TestController_IngestRangeHandler(t *testing.T) {
	const (
		fromSourceURI           = "https://valid.uri"
		uriPrefix               = "take/from/here"
		fromSourceURIWithPrefix = fromSourceURI + "/" + uriPrefix
		after                   = "some/key/to/start/after"
		prepend                 = "some/logical/prefix"
	)

	const continuationToken = "opaque"

	t.Run("ingest directory marker", func(t *testing.T) {
		ctx := context.Background()
		w := testutils.NewFakeWalker(0, 1, uriPrefix, after, continuationToken, fromSourceURIWithPrefix, nil)
		w.Entries = []store.ObjectStoreEntry{
			{
				RelativeKey: "",
				FullKey:     uriPrefix + "/",
				Address:     fromSourceURIWithPrefix + "/",
				ETag:        "dir_etag",
				Size:        0,
			},
		}
		clt, deps := setupClientWithAdminAndWalkerFactory(t, testutils.FakeFactory{Walker: w})
		_, err := deps.catalog.CreateRepository(ctx, "repo-dir-marker", onBlock(deps, "foo2"), "main")
		testutil.Must(t, err)

		resp, err := clt.IngestRangeWithResponse(ctx, "repo-dir-marker", api.IngestRangeJSONRequestBody{
			FromSourceURI:     fromSourceURIWithPrefix,
			ContinuationToken: swag.String(continuationToken),
			After:             after,
		})
		verifyResponseOK(t, resp, err)
		require.NotNil(t, resp.JSON201.Range)
		require.NotNil(t, resp.JSON201.Pagination)
		require.Equal(t, 1, resp.JSON201.Range.Count)
		require.Equal(t, resp.JSON201.Range.MinKey, "")
		require.Equal(t, resp.JSON201.Range.MaxKey, "")
		require.False(t, resp.JSON201.Pagination.HasMore)
		require.Empty(t, resp.JSON201.Pagination.LastKey)
		require.Empty(t, resp.JSON201.Pagination.ContinuationToken)
	})

	t.Run("successful ingestion no pagination", func(t *testing.T) {
		ctx := context.Background()
		count := 1000
		clt, w := func(t *testing.T, count int, expectedErr error) (api.ClientWithResponsesInterface, *testutils.FakeWalker) {
			t.Helper()
			ctx := context.Background()

			w := testutils.NewFakeWalker(count, count, uriPrefix, after, continuationToken, fromSourceURIWithPrefix, expectedErr)
			clt, deps := setupClientWithAdminAndWalkerFactory(t, testutils.FakeFactory{Walker: w})

			// setup test data
			_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), "main")
			testutil.Must(t, err)

			return clt, w
		}(t, count, nil)

		resp, err := clt.IngestRangeWithResponse(ctx, "repo1", api.IngestRangeJSONRequestBody{
			After:             after,
			FromSourceURI:     fromSourceURIWithPrefix,
			Prepend:           prepend,
			ContinuationToken: swag.String(continuationToken),
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
		clt, w := func(t *testing.T, count int, expectedErr error) (api.ClientWithResponsesInterface, *testutils.FakeWalker) {
			t.Helper()
			ctx := context.Background()

			w := testutils.NewFakeWalker(count, count, uriPrefix, after, continuationToken, fromSourceURIWithPrefix, expectedErr)
			clt, deps := setupClientWithAdminAndWalkerFactory(t, testutils.FakeFactory{Walker: w})

			// setup test data
			_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), "main")
			testutil.Must(t, err)

			return clt, w
		}(t, count, nil)

		resp, err := clt.IngestRangeWithResponse(ctx, "repo1", api.IngestRangeJSONRequestBody{
			After:             after,
			FromSourceURI:     fromSourceURIWithPrefix,
			Prepend:           prepend,
			ContinuationToken: swag.String(continuationToken),
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
		clt, _ := func(t *testing.T, count int, expectedErr error) (api.ClientWithResponsesInterface, *testutils.FakeWalker) {
			t.Helper()
			ctx := context.Background()

			w := testutils.NewFakeWalker(count, count, uriPrefix, after, continuationToken, fromSourceURIWithPrefix, expectedErr)
			clt, deps := setupClientWithAdminAndWalkerFactory(t, testutils.FakeFactory{Walker: w})

			// setup test data
			_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "foo1"), "main")
			testutil.Must(t, err)

			return clt, w
		}(t, count, expectedErr)

		resp, err := clt.IngestRangeWithResponse(ctx, "repo1", api.IngestRangeJSONRequestBody{
			After:             after,
			FromSourceURI:     fromSourceURIWithPrefix,
			Prepend:           prepend,
			ContinuationToken: swag.String(continuationToken),
		})

		require.NoError(t, err)
		require.Equal(t, http.StatusInternalServerError, resp.StatusCode())
		require.Contains(t, string(resp.Body), expectedErr.Error())
	})
}

func TestController_WriteMetaRangeHandler(t *testing.T) {
	ctx := context.Background()
	clt, deps := setupClientWithAdmin(t)
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

func TestController_ObjectsStatObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

func TestController_ObjectsListObjectsHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

func TestController_ObjectsHeadObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", "ns1", "main")
	if err != nil {
		t.Fatal(err)
	}

	expensiveString := "EXPENSIVE"

	buf := new(bytes.Buffer)
	buf.WriteString("this is file content made up of bytes")
	address := upload.DefaultPathProvider.NewPath()
	blob, err := upload.WriteBlob(context.Background(), deps.blocks, "ns1", address, buf, 37, block.PutOpts{StorageClass: &expensiveString})
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

	t.Run("head object", func(t *testing.T) {
		resp, err := clt.HeadObjectWithResponse(ctx, "repo1", "main", &api.HeadObjectParams{Path: "foo/bar"})
		if err != nil {
			t.Fatal(err)
		}
		if resp.HTTPResponse.StatusCode != http.StatusOK {
			t.Errorf("HeadObject() status code %d, expected %d", resp.HTTPResponse.StatusCode, http.StatusOK)
		}

		if resp.HTTPResponse.ContentLength != 37 {
			t.Errorf("expected 37 bytes in content length, got back %d", resp.HTTPResponse.ContentLength)
		}
		etag := resp.HTTPResponse.Header.Get("ETag")
		if etag != `"3c4838fe975c762ee97cf39fbbe566f1"` {
			t.Errorf("got unexpected etag: %s", etag)
		}

		body := string(resp.Body)
		if body != "" {
			t.Errorf("got unexpected body: '%s'", body)
		}
	})

	t.Run("head object byte range", func(t *testing.T) {
		rng := "bytes=0-9"
		resp, err := clt.HeadObjectWithResponse(ctx, "repo1", "main", &api.HeadObjectParams{
			Path:  "foo/bar",
			Range: &rng,
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp.HTTPResponse.StatusCode != http.StatusPartialContent {
			t.Errorf("HeadObject() status code %d, expected %d", resp.HTTPResponse.StatusCode, http.StatusPartialContent)
		}

		if resp.HTTPResponse.ContentLength != 10 {
			t.Errorf("expected 10 bytes in content length, got back %d", resp.HTTPResponse.ContentLength)
		}

		etag := resp.HTTPResponse.Header.Get("ETag")
		if etag != `"3c4838fe975c762ee97cf39fbbe566f1"` {
			t.Errorf("got unexpected etag: %s", etag)
		}

		body := string(resp.Body)
		if body != "" {
			t.Errorf("got unexpected body: '%s'", body)
		}
	})

	t.Run("head object bad byte range", func(t *testing.T) {
		rng := "bytes=380-390"
		resp, err := clt.HeadObjectWithResponse(ctx, "repo1", "main", &api.HeadObjectParams{
			Path:  "foo/bar",
			Range: &rng,
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp.HTTPResponse.StatusCode != http.StatusRequestedRangeNotSatisfiable {
			t.Errorf("HeadObject() status code %d, expected %d", resp.HTTPResponse.StatusCode, http.StatusRequestedRangeNotSatisfiable)
		}
	})
}

func TestController_ObjectsGetObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", "ns1", "main")
	if err != nil {
		t.Fatal(err)
	}

	expensiveString := "EXPENSIVE"

	buf := new(bytes.Buffer)
	buf.WriteString("this is file content made up of bytes")
	address := upload.DefaultPathProvider.NewPath()
	blob, err := upload.WriteBlob(context.Background(), deps.blocks, "ns1", address, buf, 37, block.PutOpts{StorageClass: &expensiveString})
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
			t.Errorf("GetObject() status code %d, expected %d", resp.HTTPResponse.StatusCode, http.StatusOK)
		}

		if resp.HTTPResponse.ContentLength != 37 {
			t.Errorf("expected 37 bytes in content length, got back %d", resp.HTTPResponse.ContentLength)
		}
		etag := resp.HTTPResponse.Header.Get("ETag")
		if etag != `"3c4838fe975c762ee97cf39fbbe566f1"` {
			t.Errorf("got unexpected etag: %s", etag)
		}

		body := string(resp.Body)
		if body != "this is file content made up of bytes" {
			t.Errorf("got unexpected body: '%s'", body)
		}
	})

	t.Run("get object byte range", func(t *testing.T) {
		rng := "bytes=0-9"
		resp, err := clt.GetObjectWithResponse(ctx, "repo1", "main", &api.GetObjectParams{
			Path:  "foo/bar",
			Range: &rng,
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp.HTTPResponse.StatusCode != http.StatusPartialContent {
			t.Errorf("GetObject() status code %d, expected %d", resp.HTTPResponse.StatusCode, http.StatusPartialContent)
		}

		if resp.HTTPResponse.ContentLength != 10 {
			t.Errorf("expected 10 bytes in content length, got back %d", resp.HTTPResponse.ContentLength)
		}

		etag := resp.HTTPResponse.Header.Get("ETag")
		if etag != `"3c4838fe975c762ee97cf39fbbe566f1"` {
			t.Errorf("got unexpected etag: %s", etag)
		}

		body := string(resp.Body)
		if body != "this is fi" {
			t.Errorf("got unexpected body: '%s'", body)
		}
	})

	t.Run("get object bad byte range", func(t *testing.T) {
		rng := "bytes=380-390"
		resp, err := clt.GetObjectWithResponse(ctx, "repo1", "main", &api.GetObjectParams{
			Path:  "foo/bar",
			Range: &rng,
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp.HTTPResponse.StatusCode != http.StatusRequestedRangeNotSatisfiable {
			t.Errorf("GetObject() status code %d, expected %d", resp.HTTPResponse.StatusCode, http.StatusRequestedRangeNotSatisfiable)
		}
	})

	t.Run("get properties", func(t *testing.T) {
		resp, err := clt.GetUnderlyingPropertiesWithResponse(ctx, "repo1", "main", &api.GetUnderlyingPropertiesParams{Path: "foo/bar"})
		if err != nil {
			t.Fatalf("expected to get underlying properties, got %v", err)
		}
		properties := resp.JSON200
		if properties == nil {
			t.Fatalf("expected to get underlying properties, status code %d", resp.StatusCode())
		}

		if api.StringValue(properties.StorageClass) != expensiveString {
			t.Errorf("expected to get \"%s\" storage class, got %#v", expensiveString, properties)
		}
	})
}

func TestController_ObjectsUploadObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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
	clt, deps := setupClientWithAdmin(t)
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

	t.Run("stage object in storage ns", func(t *testing.T) {
		linkResp, err := clt.GetPhysicalAddressWithResponse(ctx, "repo1", "main", &api.GetPhysicalAddressParams{Path: "foo/bar2"})
		verifyResponseOK(t, linkResp, err)
		if linkResp.JSON200 == nil {
			t.Fatalf("GetPhysicalAddress non 200 response - status code %d", linkResp.StatusCode())
		}
		const expectedSizeBytes = 38
		resp, err := clt.LinkPhysicalAddressWithResponse(ctx, "repo1", "main", &api.LinkPhysicalAddressParams{
			Path: "foo/bar2",
		}, api.LinkPhysicalAddressJSONRequestBody{
			Checksum:  "afb0689fe58b82c5f762991453edbbec",
			SizeBytes: expectedSizeBytes,
			Staging: api.StagingLocation{
				PhysicalAddress: linkResp.JSON200.PhysicalAddress,
				Token:           linkResp.JSON200.Token,
			},
		})
		verifyResponseOK(t, resp, err)

		sizeBytes := api.Int64Value(resp.JSON200.SizeBytes)
		if sizeBytes != expectedSizeBytes {
			t.Fatalf("expected %d bytes to be written, got back %d", expectedSizeBytes, sizeBytes)
		}

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, "repo1", "main", &api.StatObjectParams{Path: "foo/bar2"})
		verifyResponseOK(t, statResp, err)
		if statResp.JSON200 == nil {
			t.Fatalf("StatObject non 200 - status code %d", statResp.StatusCode())
		}
		objectStat := statResp.JSON200
		if objectStat.PhysicalAddress != api.StringValue(linkResp.JSON200.PhysicalAddress) {
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

func TestController_LinkPhysicalAddressHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "bucket/prefix"), "main")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("get and link physical address", func(t *testing.T) {
		linkResp, err := clt.GetPhysicalAddressWithResponse(ctx, "repo1", "main", &api.GetPhysicalAddressParams{Path: "foo/bar2"})
		verifyResponseOK(t, linkResp, err)
		if linkResp.JSON200 == nil {
			t.Fatalf("GetPhysicalAddress non 200 response - status code %d", linkResp.StatusCode())
		}
		const expectedSizeBytes = 38
		resp, err := clt.LinkPhysicalAddressWithResponse(ctx, "repo1", "main", &api.LinkPhysicalAddressParams{
			Path: "foo/bar2",
		}, api.LinkPhysicalAddressJSONRequestBody{
			Checksum:  "afb0689fe58b82c5f762991453edbbec",
			SizeBytes: expectedSizeBytes,
			Staging: api.StagingLocation{
				PhysicalAddress: linkResp.JSON200.PhysicalAddress,
				Token:           linkResp.JSON200.Token,
			},
		})
		verifyResponseOK(t, resp, err)
	})

	t.Run("link physical address twice", func(t *testing.T) {
		linkResp, err := clt.GetPhysicalAddressWithResponse(ctx, "repo1", "main", &api.GetPhysicalAddressParams{Path: "foo/bar2"})
		verifyResponseOK(t, linkResp, err)
		if linkResp.JSON200 == nil {
			t.Fatalf("GetPhysicalAddress non 200 response - status code %d", linkResp.StatusCode())
		}
		const expectedSizeBytes = 38
		resp, err := clt.LinkPhysicalAddressWithResponse(ctx, "repo1", "main", &api.LinkPhysicalAddressParams{
			Path: "foo/bar2",
		}, api.LinkPhysicalAddressJSONRequestBody{
			Checksum:  "afb0689fe58b82c5f762991453edbbec",
			SizeBytes: expectedSizeBytes,
			Staging: api.StagingLocation{
				PhysicalAddress: linkResp.JSON200.PhysicalAddress,
				Token:           linkResp.JSON200.Token,
			},
		})
		verifyResponseOK(t, resp, err)

		resp, err = clt.LinkPhysicalAddressWithResponse(ctx, "repo1", "main", &api.LinkPhysicalAddressParams{
			Path: "foo/bar2",
		}, api.LinkPhysicalAddressJSONRequestBody{
			Checksum:  "afb0689fe58b82c5f762991453edbbec",
			SizeBytes: expectedSizeBytes,
			Staging: api.StagingLocation{
				PhysicalAddress: linkResp.JSON200.PhysicalAddress,
				Token:           linkResp.JSON200.Token,
			},
		})
		testutil.Must(t, err)
		if resp.HTTPResponse.StatusCode != http.StatusNotFound {
			t.Fatalf("expected error linking the same physical address twice")
		}
	})
}

func TestController_ObjectsDeleteObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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
		if delResp.JSON200 == nil {
			t.Errorf("DeleteObjects should return 200 for successful delete, got status code %d", delResp.StatusCode())
		}
		if len(delResp.JSON200.Errors) > 0 {
			t.Errorf("DeleteObjects (round 2) should have no errors, got %v", delResp.JSON200.Errors)
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

		// delete objects again - make sure we do not fail or get any error
		delResp2, err := clt.DeleteObjectsWithResponse(ctx, repo, branch, api.DeleteObjectsJSONRequestBody{Paths: paths})
		verifyResponseOK(t, delResp2, err)
		if delResp2.JSON200 == nil {
			t.Errorf("DeleteObjects (round 2) should return 200 for successful delete, got status code %d", delResp2.StatusCode())
		}
		if len(delResp2.JSON200.Errors) > 0 {
			t.Errorf("DeleteObjects (round 2) should have no errors, got %s", spew.Sdump(delResp2.JSON200.Errors))
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

func TestController_CreatePolicyHandler(t *testing.T) {
	clt, _ := setupClientWithAdmin(t)
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
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("Get storage config", func(t *testing.T) {
		ExpectedExample := onBlock(deps, "example-bucket/")
		resp, err := clt.GetStorageConfigWithResponse(ctx)
		verifyResponseOK(t, resp, err)

		example := resp.JSON200.BlockstoreNamespaceExample
		if example != ExpectedExample {
			t.Errorf("expected to get %s, got %s", ExpectedExample, example)
		}
	})

	t.Run("Get gc config", func(t *testing.T) {
		expectedPeriod := int((6 * time.Hour).Seconds())
		resp, err := clt.GetGarbageCollectionConfigWithResponse(ctx)
		verifyResponseOK(t, resp, err)
		period := resp.JSON200.GracePeriod
		if *period != expectedPeriod {
			t.Errorf("expected to get %d, got %d", expectedPeriod, period)
		}
	})
}

func TestController_SetupLakeFSHandler(t *testing.T) {
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
			handler, deps := setupHandler(t)
			server := setupServer(t, handler)
			clt := setupClientByEndpoint(t, server.URL, "", "")

			ctx := context.Background()
			mockEmail := "test@acme.co"
			_, _ = clt.SetupCommPrefsWithResponse(ctx, api.SetupCommPrefsJSONRequestBody{
				Email:           &mockEmail,
				FeatureUpdates:  false,
				SecurityUpdates: false,
			})

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

func TestLogin(t *testing.T) {
	const configureDuration = "48h"
	viper.Set("auth.login_duration", configureDuration)

	handler, deps := setupHandler(t)
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	_ = setupCommPrefs(t, clt)
	cred := createDefaultAdminUser(t, clt)

	resp, err := clt.LoginWithResponse(context.Background(), api.LoginJSONRequestBody{
		AccessKeyId:     cred.AccessKeyID,
		SecretAccessKey: cred.SecretAccessKey,
	})
	if err != nil {
		t.Errorf("Error login with response %v", err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("expected response from status 200 got %d", resp.StatusCode())
	}
	res := resp.JSON200
	claims, err := auth.VerifyToken(deps.authService.SecretStore().SharedSecret(), res.Token)
	testutil.Must(t, err)
	resultExpiry := swag.Int64Value(res.TokenExpiration)
	if claims.ExpiresAt != resultExpiry {
		t.Errorf("token expiry (%d) not equal to expiry result (%d)", claims.ExpiresAt, resultExpiry)
	}

	// login duration
	loginDuration, err := time.ParseDuration(configureDuration)
	testutil.Must(t, err)
	tokenDuration := time.Duration(claims.ExpiresAt-claims.IssuedAt) * time.Second
	if (tokenDuration - loginDuration).Abs() > time.Minute {
		t.Errorf("token duration should be around %v got %v", loginDuration, tokenDuration)
	}

	// validate issued at
	issueSince := time.Since(time.Unix(claims.IssuedAt, 0))
	if issueSince > 5*time.Minute && issueSince < 0 {
		t.Errorf("issue since %s expected last five minutes", issueSince)
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
	clt, _ := setupClientWithAdmin(t)
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

func TestController_MergeDiffWithParent(t *testing.T) {
	clt, _ := setupClientWithAdmin(t)
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

func TestController_MergeIntoExplicitBranch(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

func TestController_MergeDirtyBranch(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	// setup env
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
	testutil.Must(t, err)
	err = deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"})
	testutil.Must(t, err)
	_, err = deps.catalog.CreateBranch(ctx, repo, "branch1", "main")
	testutil.Must(t, err)
	err = deps.catalog.CreateEntry(ctx, repo, "branch1", catalog.DBEntry{Path: "foo/bar2", PhysicalAddress: "bar2addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum2"})
	testutil.Must(t, err)
	_, err = deps.catalog.Commit(ctx, repo, "branch1", "some message", DefaultUserID, nil, nil, nil)
	testutil.Must(t, err)

	// merge branch1 to main (dirty)
	resp, err := clt.MergeIntoBranchWithResponse(ctx, repo, "branch1", "main", api.MergeIntoBranchJSONRequestBody{})
	testutil.MustDo(t, "perform merge into dirty branch", err)
	if resp.JSON400 == nil || resp.JSON400.Message != graveler.ErrDirtyBranch.Error() {
		t.Errorf("Merge dirty branch should fail with ErrDirtyBranch, got %+v", resp)
	}
}

func TestController_CreateTag(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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
		if tagResp.JSON400 == nil {
			t.Errorf("Create tag to explicit stage should fail with validation error, got (status code: %d): %s", tagResp.StatusCode(), tagResp.Body)
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

	t.Run("tag_with_conflicting_tag", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  "tag7",
			Ref: "main",
		})
		verifyResponseOK(t, tagResp, err)
		tagTagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  "tag7",
			Ref: "main",
		})
		testutil.Must(t, err)
		if tagTagResp.JSON409 == nil {
			t.Errorf("Create tag again should conflict, got %v", tagTagResp)
		}
	})

	t.Run("tag_with_conflicting_branch", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  "main",
			Ref: "main",
		})

		testutil.Must(t, err)
		if tagResp.JSON409 == nil {
			t.Errorf("Create tag again should conflict, got %v", tagResp)
		}
	})

	t.Run("tag_with_conflicting_commit", func(t *testing.T) {
		commit, err := deps.catalog.GetCommit(ctx, repo, "main")
		testutil.Must(t, err)

		tagResp, err := clt.CreateTagWithResponse(ctx, repo, api.CreateTagJSONRequestBody{
			Id:  commit.Reference,
			Ref: "main",
		})
		testutil.Must(t, err)

		if tagResp.JSON409 == nil {
			t.Errorf("Create tag again should conflict, got %v", tagResp)
		}
	})
}

func testUniqueRepoName() string {
	return "repo-" + nanoid.MustGenerate("abcdef1234567890", 8)
}

func TestController_Revert(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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
		if revertResp.JSON400 == nil {
			t.Errorf("Revert should fail with stating reference, got (status code: %d): %s", revertResp.StatusCode(), revertResp.Body)
		}
	})

	t.Run("dirty_branch", func(t *testing.T) {
		// create branch with entry without commit
		createBranch, err := deps.catalog.CreateBranch(ctx, repo, "dirty", "main")
		testutil.Must(t, err)
		err = deps.catalog.CreateEntry(ctx, repo, "dirty", catalog.DBEntry{Path: "foo/bar2", PhysicalAddress: "bar2addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum2"})
		testutil.Must(t, err)

		// revert changes should fail
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "dirty", api.RevertBranchJSONRequestBody{Ref: createBranch.Reference})
		testutil.Must(t, err)
		if revertResp.JSON400 == nil || revertResp.JSON400.Message != graveler.ErrDirtyBranch.Error() {
			t.Errorf("Revert dirty branch should fail with ErrDirtyBranch, got %+v", revertResp)
		}
	})

	t.Run("revert_no_parent", func(t *testing.T) {
		repo := testUniqueRepoName()
		// setup data - repo with one object committed
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
		testutil.Must(t, err)
		err = deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "merge/foo/bar1", PhysicalAddress: "merge1bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"})
		testutil.Must(t, err)
		_, err = deps.catalog.Commit(ctx, repo, "main", "first", DefaultUserID, nil, nil, nil)
		testutil.Must(t, err)
		// create branch with one entry committed
		_, err = deps.catalog.CreateBranch(ctx, repo, "branch1", "main")
		testutil.Must(t, err)
		err = deps.catalog.CreateEntry(ctx, repo, "branch1", catalog.DBEntry{Path: "merge/foo/bar2", PhysicalAddress: "merge2bar2addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum2"})
		testutil.Must(t, err)
		_, err = deps.catalog.Commit(ctx, repo, "branch1", "second", DefaultUserID, nil, nil, nil)
		testutil.Must(t, err)
		// merge branch1 to main
		mergeRef, err := deps.catalog.Merge(ctx, repo, "main", "branch1", DefaultUserID, "merge to main", nil, "")
		testutil.Must(t, err)

		// revert changes should fail
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "main", api.RevertBranchJSONRequestBody{Ref: mergeRef})
		testutil.Must(t, err)
		if revertResp.JSON409 == nil || revertResp.JSON409.Message != graveler.ErrRevertMergeNoParent.Error() {
			t.Errorf("Revert dirty merge no parent specified was expected, got %+v", revertResp)
		}
	})
}

func TestController_RevertConflict(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
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

func TestController_ExpandTemplate(t *testing.T) {
	clt, _ := setupClientWithAdmin(t)
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
			func(_ context.Context, req *http.Request) error {
				values := req.URL.Query()
				values.Add("lakefs_url", lfsURL)
				req.URL.RawQuery = values.Encode()
				return nil
			})
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

func TestController_UpdatePolicy(t *testing.T) {
	clt, _ := setupClientWithAdmin(t)
	ctx := context.Background()

	// test policy
	now := api.Int64Ptr(time.Now().Unix())
	const existingPolicyID = "TestUpdatePolicy"
	response, err := clt.CreatePolicyWithResponse(ctx, api.CreatePolicyJSONRequestBody{
		CreationDate: now,
		Id:           existingPolicyID,
		Statement: []api.Statement{
			{
				Action: []string{
					"fs:Read*",
					"fs:List*",
				},
				Effect:   "deny",
				Resource: "*",
			},
		},
	})
	testutil.Must(t, err)
	if response.JSON201 == nil {
		t.Fatal("Failed to create test policy", response.Status())
	}

	t.Run("unknown", func(t *testing.T) {
		const policyID = "UnknownPolicy"
		updatePolicyResponse, err := clt.UpdatePolicyWithResponse(ctx, policyID, api.UpdatePolicyJSONRequestBody{
			CreationDate: now,
			Id:           policyID,
			Statement: []api.Statement{
				{
					Action: []string{
						"fs:Read*",
						"fs:List*",
					},
					Effect:   "allow",
					Resource: "*",
				},
			},
		})
		testutil.Must(t, err)
		if updatePolicyResponse.JSON404 == nil {
			t.Errorf("Update unknown policy should fail with 404: %s", updatePolicyResponse.Status())
		}
	})

	t.Run("change_effect", func(t *testing.T) {
		updatePolicyResponse, err := clt.UpdatePolicyWithResponse(ctx, existingPolicyID, api.UpdatePolicyJSONRequestBody{
			CreationDate: now,
			Id:           existingPolicyID,
			Statement: []api.Statement{
				{
					Action: []string{
						"fs:Read*",
						"fs:List*",
					},
					Effect:   "allow",
					Resource: "*",
				},
			},
		})
		testutil.Must(t, err)
		if updatePolicyResponse.JSON200 == nil {
			t.Errorf("Update policy failed: %s", updatePolicyResponse.Status())
		}
	})

	t.Run("change_policy_id", func(t *testing.T) {
		updatePolicyResponse, err := clt.UpdatePolicyWithResponse(ctx, "SomethingElse", api.UpdatePolicyJSONRequestBody{
			CreationDate: now,
			Id:           existingPolicyID,
			Statement: []api.Statement{
				{
					Action: []string{
						"fs:Read*",
					},
					Effect:   "allow",
					Resource: "*",
				},
			},
		})
		testutil.Must(t, err)
		if updatePolicyResponse.JSON400 == nil {
			t.Errorf("Update policy with different id should fail with 400: %s", updatePolicyResponse.Status())
		}
	})
}

func TestController_GetPhysicalAddress(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("physical_address_format", func(t *testing.T) {
		const (
			repo   = "repo1"
			ns     = "s3://foo-bucket1"
			branch = "main"
		)
		_, err := deps.catalog.CreateRepository(ctx, repo, ns, branch)
		if err != nil {
			t.Fatal(err)
		}

		var prevPartitionTime time.Time
		const links = 5
		for i := 0; i < links; i++ {
			params := &api.GetPhysicalAddressParams{
				Path: "get-path/obj" + strconv.Itoa(i),
			}
			resp, err := clt.GetPhysicalAddressWithResponse(ctx, repo, branch, params)
			if err != nil {
				t.Fatalf("GetPhysicalAddressWithResponse %s, failed: %s", params.Path, err)
			}
			if resp.JSON200 == nil {
				t.Fatalf("GetPhysicalAddressWithResponse %s, non JSON 200 response: %s", params.Path, resp.Status())
			}

			address := api.StringValue(resp.JSON200.PhysicalAddress)
			t.Log(address)

			const expectedPrefix = ns + "/" + upload.DefaultDataPrefix + "/"
			if !strings.HasPrefix(address, expectedPrefix) {
				t.Fatalf("GetPhysicalAddressWithResponse address=%s, expected prefix=%s", address, expectedPrefix)
			}
			const expectedParts = 3
			objPath := address[len(ns)+1:]
			parts := strings.Split(objPath, "/")
			if len(parts) != expectedParts {
				t.Fatalf("GetPhysicalAddressWithResponse path=%s, expected %d parts", objPath, expectedParts)
			}
			partitionID, err := xid.FromString(parts[1])
			if err != nil {
				t.Fatalf("GetPhysicalAddressWithResponse unknown partition format (path=%s): %s", objPath, err)
			}
			partitionTime := partitionID.Time()
			if i > 0 && partitionTime.After(prevPartitionTime) {
				t.Fatalf("GetPhysicalAddressWithResponse partition time '%s', should be equal or smaller than previous '%s'", partitionTime, prevPartitionTime)
			}
			prevPartitionTime = partitionTime
		}
	})
}

func TestController_PrepareGarbageCollectionUncommitted(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("no_repository", func(t *testing.T) {
		resp, err := clt.PrepareGarbageCollectionUncommittedWithResponse(ctx, "", api.PrepareGarbageCollectionUncommittedJSONRequestBody{})
		if err != nil {
			t.Fatalf("PrepareGarbageCollectionUncommittedWithResponse failed: %s", err)
		}
		if resp.JSON400 == nil {
			t.Fatalf("PrepareGarbageCollectionUncommittedWithResponse expected BadRequest: %+v", resp)
		}
	})

	t.Run("repository_not_exists", func(t *testing.T) {
		repo := testUniqueRepoName()
		resp, err := clt.PrepareGarbageCollectionUncommittedWithResponse(ctx, repo, api.PrepareGarbageCollectionUncommittedJSONRequestBody{})
		if err != nil {
			t.Fatalf("PrepareGarbageCollectionUncommittedWithResponse failed: %s", err)
		}
		if resp.JSON404 == nil {
			t.Fatalf("PrepareGarbageCollectionUncommittedWithResponse expected NotFound: %+v", resp)
		}
	})

	t.Run("uncommitted_data", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
		testutil.Must(t, err)
		const items = 3
		for i := 0; i < items; i++ {
			path := fmt.Sprintf("uncommitted/obj%d", i)
			uploadResp, err := uploadObjectHelper(t, ctx, clt, path, strings.NewReader(path), repo, "main")
			verifyResponseOK(t, uploadResp, err)
		}
		// first call should return the list uncommitted object from staging
		resp, err := clt.PrepareGarbageCollectionUncommittedWithResponse(ctx, repo, api.PrepareGarbageCollectionUncommittedJSONRequestBody{})
		verifyResponseOK(t, resp, err)
		if resp.JSON201 == nil {
			t.Fatalf("PrepareGarbageCollectionUncommittedWithResponse status code:%d, expected 201", resp.StatusCode())
		}
		if resp.JSON201.RunId == "" {
			t.Errorf("PrepareGarbageCollectionUncommittedWithResponse empty RunID, value expected")
		}
		if resp.JSON201.GcUncommittedLocation == "" {
			t.Errorf("PrepareGarbageCollectionUncommittedWithResponse empty GcUncommittedLocation, value expected")
		}
		token := api.StringValue(resp.JSON201.ContinuationToken)
		if token == "" {
			t.Fatal("PrepareGarbageCollectionUncommittedWithResponse ContinuationToken, expected value")
		}

		// second call should return the list tracked linked addresses
		resp, err = clt.PrepareGarbageCollectionUncommittedWithResponse(ctx, repo, api.PrepareGarbageCollectionUncommittedJSONRequestBody{
			ContinuationToken: resp.JSON201.ContinuationToken,
		})
		verifyResponseOK(t, resp, err)
		if resp.JSON201 == nil {
			t.Fatalf("PrepareGarbageCollectionUncommittedWithResponse status code:%d, expected 201", resp.StatusCode())
		}
		if resp.JSON201.RunId == "" {
			t.Errorf("PrepareGarbageCollectionUncommittedWithResponse empty RunID, value expected")
		}
		if resp.JSON201.GcUncommittedLocation == "" {
			t.Errorf("PrepareGarbageCollectionUncommittedWithResponse empty GcUncommittedLocation, value expected")
		}
		token = api.StringValue(resp.JSON201.ContinuationToken)
		if token != "" {
			t.Errorf("PrepareGarbageCollectionUncommittedWithResponse ContinuationToken=%s, expected no value", token)
		}
	})

	t.Run("committed_data", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main")
		testutil.Must(t, err)
		const items = 3
		for i := 0; i < items; i++ {
			path := fmt.Sprintf("committed/obj%d", i)
			uploadResp, err := uploadObjectHelper(t, ctx, clt, path, strings.NewReader(path), repo, "main")
			verifyResponseOK(t, uploadResp, err)
		}
		if _, err := deps.catalog.Commit(ctx, repo, "main", "committed objects", "some_user", nil, nil, nil); err != nil {
			t.Fatalf("failed to commit objects: %s", err)
		}
		resp, err := clt.PrepareGarbageCollectionUncommittedWithResponse(ctx, repo, api.PrepareGarbageCollectionUncommittedJSONRequestBody{})
		verifyResponseOK(t, resp, err)
		if resp.JSON201 == nil {
			t.Fatalf("PrepareGarbageCollectionUncommittedWithResponse status code:%d, expected 201", resp.StatusCode())
		}
		if resp.JSON201.RunId == "" {
			t.Errorf("PrepareGarbageCollectionUncommittedWithResponse empty RunID, value expected")
		}
		if resp.JSON201.GcUncommittedLocation == "" {
			t.Errorf("PrepareGarbageCollectionUncommittedWithResponse Location, value expected")
		}
		token := api.StringValue(resp.JSON201.ContinuationToken)
		if token == "" {
			t.Fatal("PrepareGarbageCollectionUncommittedWithResponse ContinuationToken, value expected")
		}

		resp, err = clt.PrepareGarbageCollectionUncommittedWithResponse(ctx, repo, api.PrepareGarbageCollectionUncommittedJSONRequestBody{
			ContinuationToken: resp.JSON201.ContinuationToken,
		})
		verifyResponseOK(t, resp, err)
		if resp.JSON201 == nil {
			t.Fatalf("PrepareGarbageCollectionUncommitted status code:%d, expected 201", resp.StatusCode())
		}
		if resp.JSON201.RunId == "" {
			t.Errorf("PrepareGarbageCollectionUncommitted empty RunID, value expected")
		}
		if resp.JSON201.GcUncommittedLocation == "" {
			t.Errorf("PrepareGarbageCollectionUncommitted Location, value expected")
		}
		token = api.StringValue(resp.JSON201.ContinuationToken)
		if token != "" {
			t.Errorf("PrepareGarbageCollectionUncommitted token=%s, expected empty token", token)
		}
	})
}

func TestController_ClientDisconnect(t *testing.T) {
	handler, deps := setupHandlerWithWalkerFactory(t, store.NewFactory(nil))

	// setup lakefs
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	_ = setupCommPrefs(t, clt)
	cred := createDefaultAdminUser(t, clt)

	// setup repository
	ctx := context.Background()
	_, err := deps.catalog.CreateRepository(ctx, "repo1", onBlock(deps, "repo1"), "main")
	testutil.Must(t, err)

	// prepare a client that will not wait for a response and timeout
	dialer := net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			ResponseHeaderTimeout: time.Nanosecond,
			DialContext:           dialer.DialContext,
			DisableKeepAlives:     true,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     false,
		},
	}
	clt = setupClientByEndpoint(t, server.URL, cred.AccessKeyID, cred.SecretAccessKey, api.WithHTTPClient(httpClient))

	// upload request
	contentType, reader := writeMultipart("content", "file.data", "something special")
	_, err = clt.UploadObjectWithBodyWithResponse(ctx, "repo1", "main", &api.UploadObjectParams{
		Path: "test/file.data",
	}, contentType, reader)
	if err == nil {
		t.Fatal("Expected to request complete without error, expected to fail")
	}

	// wait for server to identify we left and update the counter
	time.Sleep(time.Second)

	// request for metrics
	metricsResp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = metricsResp.Body.Close()
	}()
	body, err := io.ReadAll(metricsResp.Body)
	if err != nil {
		t.Fatal(err)
	}

	// process relevant metrics
	const apiReqTotalMetricLabel = `api_requests_total{code="499",method="post"}`
	var clientRequestClosedCount int
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, apiReqTotalMetricLabel) {
			if count, err := strconv.Atoi(line[len(apiReqTotalMetricLabel)+1:]); err == nil {
				clientRequestClosedCount += count
			}
		}
	}

	const expectedCount = 1
	if clientRequestClosedCount != expectedCount {
		t.Fatalf("Metric for client request closed: %d, expected: %d", clientRequestClosedCount, expectedCount)
	}
}

func TestController_PostStatsEvents(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	type key struct {
		class string
		name  string
	}

	tests := []struct {
		name                string
		events              []api.StatsEvent
		expectedEventCounts map[key]int
		expectedStatusCode  int
	}{
		{
			name: "single_event_count_1",
			events: []api.StatsEvent{
				{
					Class: "single_event_count_1",
					Name:  "name",
					Count: 1,
				},
			},
			expectedEventCounts: map[key]int{
				{class: "single_event_count_1", name: "name"}: 1,
			},
			expectedStatusCode: http.StatusNoContent,
		},
		{
			name: "single_event_count_gt_1",
			events: []api.StatsEvent{
				{
					Class: "single_event_count_gt_1",
					Name:  "name",
					Count: 3,
				},
			},
			expectedEventCounts: map[key]int{
				{class: "single_event_count_gt_1", name: "name"}: 3,
			},
			expectedStatusCode: http.StatusNoContent,
		},
		{
			name: "multiple_events",
			events: []api.StatsEvent{
				{
					Class: "class_multiple_events_ev_1",
					Name:  "name1",
					Count: 1,
				},
				{
					Class: "class_multiple_events_ev_2",
					Name:  "name2",
					Count: 1,
				},
			},
			expectedEventCounts: map[key]int{
				{class: "class_multiple_events_ev_1", name: "name1"}: 1,
				{class: "class_multiple_events_ev_2", name: "name2"}: 1,
			},
			expectedStatusCode: http.StatusNoContent,
		},
		{
			name: "multiple_events_same_class",
			events: []api.StatsEvent{
				{
					Class: "class_multiple_events_same_class",
					Name:  "name1",
					Count: 1,
				},
				{
					Class: "class_multiple_events_same_class",
					Name:  "name2",
					Count: 1,
				},
			},
			expectedEventCounts: map[key]int{
				{class: "class_multiple_events_same_class", name: "name1"}: 1,
				{class: "class_multiple_events_same_class", name: "name2"}: 1,
			},
			expectedStatusCode: http.StatusNoContent,
		},
		{
			name: "multiple_events_same_name",
			events: []api.StatsEvent{
				{
					Class: "multiple_events_same_name_1",
					Name:  "same_name",
					Count: 1,
				},
				{
					Class: "multiple_events_same_name_2",
					Name:  "same_name",
					Count: 1,
				},
			},
			expectedEventCounts: map[key]int{
				{class: "multiple_events_same_name_1", name: "same_name"}: 1,
				{class: "multiple_events_same_name_2", name: "same_name"}: 1,
			},
			expectedStatusCode: http.StatusNoContent,
		},
		{
			name: "multiple_events_same_class_same_name",
			events: []api.StatsEvent{
				{
					Class: "multiple_events_same_class_same_name",
					Name:  "same_name",
					Count: 1,
				},
				{
					Class: "multiple_events_same_class_same_name",
					Name:  "same_name",
					Count: 1,
				},
			},
			expectedEventCounts: map[key]int{
				{class: "multiple_events_same_class_same_name", name: "same_name"}: 2,
			},
			expectedStatusCode: http.StatusNoContent,
		},
		{
			name: "empty_usage_class",
			events: []api.StatsEvent{
				{
					Class: "",
					Name:  "name",
					Count: 1,
				},
			},
			expectedEventCounts: map[key]int{
				{class: "", name: "name"}: 0,
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "empty_usage_name",
			events: []api.StatsEvent{
				{
					Class: "class_empty_usage_name",
					Name:  "",
					Count: 1,
				},
			},
			expectedEventCounts: map[key]int{
				{class: "class_empty_usage_name", name: ""}: 0,
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "zero_usage_count",
			events: []api.StatsEvent{
				{
					Class: "class_zero_usage_count",
					Name:  "name",
					Count: 0,
				},
			},
			expectedEventCounts: map[key]int{
				{class: "class_zero_usage_count", name: "name"}: 0,
			},
			expectedStatusCode: http.StatusNoContent,
		},
		{
			name: "negative_usage_count",
			events: []api.StatsEvent{
				{
					Class: "class_negative_usage_count",
					Name:  "name",
					Count: -23,
				},
			},
			expectedEventCounts: map[key]int{
				{class: "class_negative_usage_count", name: "name"}: 0,
			},
			expectedStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := clt.PostStatsEventsWithResponse(ctx, api.PostStatsEventsJSONRequestBody{
				Events: tt.events,
			})
			if err != nil {
				t.Fatalf("PostStatsEvents failed: %s", err)
			}

			if resp.StatusCode() != tt.expectedStatusCode {
				t.Fatalf("PostStatsEvents status code: %d, expected: %d", resp.StatusCode(), tt.expectedStatusCode)
			}

			for _, sentEv := range tt.events {
				collectedEventsToCount := map[key]int{}
				k := key{class: sentEv.Class, name: sentEv.Name}
				_, isMapContainKey := collectedEventsToCount[k]
				if isMapContainKey {
					continue
				}
				for _, collectedMetric := range deps.collector.Metrics {
					if collectedMetric.Event.Class == sentEv.Class && collectedMetric.Event.Name == sentEv.Name {
						collectedEventsToCount[k] += int(collectedMetric.Value)
					}
				}
				if collectedEventsToCount[k] != tt.expectedEventCounts[k] {
					t.Fatalf("PostStatsEvents events for class %s and name: %s, count: %d, expected: %d", sentEv.Class, sentEv.Name, collectedEventsToCount[k], tt.expectedEventCounts[k])
				}
			}
		})
	}
}

func TestController_CopyObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "bucket/prefix"), "main")
	require.NoError(t, err)
	_, err = deps.catalog.CreateBranch(ctx, repo, "alt", "main")
	require.NoError(t, err)

	uploadContent := func(t *testing.T, repository, branch, objPath string) api.ObjectStats {
		t.Helper()
		const content = "hello world this is my awesome content"
		uploadResp, err := uploadObjectHelper(t, ctx, clt, objPath, strings.NewReader(content), repository, branch)
		verifyResponseOK(t, uploadResp, err)
		require.NotNil(t, uploadResp.JSON201)
		require.Equal(t, len(content), int(api.Int64Value(uploadResp.JSON201.SizeBytes)))
		return *uploadResp.JSON201
	}

	const copyTypeHeaderName = "X-Lakefs-Copy-Type"

	t.Run("shallow", func(t *testing.T) {
		const (
			srcPath  = "foo/bar"
			destPath = "foo/bar-shallow-copy"
		)
		objStat := uploadContent(t, repo, "main", srcPath)
		copyResp, err := clt.CopyObjectWithResponse(ctx, repo, "main", &api.CopyObjectParams{
			DestPath: destPath,
		}, api.CopyObjectJSONRequestBody{
			SrcPath: srcPath,
		})
		verifyResponseOK(t, copyResp, err)

		copyType := copyResp.HTTPResponse.Header.Get(copyTypeHeaderName)
		require.Equal(t, copyType, "shallow")
		// Verify creation path, date and physical address are different
		copyStat := copyResp.JSON201
		require.NotNil(t, copyStat)
		require.Equal(t, objStat.PhysicalAddress, copyStat.PhysicalAddress)
		require.GreaterOrEqual(t, objStat.Mtime, copyStat.Mtime)
		require.Equal(t, destPath, copyStat.Path)

		// Verify all else is equal
		objStat.Mtime = copyStat.Mtime
		objStat.Path = copyStat.Path
		require.Nil(t, deep.Equal(objStat, *copyStat))

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, repo, "main", &api.StatObjectParams{Path: destPath})
		verifyResponseOK(t, statResp, err)
		require.Nil(t, deep.Equal(statResp.JSON200, copyStat))
	})

	t.Run("full_different_branch", func(t *testing.T) {
		const (
			srcPath  = "foo/bar2"
			destPath = "foo/bar-full-from-branch"
		)
		objStat := uploadContent(t, repo, "alt", srcPath)
		copyResp, err := clt.CopyObjectWithResponse(ctx, repo, "main", &api.CopyObjectParams{
			DestPath: destPath,
		}, api.CopyObjectJSONRequestBody{
			SrcPath: srcPath,
			SrcRef:  api.StringPtr("alt"),
		})
		verifyResponseOK(t, copyResp, err)

		copyType := copyResp.HTTPResponse.Header.Get(copyTypeHeaderName)
		require.Equal(t, copyType, "full")
		// Verify creation path, date and physical address are different
		copyStat := copyResp.JSON201
		require.NotNil(t, copyStat)
		require.NotEmpty(t, copyStat.PhysicalAddress)
		require.NotEqual(t, objStat.PhysicalAddress, copyStat.PhysicalAddress)
		require.GreaterOrEqual(t, objStat.Mtime, copyStat.Mtime)
		require.Equal(t, destPath, copyStat.Path)

		// Verify all else is equal
		objStat.Mtime = copyStat.Mtime
		objStat.Path = copyStat.Path
		objStat.PhysicalAddress = copyStat.PhysicalAddress
		require.Nil(t, deep.Equal(objStat, *copyStat))

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, repo, "main", &api.StatObjectParams{Path: destPath})
		verifyResponseOK(t, statResp, err)
		require.Nil(t, deep.Equal(statResp.JSON200, copyStat))
	})

	t.Run("full_committed", func(t *testing.T) {
		const (
			srcPath  = "foo/bar3"
			destPath = "foo/bar-full-committed"
		)
		objStat := uploadContent(t, repo, "main", srcPath)
		commitResp, err := clt.CommitWithResponse(ctx, repo, "main", &api.CommitParams{}, api.CommitJSONRequestBody{
			Message: "commit bar3",
		})
		verifyResponseOK(t, commitResp, err)
		require.NotNil(t, commitResp.JSON201)

		copyResp, err := clt.CopyObjectWithResponse(ctx, repo, "main", &api.CopyObjectParams{
			DestPath: destPath,
		}, api.CopyObjectJSONRequestBody{
			SrcPath: srcPath,
			SrcRef:  api.StringPtr("main"),
		})
		verifyResponseOK(t, copyResp, err)

		copyType := copyResp.HTTPResponse.Header.Get(copyTypeHeaderName)
		require.Equal(t, copyType, "full")
		// Verify creation path, date and physical address are different
		copyStat := copyResp.JSON201
		require.NotNil(t, copyStat)
		require.NotEmpty(t, copyStat.PhysicalAddress)
		require.NotEqual(t, objStat.PhysicalAddress, copyStat.PhysicalAddress)
		require.GreaterOrEqual(t, objStat.Mtime, copyStat.Mtime)
		require.Equal(t, destPath, copyStat.Path)

		// Verify all else is equal
		objStat.Mtime = copyStat.Mtime
		objStat.Path = copyStat.Path
		objStat.PhysicalAddress = copyStat.PhysicalAddress
		require.Nil(t, deep.Equal(objStat, *copyStat))

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, repo, "main", &api.StatObjectParams{Path: destPath})
		verifyResponseOK(t, statResp, err)
		require.Nil(t, deep.Equal(statResp.JSON200, copyStat))
	})

	t.Run("not_found", func(t *testing.T) {
		resp, err := clt.CopyObjectWithResponse(ctx, repo, "main", &api.CopyObjectParams{
			DestPath: "bar/foo",
		}, api.CopyObjectJSONRequestBody{
			SrcPath: "not/found",
			SrcRef:  api.StringPtr("main"),
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404)

		// without src ref
		resp, err = clt.CopyObjectWithResponse(ctx, repo, "main", &api.CopyObjectParams{
			DestPath: "bar/foo",
		}, api.CopyObjectJSONRequestBody{
			SrcPath: "not/found",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404)
	})

	t.Run("empty_destination", func(t *testing.T) {
		resp, err := clt.CopyObjectWithResponse(ctx, repo, "main", &api.CopyObjectParams{
			DestPath: "",
		}, api.CopyObjectJSONRequestBody{
			SrcPath: "foo/bar",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON400)
	})
}
