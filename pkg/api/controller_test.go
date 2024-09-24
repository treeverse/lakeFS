package api_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
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
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
	"golang.org/x/exp/slices"
)

const DefaultUserID = "example_user"

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
	if !apiutil.IsStatusCodeOK(statusCode) {
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
		resp, err := clt.ListRepositoriesWithResponse(ctx, &apigen.ListRepositoriesParams{})
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
		_, err := deps.catalog.CreateRepository(ctx, "foo1", onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)
		_, err = deps.catalog.CreateRepository(ctx, "foo2", onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)
		_, err = deps.catalog.CreateRepository(ctx, "foo3", onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)

		resp, err := clt.ListRepositoriesWithResponse(ctx, &apigen.ListRepositoriesParams{})
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
		resp, err := clt.ListRepositoriesWithResponse(ctx, &apigen.ListRepositoriesParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(2)),
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
		resp, err := clt.ListRepositoriesWithResponse(ctx, &apigen.ListRepositoriesParams{
			After:  apiutil.Ptr[apigen.PaginationAfter]("foo2"),
			Amount: apiutil.Ptr[apigen.PaginationAmount](2),
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
		_, err := deps.catalog.CreateRepository(context.Background(), "foo1", onBlock(deps, "foo1"), testBranchName, false)
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
		resp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
			Name:             name,
			StorageNamespace: onBlock(deps, name),
		})
		verifyResponseOK(t, resp, err)

		resp, err = clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
			Name:             name + "_2",
			StorageNamespace: onBlock(deps, name),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode())
	})
}

func testCommitEntries(t *testing.T, ctx context.Context, cat *catalog.Catalog, deps *dependencies, params commitEntriesParams) string {
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
	commit, err := cat.Commit(ctx, params.repo, params.branch, "commit"+params.commitName, params.user, nil, nil, nil, false)
	testutil.MustDo(t, "commit", err)
	return commit.Reference
}

func TestController_LogCommitsMissingBranch(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "ns1"), "main", false)
	testutil.Must(t, err)

	resp, err := clt.LogCommitsWithResponse(ctx, repo, "otherbranch", &apigen.LogCommitsParams{})
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
			repo := testUniqueRepoName()
			_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
			testutil.Must(t, err)

			const prefix = "foo/bar"
			for i := 0; i < tt.commits; i++ {
				n := strconv.Itoa(i + 1)
				p := prefix + n
				err := deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: p, PhysicalAddress: onBlock(deps, "bar"+n+"addr"), CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n})
				testutil.MustDo(t, "create entry "+p, err)
				_, err = deps.catalog.Commit(ctx, repo, "main", "commit"+n, "some_user", nil, nil, nil, false)
				testutil.MustDo(t, "commit "+p, err)
			}
			params := &apigen.LogCommitsParams{}
			if tt.objects != nil {
				params.Objects = &tt.objects
			}
			if tt.prefixes != nil {
				params.Prefixes = &tt.prefixes
			}
			if tt.limit {
				params.Limit = &tt.limit
				params.Amount = apiutil.Ptr(apigen.PaginationAmount(1))
			}
			resp, err := clt.LogCommitsWithResponse(ctx, repo, "main", params)
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

	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	testutil.Must(t, err)

	commits := 100
	const prefix = "foo/bar"
	commitsToLook := map[string]*catalog.CommitLog{}
	for i := 0; i < commits; i++ {
		n := strconv.Itoa(i + 1)
		p := prefix + n
		err := deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: p, PhysicalAddress: onBlock(deps, "bar"+n+"addr"), CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n})
		testutil.MustDo(t, "create entry "+p, err)
		log, err := deps.catalog.Commit(ctx, repo, "main", t.Name()+" commit"+n, "some_user", nil, nil, nil, false)
		testutil.MustDo(t, "commit "+p, err)
		if i%4 == 0 {
			commitsToLook[p] = log
		}
	}

	var g multierror.Group
	for objPath, logRef := range commitsToLook {
		objects := []string{objPath}
		params := &apigen.LogCommitsParams{Objects: &objects}
		log := logRef
		g.Go(func() error {
			resp, err := clt.LogCommitsWithResponse(ctx, repo, "main", params)
			verifyResponseOK(t, resp, err)

			commitsLog := resp.JSON200.Results
			if len(commitsLog) != 1 {
				t.Fatalf("Log %d commits(%v), expected %d", len(commitsLog), commitsLog, 1)
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
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	testutil.Must(t, err)
	const prefix = "foo/bar"
	const totalCommits = 10
	commits := make([]*catalog.CommitLog, totalCommits)
	for i := 0; i < totalCommits; i++ {
		n := strconv.Itoa(i + 1)
		p := prefix + n
		err := deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: p, PhysicalAddress: onBlock(deps, "bar"+n+"addr"), CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n})
		testutil.MustDo(t, "create entry "+p, err)
		commit, err := deps.catalog.Commit(ctx, repo, "main", "commit"+n, "some_user", nil, nil, nil, false)
		testutil.MustDo(t, "commit "+p, err)
		commits[i] = commit
	}

	tests := []struct {
		name            string
		amount          int
		limit           bool
		objects         []string
		prefixes        []string
		expectedCommits []string
		expectedMore    bool
		stopAt          string
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
		{
			name:            "stop_at",
			expectedCommits: []string{"commit10", "commit9"},
			expectedMore:    false,
			stopAt:          commits[8].Reference,
		},
		{
			name:            "stop_at_short_sha",
			expectedCommits: []string{"commit10", "commit9"},
			expectedMore:    false,
			stopAt:          commits[8].Reference[:7],
		},
		{
			name:            "stop_at_branch_ref",
			expectedCommits: []string{"commit10"},
			expectedMore:    false,
			stopAt:          "main",
		},
		{
			name:            "stop_at_branch_ref_expression",
			expectedCommits: []string{"commit10", "commit9", "commit8"},
			expectedMore:    false,
			stopAt:          "main~2",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			params := &apigen.LogCommitsParams{}
			if tt.objects != nil {
				params.Objects = &tt.objects
			}
			if tt.prefixes != nil {
				params.Prefixes = &tt.prefixes
			}
			if tt.limit {
				params.Limit = &tt.limit
				params.Amount = apiutil.Ptr(apigen.PaginationAmount(1))
			}
			if tt.amount > 0 {
				params.Amount = apiutil.Ptr(apigen.PaginationAmount(tt.amount))
			}
			if tt.stopAt != "" {
				params.StopAt = &tt.stopAt
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

	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "ns1"), "main", false)
	testutil.Must(t, err)

	commitsMap := make(map[string]string)
	commitsMap["commitA"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         repo,
		branch:       "main",
		filesVersion: 1,
		paths:        []string{"data/a/a.txt", "data/a/b.txt", "data/a/c.txt", "data/b/b.txt"},
		user:         "user1",
		commitName:   "A",
	})
	commitsMap["commitB"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         repo,
		branch:       "main",
		filesVersion: 1,
		paths:        []string{"data/a/foo.txt", "data/b/bar.txt"},
		user:         "user1",
		commitName:   "B",
	})
	commitsMap["commitC"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         repo,
		branch:       "main",
		filesVersion: 2,
		paths:        []string{"data/a/a.txt"},
		user:         "user1",
		commitName:   "C",
	})
	_, err = deps.catalog.CreateBranch(ctx, repo, "branch-a", "main")
	testutil.Must(t, err)
	commitsMap["commitL"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         repo,
		branch:       "branch-a",
		filesVersion: 2,
		paths:        []string{"data/a/foo.txt", "data/b/bar.txt"},
		user:         "user2",
		commitName:   "L",
	})
	commitsMap["commitD"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         repo,
		branch:       "main",
		filesVersion: 2,
		paths:        []string{"data/b/b.txt"},
		user:         "user1",
		commitName:   "D",
	})
	_, err = deps.catalog.CreateBranch(ctx, repo, "branch-b", "main")
	testutil.Must(t, err)
	commitsMap["commitP"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         repo,
		branch:       "branch-b",
		filesVersion: 1,
		paths:        []string{"data/c/banana.txt"},
		user:         "user3",
		commitName:   "P",
	})
	mergeCommit, err := deps.catalog.Merge(ctx, repo, "main", "branch-b", "user3", "commitR", catalog.Metadata{}, "")
	testutil.Must(t, err)
	commitsMap["commitR"] = mergeCommit
	commitsMap["commitM"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         repo,
		branch:       "branch-a",
		filesVersion: 1,
		paths:        []string{"data/a/d.txt"},
		user:         "user2",
		commitName:   "M",
	})
	mergeCommit, err = deps.catalog.Merge(ctx, repo, "main", "branch-a", "user2", "commitN", catalog.Metadata{}, "")
	testutil.Must(t, err)
	commitsMap["commitN"] = mergeCommit
	commitsMap["commitX"] = testCommitEntries(t, ctx, deps.catalog, deps, commitEntriesParams{
		repo:         repo,
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
			resp, err := clt.LogCommitsWithResponse(ctx, repo, "main", &apigen.LogCommitsParams{Objects: c.objectList, Prefixes: c.prefixList})
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
		_, err := deps.catalog.CreateRepository(ctx, "foo1", onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)
		testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, "foo1", "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
		commit1, err := deps.catalog.Commit(ctx, "foo1", "main", "some message", DefaultUserID, nil, nil, nil, false)
		testutil.Must(t, err)
		reference1, err := deps.catalog.GetBranchReference(ctx, "foo1", "main")
		if err != nil {
			t.Fatal(err)
		}
		if reference1 != commit1.Reference {
			t.Fatalf("Commit reference %v, not equals to branch reference %s", commit1, reference1)
		}
		resp, err := clt.GetCommitWithResponse(ctx, "foo1", commit1.Reference)
		verifyResponseOK(t, resp, err)

		committer := resp.JSON200.Committer
		if committer != DefaultUserID {
			t.Fatalf("unexpected commit id %s, expected %s", committer, DefaultUserID)
		}
	})

	t.Run("branch commit", func(t *testing.T) {
		ctx := context.Background()
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.Must(t, err)
		testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
		commit1, err := deps.catalog.Commit(ctx, repo, "main", "some message", DefaultUserID, nil, nil, nil, false)
		testutil.Must(t, err)
		reference1, err := deps.catalog.GetBranchReference(ctx, repo, "main")
		if err != nil {
			t.Fatal(err)
		}
		if reference1 != commit1.Reference {
			t.Fatalf("Commit reference %v, not equals to branch reference %s", commit1, reference1)
		}
		resp, err := clt.GetCommitWithResponse(ctx, repo, "main")
		verifyResponseOK(t, resp, err)
		if resp.JSON200 == nil {
			t.Fatal("GetCommit expected to return 200 with response")
		}
		if resp.JSON200.Id != commit1.Reference {
			t.Fatalf("GetCommit ID=%s, expected=%s", resp.JSON200.Id, commit1.Reference)
		}
		if resp.JSON200.Committer != DefaultUserID {
			t.Fatalf("unexpected commit id %s, expected %s", resp.JSON200.Committer, DefaultUserID)
		}
	})

	t.Run("tag commit", func(t *testing.T) {
		ctx := context.Background()
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.Must(t, err)
		testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
		commit1, err := deps.catalog.Commit(ctx, repo, "main", "some message", DefaultUserID, nil, nil, nil, false)
		testutil.Must(t, err)
		_, err = deps.catalog.CreateTag(ctx, repo, "tag1", commit1.Reference)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := clt.GetCommitWithResponse(ctx, repo, "tag1")
		verifyResponseOK(t, resp, err)
		if resp.JSON200 == nil {
			t.Fatal("GetCommit expected to return 200 with response")
		}
		if resp.JSON200.Id != commit1.Reference {
			t.Fatalf("GetCommit ID=%s, expected=%s", resp.JSON200.Id, commit1.Reference)
		}
		if resp.JSON200.Committer != DefaultUserID {
			t.Fatalf("unexpected commit id %s, expected %s", resp.JSON200.Committer, DefaultUserID)
		}
	})

	t.Run("initial commit", func(t *testing.T) {
		// validate a new repository's initial commit existence and structure
		ctx := context.Background()
		_, err := deps.catalog.CreateRepository(ctx, "foo2", onBlock(deps, "foo2"), "main", false)
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
		metadata := apigen.Commit_Metadata{}
		expectedCommit := &apigen.Commit{
			Committer:    "",
			CreationDate: commit.CreationDate,
			Id:           commit.Id,
			Message:      graveler.FirstCommitMsg,
			MetaRangeId:  "",
			Metadata:     &metadata,
			Parents:      []string{},
			Generation:   swag.Int64(1),
			Version:      swag.Int(1),
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
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
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
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.MustDo(t, fmt.Sprintf("create repo %s", repo), err)
		testutil.MustDo(t, fmt.Sprintf("commit bar on %s", repo), deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message: "some message",
		})
		verifyResponseOK(t, resp, err)
	})

	t.Run("commit success with source metarange", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.MustDo(t, fmt.Sprintf("create repo %s", repo), err)

		_, err = deps.catalog.CreateBranch(ctx, repo, "foo-branch", "main")
		testutil.Must(t, err)
		testutil.MustDo(t, fmt.Sprintf("commit bar on %s", repo), deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message: "some message",
		})
		verifyResponseOK(t, resp, err)

		resp, err = clt.CommitWithResponse(ctx, repo, "foo-branch", &apigen.CommitParams{SourceMetarange: &resp.JSON201.MetaRangeId}, apigen.CommitJSONRequestBody{
			Message: "some message",
		})
		verifyResponseOK(t, resp, err)
	})

	t.Run("commit failure with source metarange and dirty branch", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.MustDo(t, fmt.Sprintf("create repo %s", repo), err)

		_, err = deps.catalog.CreateBranch(ctx, repo, "foo-branch", "main")
		testutil.Must(t, err)

		testutil.MustDo(t, fmt.Sprintf("commit bar on %s", repo), deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message: "some message",
		})
		verifyResponseOK(t, resp, err)

		testutil.MustDo(t, fmt.Sprintf("commit bar on %s", repo), deps.catalog.CreateEntry(ctx, repo, "foo-branch", catalog.DBEntry{Path: "foo/bar/2", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		resp, err = clt.CommitWithResponse(ctx, repo, "foo-branch", &apigen.CommitParams{SourceMetarange: &resp.JSON201.MetaRangeId}, apigen.CommitJSONRequestBody{
			Message: "some message",
		})

		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, graveler.ErrCommitMetaRangeDirtyBranch.Error())
	})

	t.Run("commit failure empty branch", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.MustDo(t, fmt.Sprintf("create repo %s", repo), err)

		_, err = deps.catalog.CreateBranch(ctx, repo, "foo-branch", "main")
		testutil.Must(t, err)

		resp, err := clt.CommitWithResponse(ctx, repo, "foo-branch", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message: "some message",
		})

		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, graveler.ErrNoChanges.Error())
	})

	t.Run("commit success - with creation date", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.MustDo(t, fmt.Sprintf("create repo %s", repo), err)
		testutil.MustDo(t, fmt.Sprintf("commit bar on %s", repo), deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}))
		date := int64(1642626109)
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message: "some message",
			Date:    &date,
		})
		verifyResponseOK(t, resp, err)
		if resp.JSON201.CreationDate != date {
			t.Errorf("creation date expected %d, got: %d", date, resp.JSON201.CreationDate)
		}
	})

	t.Run("protected branch", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.MustDo(t, "create repository", err)
		rules := map[string]*graveler.BranchProtectionBlockedActions{
			"main": {
				Value: []graveler.BranchProtectionBlockedAction{
					graveler.BranchProtectionBlockedAction_COMMIT,
				},
			},
		}
		err = deps.catalog.SetBranchProtectionRules(ctx, repo, &graveler.BranchProtectionRules{
			BranchPatternToBlockedActions: rules,
		}, swag.String(""))
		testutil.MustDo(t, "protection rule", err)
		err = deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil})
		testutil.MustDo(t, "commit to protected branch", err)
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message: "committed to protected branch",
		})
		testutil.Must(t, err)
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("Commit to protected branch should be forbidden (403), got %s", resp.Status())
		}
	})
	t.Run("read only repo", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", true)
		testutil.MustDo(t, "create repository", err)
		err = deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil})
		require.Error(t, err, "read-only repository")
		err = deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar", PhysicalAddress: "pa", CreationDate: time.Now(), Size: 666, Checksum: "cs", Metadata: nil}, graveler.WithForce(true))
		testutil.MustDo(t, "create entry", err)
		resp, err := clt.CommitWithResponse(ctx, repo, "main", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message: "committed to read-only repository",
		})
		testutil.Must(t, err)
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("Commit to read-only repository should be forbidden (403), got %s", resp.Status())
		}
		resp, err = clt.CommitWithResponse(ctx, repo, "main", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message: "committed to read-only repository",
			Force:   swag.Bool(true),
		})
		verifyResponseOK(t, resp, err)
	})
}

func TestController_CreateRepositoryHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("create repo success", func(t *testing.T) {
		repoName := testUniqueRepoName()
		resp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
			DefaultBranch:    apiutil.Ptr("main"),
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
		repoName := testUniqueRepoName()
		bareRepo := true
		resp, err := clt.CreateRepositoryWithResponse(ctx,
			&apigen.CreateRepositoryParams{
				Bare: &bareRepo,
			}, apigen.CreateRepositoryJSONRequestBody{
				DefaultBranch:    apiutil.Ptr("main"),
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
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", false)
		if err != nil {
			t.Fatal(err)
		}
		const times = 3 // try to create the same repo multiple times
		for i := 0; i < times; i++ {
			resp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
				DefaultBranch:    apiutil.Ptr("main"),
				Name:             repo,
				StorageNamespace: onBlock(deps, "foo-bucket-2"),
			})
			if err != nil {
				t.Fatal(err)
			}
			if resp == nil {
				t.Fatal("CreateRepository missing response")
			}
			if resp.JSON409 == nil {
				t.Fatal("expected status code 409 creating duplicate repo, got ", resp.StatusCode())
			}
		}
	})

	t.Run("create read-only repo skip ensure storage namespace", func(t *testing.T) {
		repoName := testUniqueRepoName()
		path := "bucket-1/namespace-1"
		resp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
			DefaultBranch:    apiutil.Ptr("main"),
			Name:             repoName,
			StorageNamespace: onBlock(deps, path),
		})
		verifyResponseOK(t, resp, err)

		response := resp.JSON201
		if response == nil {
			t.Fatal("CreateRepository got bad response")
		}
		if response.Id != repoName {
			t.Fatalf("CreateRepository id=%s, expected=%s", response.Id, repoName)
		}

		// delete the repo but keeps the dummy file
		err = deps.catalog.DeleteRepository(ctx, repoName)
		if err != nil {
			t.Fatal(err)
		}
		// try to create the same repo once as a "normal" repo and once as a read-only repo

		resp2, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
			DefaultBranch:    apiutil.Ptr("main"),
			Name:             repoName,
			StorageNamespace: onBlock(deps, path),
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp2 == nil {
			t.Fatal("CreateRepository missing response")
		}
		if resp2.JSON400 == nil {
			t.Fatal("expected status code 400 creating duplicate repo, got ", resp.StatusCode())
		}

		resp3, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
			DefaultBranch:    apiutil.Ptr("main"),
			Name:             repoName,
			StorageNamespace: onBlock(deps, path),
			ReadOnly:         apiutil.Ptr(true),
		})
		verifyResponseOK(t, resp3, err)
		response = resp3.JSON201
		if response == nil {
			t.Fatal("CreateRepository got bad response")
		}
		if response.Id != repoName {
			t.Fatalf("CreateRepository id=%s, expected=%s", response.Id, repoName)
		}
	})

	t.Run("create repo with conflicting storage type", func(t *testing.T) {
		repo := testUniqueRepoName()
		resp, _ := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
			DefaultBranch:    apiutil.Ptr("main"),
			Name:             repo,
			StorageNamespace: "s3://foo-bucket",
		})
		if resp == nil {
			t.Fatal("CreateRepository missing response")
		}
		if resp.JSON400 == nil {
			t.Fatal("expected status code 400 for invalid namespace, got", resp.StatusCode())
		}
	})
}

func TestController_DeleteRepositoryHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("delete repo success", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)

		resp, err := clt.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{})
		verifyResponseOK(t, resp, err)

		// delete again to expect repository not found
		resp, err = clt.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{})
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatalf("expected repository to be gone (404), instead got status: %s", resp.Status())
		}
	})

	t.Run("delete repo doesnt exist", func(t *testing.T) {
		repo := testUniqueRepoName()
		resp, err := clt.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{})
		testutil.Must(t, err)
		if resp.StatusCode() == http.StatusOK {
			t.Fatalf("DeleteRepository should fail on non existing repository, got %d", resp.StatusCode())
		}
	})

	t.Run("delete repo doesnt delete other repos", func(t *testing.T) {
		names := []string{"rr0", "rr1", "rr11", "rr2"}
		for _, name := range names {
			_, err := deps.catalog.CreateRepository(ctx, name, onBlock(deps, "foo1"), "main", false)
			testutil.Must(t, err)
		}

		// delete one repository and check that all rest are there
		resp, err := clt.DeleteRepositoryWithResponse(ctx, "rr1", &apigen.DeleteRepositoryParams{})
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

	t.Run("delete read-only repository", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", true)
		testutil.Must(t, err)

		resp, err := clt.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{})
		testutil.Must(t, err)
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("expected status code 403 for deleting a read-only repository, got %d instead", resp.StatusCode())
		}

		resp, err = clt.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{Force: swag.Bool(true)})
		verifyResponseOK(t, resp, err)

		// delete again to expect repository not found
		resp, err = clt.DeleteRepositoryWithResponse(ctx, repo, &apigen.DeleteRepositoryParams{})
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatalf("expected repository to be gone (404), instead got status: %s", resp.Status())
		}
	})
}

func TestController_SetRepositoryMetadataHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("set, append and get", func(t *testing.T) {
		tt := []struct {
			name        string
			properties  map[string]string
			appendProps map[string]string
			expected    map[string]string
		}{
			{
				name:        "empty",
				properties:  map[string]string{},
				appendProps: map[string]string{},
				expected:    nil,
			},
			{
				name:        "append empty",
				properties:  map[string]string{"foo": "bar"},
				appendProps: map[string]string{},
				expected:    map[string]string{"foo": "bar"},
			},
			{
				name:        "append new",
				properties:  map[string]string{"foo": "bar"},
				appendProps: map[string]string{"foo1": "bar1"},
				expected:    map[string]string{"foo": "bar", "foo1": "bar1"},
			},
			{
				name:        "append multiple",
				properties:  map[string]string{"foo": "bar"},
				appendProps: map[string]string{"foo1": "bar1", "foo2": "bar2", "foo3": "bar3", "foo4": "bar4"},
				expected:    map[string]string{"foo": "bar", "foo1": "bar1", "foo2": "bar2", "foo3": "bar3", "foo4": "bar4"},
			},
			{
				name:        "append override",
				properties:  map[string]string{"foo": "bar"},
				appendProps: map[string]string{"foo": "bar1"},
				expected:    map[string]string{"foo": "bar1"},
			},
			{
				name:        "append override empty",
				properties:  map[string]string{"foo": "bar"},
				appendProps: map[string]string{"foo": ""},
				expected:    map[string]string{"foo": ""},
			},
		}
		for _, tt1 := range tt {
			t.Run(tt1.name, func(t *testing.T) {
				repoName := testUniqueRepoName()
				createResp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
					DefaultBranch:    apiutil.Ptr("main"),
					Name:             repoName,
					StorageNamespace: onBlock(deps, repoName),
				})
				verifyResponseOK(t, createResp, err)

				resp, err := clt.SetRepositoryMetadataWithResponse(ctx, repoName, apigen.SetRepositoryMetadataJSONRequestBody{Metadata: apigen.RepositoryMetadataSet_Metadata{AdditionalProperties: tt1.properties}})
				verifyResponseOK(t, resp, err)

				resp, err = clt.SetRepositoryMetadataWithResponse(ctx, repoName, apigen.SetRepositoryMetadataJSONRequestBody{Metadata: apigen.RepositoryMetadataSet_Metadata{AdditionalProperties: tt1.appendProps}})
				verifyResponseOK(t, resp, err)

				getResp, err := clt.GetRepositoryMetadataWithResponse(ctx, repoName)
				verifyResponseOK(t, resp, err)
				if diff := deep.Equal(getResp.JSON200.AdditionalProperties, tt1.expected); diff != nil {
					t.Fatal("Get repository metadata results diff:", diff)
				}
			})
		}
	})

	t.Run("set metadata bare repo", func(t *testing.T) {
		repoName := testUniqueRepoName()
		bareRepo := true
		createResp, err := clt.CreateRepositoryWithResponse(ctx,
			&apigen.CreateRepositoryParams{
				Bare: &bareRepo,
			}, apigen.CreateRepositoryJSONRequestBody{
				DefaultBranch:    apiutil.Ptr("main"),
				Name:             repoName,
				StorageNamespace: onBlock(deps, "foo-bucket-2"),
			})
		verifyResponseOK(t, createResp, err)

		resp, err := clt.SetRepositoryMetadataWithResponse(ctx, repoName, apigen.SetRepositoryMetadataJSONRequestBody{Metadata: apigen.RepositoryMetadataSet_Metadata{AdditionalProperties: map[string]string{"foo": "bar"}}})
		verifyResponseOK(t, resp, err)
	})

	t.Run("set repository metadata not exist", func(t *testing.T) {
		repoName := testUniqueRepoName()
		resp, err := clt.SetRepositoryMetadataWithResponse(ctx, repoName, apigen.SetRepositoryMetadataJSONRequestBody{Metadata: apigen.RepositoryMetadataSet_Metadata{AdditionalProperties: map[string]string{"foo": "bar"}}})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404)
	})
}

func TestController_DeleteRepositoryMetadataHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("set, delete and get", func(t *testing.T) {
		tests := []struct {
			name        string
			properties  map[string]string
			deleteProps []string
			expected    map[string]string
		}{
			{
				name:        "empty",
				properties:  map[string]string{},
				deleteProps: []string{},
				expected:    nil,
			},
			{
				name:        "delete nothing",
				properties:  map[string]string{"foo": "bar"},
				deleteProps: []string{},
				expected:    map[string]string{"foo": "bar"},
			},
			{
				name:        "delete one",
				properties:  map[string]string{"foo": "bar", "foo1": "bar1"},
				deleteProps: []string{"foo1"},
				expected:    map[string]string{"foo": "bar"},
			},
			{
				name:        "delete non existing",
				properties:  map[string]string{"foo": "bar"},
				deleteProps: []string{"non-existing"},
				expected:    map[string]string{"foo": "bar"},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				repoName := testUniqueRepoName()
				createResp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
					DefaultBranch:    apiutil.Ptr("main"),
					Name:             repoName,
					StorageNamespace: onBlock(deps, repoName),
				})
				verifyResponseOK(t, createResp, err)

				resp, err := clt.SetRepositoryMetadataWithResponse(ctx, repoName, apigen.SetRepositoryMetadataJSONRequestBody{Metadata: apigen.RepositoryMetadataSet_Metadata{AdditionalProperties: tt.properties}})
				verifyResponseOK(t, resp, err)

				deleteResp, err := clt.DeleteRepositoryMetadataWithResponse(ctx, repoName, apigen.DeleteRepositoryMetadataJSONRequestBody{Keys: tt.deleteProps})
				verifyResponseOK(t, deleteResp, err)

				getResp, err := clt.GetRepositoryMetadataWithResponse(ctx, repoName)
				verifyResponseOK(t, getResp, err)
				if diff := deep.Equal(getResp.JSON200.AdditionalProperties, tt.expected); diff != nil {
					t.Fatal("Get repository metadata results diff:", diff)
				}
			})
		}
	})

	t.Run("delete metadata bare repo", func(t *testing.T) {
		repoName := testUniqueRepoName()
		bareRepo := true
		createResp, err := clt.CreateRepositoryWithResponse(ctx,
			&apigen.CreateRepositoryParams{
				Bare: &bareRepo,
			}, apigen.CreateRepositoryJSONRequestBody{
				DefaultBranch:    apiutil.Ptr("main"),
				Name:             repoName,
				StorageNamespace: onBlock(deps, "foo-bucket-2"),
			})
		verifyResponseOK(t, createResp, err)

		resp, err := clt.SetRepositoryMetadataWithResponse(ctx, repoName, apigen.SetRepositoryMetadataJSONRequestBody{Metadata: apigen.RepositoryMetadataSet_Metadata{AdditionalProperties: map[string]string{"foo": "bar"}}})
		verifyResponseOK(t, resp, err)
	})

	t.Run("delete repository metadata, repository not exist", func(t *testing.T) {
		repoName := testUniqueRepoName()
		resp, err := clt.SetRepositoryMetadataWithResponse(ctx, repoName, apigen.SetRepositoryMetadataJSONRequestBody{Metadata: apigen.RepositoryMetadataSet_Metadata{AdditionalProperties: map[string]string{"foo": "bar"}}})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404)
	})
}

func TestController_GetRepositoryMetadataHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("get repo metadata empty", func(t *testing.T) {
		repoName := testUniqueRepoName()
		createResp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
			DefaultBranch:    apiutil.Ptr("main"),
			Name:             repoName,
			StorageNamespace: onBlock(deps, "foo-bucket-1"),
		})
		verifyResponseOK(t, createResp, err)

		resp, err := clt.GetRepositoryMetadataWithResponse(ctx, repoName)
		verifyResponseOK(t, resp, err)
		require.NotNil(t, resp.JSON200)
		require.Nil(t, resp.JSON200.AdditionalProperties)
	})

	t.Run("get metadata bare repo", func(t *testing.T) {
		repoName := testUniqueRepoName()
		bareRepo := true
		createResp, err := clt.CreateRepositoryWithResponse(ctx,
			&apigen.CreateRepositoryParams{
				Bare: &bareRepo,
			}, apigen.CreateRepositoryJSONRequestBody{
				DefaultBranch:    apiutil.Ptr("main"),
				Name:             repoName,
				StorageNamespace: onBlock(deps, "foo-bucket-2"),
			})
		verifyResponseOK(t, createResp, err)

		resp, err := clt.GetRepositoryMetadataWithResponse(ctx, repoName)
		verifyResponseOK(t, resp, err)
		require.NotNil(t, resp.JSON200)
		require.Nil(t, resp.JSON200.AdditionalProperties)
	})

	t.Run("get repository metadata not exist", func(t *testing.T) {
		repoName := testUniqueRepoName()
		resp, err := clt.GetRepositoryMetadataWithResponse(ctx, repoName)
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404)
	})
}

func TestController_ListBranchesHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("list branches only default", func(t *testing.T) {
		ctx := context.Background()
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)
		resp, err := clt.ListBranchesWithResponse(ctx, repo, &apigen.ListBranchesParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(-1)),
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
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo2"), "main", false)
		testutil.Must(t, err)

		// create the first "dummy" commit on main so that we can create branches from it
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, repo, "main", "first commit", "test", nil, nil, nil, false)
		testutil.Must(t, err)

		for i := 0; i < 7; i++ {
			branchName := "main" + strconv.Itoa(i+1)
			_, err := deps.catalog.CreateBranch(ctx, repo, branchName, "main")
			testutil.MustDo(t, "create branch "+branchName, err)
		}
		resp, err := clt.ListBranchesWithResponse(ctx, repo, &apigen.ListBranchesParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(2)),
		})
		verifyResponseOK(t, resp, err)
		if len(resp.JSON200.Results) != 2 {
			t.Fatalf("expected 2 branches to return, got %d", len(resp.JSON200.Results))
		}

		resp, err = clt.ListBranchesWithResponse(ctx, repo, &apigen.ListBranchesParams{
			After:  apiutil.Ptr[apigen.PaginationAfter]("main1"),
			Amount: apiutil.Ptr[apigen.PaginationAmount](2),
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
		resp, err := clt.ListBranchesWithResponse(ctx, "repo666", &apigen.ListBranchesParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(2)),
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
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", false)
	testutil.Must(t, err)
	testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "obj1"}))
	commitLog, err := deps.catalog.Commit(ctx, repo, "main", "first commit", "test", nil, nil, nil, false)
	testutil.Must(t, err)
	const createTagLen = 7
	var createdTags []apigen.Ref
	for i := 0; i < createTagLen; i++ {
		tagID := "tag" + strconv.Itoa(i)
		commitID := commitLog.Reference
		_, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  tagID,
			Ref: commitID,
		})
		testutil.Must(t, err)
		createdTags = append(createdTags, apigen.Ref{
			Id:       tagID,
			CommitId: commitID,
		})
	}

	t.Run("default", func(t *testing.T) {
		resp, err := clt.ListTagsWithResponse(ctx, repo, &apigen.ListTagsParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(-1)),
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
		var results []apigen.Ref
		var after string
		var calls int
		for {
			calls++
			resp, err := clt.ListTagsWithResponse(ctx, repo, &apigen.ListTagsParams{
				After:  apiutil.Ptr(apigen.PaginationAfter(after)),
				Amount: apiutil.Ptr(apigen.PaginationAmount(pageSize)),
			})
			testutil.Must(t, err)
			payload := resp.JSON200
			if payload == nil {
				t.Fatal("ListTags missing response, got", resp.Status())
			}
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
		resp, err := clt.ListTagsWithResponse(ctx, "repo666", &apigen.ListTagsParams{})
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatal("ListTags should return not found error")
		}
	})
}

func TestController_GetBranchHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	const testBranch = "main"
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), testBranch, false)
	testutil.Must(t, err)

	t.Run("get default branch", func(t *testing.T) {
		// create the first "dummy" commit on main so that we can create branches from it
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, testBranch, catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, repo, testBranch, "first commit", "test", nil, nil, nil, false)
		testutil.Must(t, err)

		resp, err := clt.GetBranchWithResponse(ctx, repo, testBranch)
		verifyResponseOK(t, resp, err)
		reference := resp.JSON200
		if reference == nil || reference.CommitId == "" {
			t.Fatalf("Got no reference for branch '%s'", testBranch)
		}
	})

	t.Run("get missing branch", func(t *testing.T) {
		resp, err := clt.GetBranchWithResponse(ctx, repo, "main333")
		if err != nil {
			t.Fatal("GetBranch error", err)
		}
		if resp.JSON404 == nil {
			t.Fatal("GetBranch expected not found error")
		}
	})

	t.Run("get branch for missing repo", func(t *testing.T) {
		missingRepo := testUniqueRepoName()
		resp, err := clt.GetBranchWithResponse(ctx, missingRepo, "main")
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
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), testBranch, false)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("diff branch no changes", func(t *testing.T) {
		// create an entry and remove it
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, testBranch, catalog.DBEntry{Path: "a/b/c"}))
		testutil.Must(t, deps.catalog.DeleteEntry(ctx, repo, testBranch, "a/b/c"))

		resp, err := clt.DiffBranchWithResponse(ctx, repo, testBranch, &apigen.DiffBranchParams{})
		verifyResponseOK(t, resp, err)
		changes := len(resp.JSON200.Results)
		if changes != 0 {
			t.Fatalf("expected no diff results, got %d", changes)
		}
	})

	t.Run("diff branch with writes", func(t *testing.T) {
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, testBranch, catalog.DBEntry{Path: "a/b"}))
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, testBranch, catalog.DBEntry{Path: "a/b/c"}))
		testutil.Must(t, deps.catalog.DeleteEntry(ctx, repo, testBranch, "a/b/c"))
		resp, err := clt.DiffBranchWithResponse(ctx, repo, testBranch, &apigen.DiffBranchParams{})
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
		resp, err := clt.DiffBranchWithResponse(ctx, repo, "some-other-missing-branch", &apigen.DiffBranchParams{})
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
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, repo, "main", "first commit", "test", nil, nil, nil, false)
		testutil.Must(t, err)

		const newBranchName = "main2"
		resp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
			Name:   newBranchName,
			Source: "main",
		})
		verifyResponseOK(t, resp, err)
		reference := string(resp.Body)
		if len(reference) == 0 {
			t.Fatalf("branch %s creation got no reference", newBranchName)
		}
		const objPath = "some/path"
		const content = "hello world!"

		uploadResp, err := uploadObjectHelper(t, ctx, clt, objPath, strings.NewReader(content), repo, newBranchName)
		verifyResponseOK(t, uploadResp, err)

		if _, err := deps.catalog.Commit(ctx, repo, "main2", "commit 1", "some_user", nil, nil, nil, false); err != nil {
			t.Fatalf("failed to commit 'repo1': %s", err)
		}
		resp2, err := clt.DiffRefsWithResponse(ctx, repo, "main", newBranchName, &apigen.DiffRefsParams{})
		verifyResponseOK(t, resp2, err)
		results := resp2.JSON200.Results
		if len(results) != 1 {
			t.Fatalf("unexpected length of results: %d", len(results))
		}
		if results[0].Path != objPath {
			t.Fatalf("wrong result: %s", results[0].Path)
		}
	})

	t.Run("create branch missing commit", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)
		resp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
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
		repo := testUniqueRepoName()
		resp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
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
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)

		resp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
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
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)

		name := "tag123"
		_, err = deps.catalog.CreateTag(ctx, repo, name, "main")
		testutil.Must(t, err)

		resp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
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
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)

		log, err := deps.catalog.GetCommit(ctx, repo, "main")
		testutil.Must(t, err)

		resp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
			Name:   log.Reference,
			Source: "main",
		})
		if err != nil {
			t.Fatal("CreateBranch failed with error:", err)
		}
		if resp.JSON409 == nil {
			t.Fatal("CreateBranch expected conflict, got", resp.Status())
		}
	})

	t.Run("read-only repository", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", true)
		testutil.Must(t, err)
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "a/b"}, graveler.WithForce(true)))
		_, err = deps.catalog.Commit(ctx, repo, "main", "first commit", "test", nil, nil, nil, false, graveler.WithForce(true))
		testutil.Must(t, err)

		const newBranchName = "main2"
		resp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
			Name:   newBranchName,
			Source: "main",
		})
		testutil.Must(t, err)
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatal("CreateBranch expected 403 forbidden, got", resp.Status())
		}
		resp, err = clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
			Name:   newBranchName,
			Source: "main",
			Force:  swag.Bool(true),
		})
		verifyResponseOK(t, resp, err)
	})
}

func TestController_DiffRefsHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("diff prefix with and without delimiter", func(t *testing.T) {
		repoName := testUniqueRepoName()
		const newBranchName = "main2"
		_, err := deps.catalog.CreateRepository(ctx, repoName, onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)

		resp, err := clt.CreateBranchWithResponse(ctx, repoName, apigen.CreateBranchJSONRequestBody{
			Name:   newBranchName,
			Source: "main",
		})
		verifyResponseOK(t, resp, err)
		reference := string(resp.Body)
		if len(reference) == 0 {
			t.Fatalf("branch %s creation got no reference", newBranchName)
		}
		const prefix = "some/"
		const objPath = "path"
		const fullPath = prefix + objPath
		const content = "hello world!"

		uploadResp, err := uploadObjectHelper(t, ctx, clt, fullPath, strings.NewReader(content), repoName, newBranchName)
		verifyResponseOK(t, uploadResp, err)

		if _, err := deps.catalog.Commit(ctx, repoName, newBranchName, "commit 1", "some_user", nil, nil, nil, false); err != nil {
			t.Fatalf("failed to commit 'repo1': %s", err)
		}
		resp2, err := clt.DiffRefsWithResponse(ctx, repoName, "main", newBranchName, &apigen.DiffRefsParams{})
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

		delimiter := apigen.PaginationDelimiter("/")
		resp2, err = clt.DiffRefsWithResponse(ctx, repoName, "main", newBranchName, &apigen.DiffRefsParams{Delimiter: &delimiter})
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

func uploadObjectHelper(t testing.TB, ctx context.Context, clt apigen.ClientWithResponsesInterface, path string, reader io.Reader, repo, branch string) (*apigen.UploadObjectResponse, error) {
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

	return clt.UploadObjectWithBodyWithResponse(ctx, repo, branch, &apigen.UploadObjectParams{
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

func TestController_UploadObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	_, err := deps.catalog.CreateRepository(ctx, "my-new-repo", onBlock(deps, "foo1"), "main", false)
	testutil.Must(t, err)

	t.Run("upload object", func(t *testing.T) {
		// write
		contentType, buf := writeMultipart("content", "bar", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &apigen.UploadObjectParams{
			Path: "foo/bar",
		}, contentType, buf)

		testutil.Must(t, err)
		if b.StatusCode() == http.StatusInternalServerError {
			t.Fatalf("got 500 while uploading: %v", b.JSONDefault)
		}
		if b.StatusCode() != http.StatusCreated {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}
	})

	t.Run("overwrite", func(t *testing.T) {
		// write first
		contentType, buf := writeMultipart("content", "baz1", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &apigen.UploadObjectParams{
			Path: "foo/baz1",
		}, contentType, buf)
		testutil.Must(t, err)
		if b.StatusCode() != http.StatusCreated {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}
		// overwrite
		contentType, buf = writeMultipart("content", "baz1", "something else!")
		b, err = clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &apigen.UploadObjectParams{
			Path: "foo/baz1",
		}, contentType, buf)

		testutil.Must(t, err)
		if b.StatusCode() != http.StatusCreated {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}
	})

	t.Run("disable overwrite with if-none-match (uncommitted entry)", func(t *testing.T) {
		// write first
		contentType, buf := writeMultipart("content", "baz2", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &apigen.UploadObjectParams{
			Path: "foo/baz2",
		}, contentType, buf)
		testutil.Must(t, err)
		if b.StatusCode() != http.StatusCreated {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}
		// overwrite
		contentType, buf = writeMultipart("content", "baz2", "something else!")
		all := apigen.IfNoneMatch("*")
		b, err = clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &apigen.UploadObjectParams{
			Path:        "foo/baz2",
			IfNoneMatch: &all,
		}, contentType, buf)

		testutil.Must(t, err)
		if b.StatusCode() != http.StatusPreconditionFailed {
			t.Fatalf("expected 412 for UploadObject, got %d", b.StatusCode())
		}
	})

	t.Run("disable overwrite with if-none-match (committed entry)", func(t *testing.T) {
		_, err := deps.catalog.CreateBranch(ctx, "my-new-repo", "another-branch", "main")
		testutil.Must(t, err)

		// write first
		contentType, buf := writeMultipart("content", "baz3", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "another-branch", &apigen.UploadObjectParams{
			Path: "foo/baz3",
		}, contentType, buf)
		testutil.Must(t, err)
		if b.StatusCode() != http.StatusCreated {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}

		// commit
		_, err = deps.catalog.Commit(ctx, "my-new-repo", "another-branch", "a commit!", "user1", nil, nil, nil, false)
		testutil.Must(t, err)

		// overwrite after commit
		all := apigen.IfNoneMatch("*")
		contentType, buf = writeMultipart("content", "baz3", "something else!")
		b, err = clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "another-branch", &apigen.UploadObjectParams{
			Path:        "foo/baz3",
			IfNoneMatch: &all,
		}, contentType, buf)

		testutil.Must(t, err)
		if b.StatusCode() != http.StatusPreconditionFailed {
			t.Fatalf("expected 412 for UploadObject, got %d", b.StatusCode())
		}
	})

	t.Run("disable overwrite with if-none-match (no entry)", func(t *testing.T) {
		ifNoneMatch := apigen.IfNoneMatch("*")
		contentType, buf := writeMultipart("content", "baz4", "something else!")
		resp, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &apigen.UploadObjectParams{
			Path:        "foo/baz4",
			IfNoneMatch: &ifNoneMatch,
		}, contentType, buf)
		if err != nil {
			t.Fatalf("UploadObject err=%s, expected no error", err)
		}
		if resp.JSON201 == nil {
			t.Fatalf("UploadObject status code=%d, expected 201", resp.StatusCode())
		}
	})

	t.Run("invalid if non match value", func(t *testing.T) {
		ifNoneMatch := apigen.IfNoneMatch("invalid")
		contentType, buf := writeMultipart("content", "baz4", "something else!")
		resp, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &apigen.UploadObjectParams{
			Path:        "foo/baz4",
			IfNoneMatch: &ifNoneMatch,
		}, contentType, buf)
		if err != nil {
			t.Fatalf("UploadObject err=%s, expected no error", err)
		}
		if resp.JSON400 == nil {
			t.Fatalf("UploadObject status code=%d, expected 400", resp.StatusCode())
		}
	})

	t.Run("upload object missing 'content' key", func(t *testing.T) {
		// write
		contentType, buf := writeMultipart("this-is-not-content", "bar", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, "my-new-repo", "main", &apigen.UploadObjectParams{
			Path: "foo/bar",
		}, contentType, buf)

		testutil.Must(t, err)
		if b.StatusCode() != http.StatusInternalServerError {
			t.Fatalf("expected 500 for UploadObject, got %d", b.StatusCode())
		}
		if !strings.Contains(b.JSONDefault.Message, "missing key 'content'") {
			t.Fatalf("error message should state missing 'content' key")
		}
	})

	t.Run("read-only repository", func(t *testing.T) {
		repoName := "my-new-read-only-repo"
		_, err := deps.catalog.CreateRepository(ctx, repoName, onBlock(deps, "foo2"), "main", true)
		testutil.Must(t, err)
		// write
		contentType, buf := writeMultipart("content", "bar", "hello world!")
		b, err := clt.UploadObjectWithBodyWithResponse(ctx, repoName, "main", &apigen.UploadObjectParams{
			Path: "foo/bar",
		}, contentType, buf)
		testutil.Must(t, err)
		if b.JSON403 == nil {
			t.Fatalf("expected 403 forbidden for UploadObject, got %d", b.StatusCode())
		}
		contentType, buf = writeMultipart("content", "bar", "hello world!")
		b, err = clt.UploadObjectWithBodyWithResponse(ctx, repoName, "main", &apigen.UploadObjectParams{
			Path:  "foo/bar",
			Force: swag.Bool(true),
		}, contentType, buf)
		testutil.Must(t, err)
		if b.StatusCode() == http.StatusInternalServerError {
			t.Fatalf("got internal server error (500) while uploading: %v", b.JSONDefault)
		}
		if b.StatusCode() != http.StatusCreated {
			t.Fatalf("expected 201 for UploadObject, got %d", b.StatusCode())
		}
	})
}

func TestController_DeleteBranchHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("delete branch success", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "my-new-repo", onBlock(deps, "foo1"), "main", false)
		testutil.Must(t, err)
		testutil.Must(t, deps.catalog.CreateEntry(ctx, "my-new-repo", "main", catalog.DBEntry{Path: "a/b"}))
		_, err = deps.catalog.Commit(ctx, "my-new-repo", "main", "first commit", "test", nil, nil, nil, false)
		testutil.Must(t, err)

		_, err = deps.catalog.CreateBranch(ctx, "my-new-repo", "main2", "main")
		if err != nil {
			t.Fatal(err)
		}

		delResp, err := clt.DeleteBranchWithResponse(ctx, "my-new-repo", "main2", &apigen.DeleteBranchParams{})
		verifyResponseOK(t, delResp, err)

		_, err = deps.catalog.GetBranchReference(ctx, "my-new-repo", "main2")
		if !errors.Is(err, graveler.ErrNotFound) {
			t.Fatalf("expected branch to be gone, instead got error: %s", err)
		}
	})

	t.Run("delete default branch", func(t *testing.T) {
		_, err := deps.catalog.CreateRepository(ctx, "my-new-repo2", onBlock(deps, "foo2"), "main", false)
		testutil.Must(t, err)
		resp, err := clt.DeleteBranchWithResponse(ctx, "my-new-repo2", "main", &apigen.DeleteBranchParams{})
		if err != nil {
			t.Fatal("DeleteBranch error:", err)
		}
		if resp.JSONDefault == nil {
			t.Fatal("DeleteBranch expected error while trying to delete default branch")
		}
	})

	t.Run("delete branch doesnt exist", func(t *testing.T) {
		resp, err := clt.DeleteBranchWithResponse(ctx, "my-new-repo", "main5", &apigen.DeleteBranchParams{})
		if err != nil {
			t.Fatal("DeleteBranch error:", err)
		}
		if resp.JSON404 == nil {
			t.Fatal("DeleteBranch expected not found")
		}
	})

	t.Run("read-only repository", func(t *testing.T) {
		repoName := "read-only-repo"
		_, err := deps.catalog.CreateRepository(ctx, repoName, onBlock(deps, "foo1"), "main", true)
		testutil.Must(t, err)
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repoName, "main", catalog.DBEntry{Path: "a/b"}, graveler.WithForce(true)))
		_, err = deps.catalog.Commit(ctx, repoName, "main", "first commit", "test", nil, nil, nil, false, graveler.WithForce(true))
		testutil.Must(t, err)

		_, err = deps.catalog.CreateBranch(ctx, repoName, "main2", "main", graveler.WithForce(true))
		if err != nil {
			t.Fatal(err)
		}

		delResp, err := clt.DeleteBranchWithResponse(ctx, repoName, "main2", &apigen.DeleteBranchParams{})
		testutil.Must(t, err)
		if delResp.JSON403 == nil {
			t.Fatalf("expected 403 forbidden for DeleteBranch, got %d", delResp.StatusCode())
		}
		delResp, err = clt.DeleteBranchWithResponse(ctx, repoName, "main2", &apigen.DeleteBranchParams{Force: swag.Bool(true)})
		verifyResponseOK(t, delResp, err)

		_, err = deps.catalog.GetBranchReference(ctx, repoName, "main2")
		if !errors.Is(err, graveler.ErrNotFound) {
			t.Fatalf("expected branch to be gone, instead got error: %s", err)
		}
	})
}

func TestController_ObjectsStatObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "some-bucket"), "main", false)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("object_stat_no_metadata", func(t *testing.T) {
		const objPath = "foo/bar-no-metadata"
		entry := catalog.DBEntry{
			Path:            objPath,
			PhysicalAddress: "this_is_bars_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "this_is_a_checksum",
		}
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", entry))

		resp, err := clt.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: objPath})
		verifyResponseOK(t, resp, err)
		if resp.JSON200 == nil {
			t.Fatalf("expected to get back object stats, got status %s", resp.Status())
		}
		if resp.JSON200.Metadata == nil {
			t.Fatal("expected to not get back empty user-defined metadata, got nil")
		}
	})

	t.Run("get object stats", func(t *testing.T) {
		entry := catalog.DBEntry{
			Path:            "foo/bar",
			PhysicalAddress: "this_is_bars_address",
			CreationDate:    time.Now(),
			Size:            666,
			Checksum:        "this_is_a_checksum",
			Metadata:        catalog.Metadata{"additionalProperty1": "testing get object stats"},
		}
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", entry))

		resp, err := clt.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, resp, err)
		objectStats := resp.JSON200

		// verify bar stat info
		if objectStats.Path != entry.Path {
			t.Fatalf("expected to get back our path, got %s", objectStats.Path)
		}
		if apiutil.Value(objectStats.SizeBytes) != entry.Size {
			t.Fatalf("expected correct size, got %d", objectStats.SizeBytes)
		}
		if objectStats.PhysicalAddress != onBlock(deps, "some-bucket/")+entry.PhysicalAddress {
			t.Fatalf("expected correct PhysicalAddress, got %s", objectStats.PhysicalAddress)
		}
		if diff := deep.Equal(objectStats.Metadata.AdditionalProperties, map[string]string(entry.Metadata)); diff != nil {
			t.Fatalf("expected to get back user-defined metadata: %s", diff)
		}
		contentType := apiutil.Value(objectStats.ContentType)
		if contentType != catalog.DefaultContentType {
			t.Fatalf("expected to get default content type, got: %s", contentType)
		}

		// verify get stat without metadata works
		getUserMetadata := false
		resp, err = clt.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: "foo/bar", UserMetadata: &getUserMetadata})
		verifyResponseOK(t, resp, err)
		objectStatsNoMetadata := resp.JSON200
		if objectStatsNoMetadata.Metadata == nil || len(objectStatsNoMetadata.Metadata.AdditionalProperties) != 0 {
			t.Fatalf("expected to not get back empty user-defined metadata, got %+v", objectStatsNoMetadata.Metadata.AdditionalProperties)
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
		testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", entry))

		resp, err := clt.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: "foo/bar2"})
		verifyResponseOK(t, resp, err)
		objectStats := resp.JSON200

		// verify stat custom content-type
		contentType := apiutil.Value(objectStats.ContentType)
		if contentType != entry.ContentType {
			t.Fatalf("expected to get entry content type, got: %s, expected: %s", contentType, entry.ContentType)
		}
	})
}

func TestController_ObjectsListObjectsHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "bucket/prefix"), "main", false)
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
		err := deps.catalog.CreateEntry(ctx, repo, "main", entry)
		testutil.Must(t, err)
	}

	t.Run("get object list", func(t *testing.T) {
		prefix := apigen.PaginationPrefix("foo/")
		resp, err := clt.ListObjectsWithResponse(ctx, repo, "main", &apigen.ListObjectsParams{
			Prefix: &prefix,
		})
		verifyResponseOK(t, resp, err)
		results := resp.JSON200.Results
		if len(results) != 4 {
			t.Fatalf("expected 4 entries, got back %d", len(results))
		}
	})

	t.Run("get object list without user-defined metadata", func(t *testing.T) {
		prefix := apigen.PaginationPrefix("foo/")
		getUserMetadata := false
		resp, err := clt.ListObjectsWithResponse(ctx, repo, "main", &apigen.ListObjectsParams{
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
		prefix := apigen.PaginationPrefix("foo/")
		resp, err := clt.ListObjectsWithResponse(ctx, repo, "main", &apigen.ListObjectsParams{
			Prefix: &prefix,
			Amount: apiutil.Ptr(apigen.PaginationAmount(2)),
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

	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "ns1"), "main", false)
	if err != nil {
		t.Fatal(err)
	}

	expensiveString := "EXPENSIVE"

	buf := new(bytes.Buffer)
	buf.WriteString("this is file content made up of bytes")
	address := upload.DefaultPathProvider.NewPath()
	blob, err := upload.WriteBlob(context.Background(), deps.blocks, onBlock(deps, "ns1"), address, buf, 37, block.PutOpts{StorageClass: &expensiveString})
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
	testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", entry))

	expired := catalog.DBEntry{
		Path:            "foo/expired",
		PhysicalAddress: "an_expired_physical_address",
		CreationDate:    time.Now(),
		Size:            99999,
		Checksum:        "b10b",
		Expired:         true,
	}
	testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", expired))
	expectedEtag := "\"" + blob.Checksum + "\""

	t.Run("head object", func(t *testing.T) {
		resp, err := clt.HeadObjectWithResponse(ctx, repo, "main", &apigen.HeadObjectParams{Path: "foo/bar"})
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.HTTPResponse.StatusCode)
		require.Equal(t, int64(37), resp.HTTPResponse.ContentLength)
		require.Equal(t, expectedEtag, resp.HTTPResponse.Header.Get("ETag"))
		require.Empty(t, string(resp.Body))
	})

	t.Run("head object byte range", func(t *testing.T) {
		rng := "bytes=0-9"
		resp, err := clt.HeadObjectWithResponse(ctx, repo, "main", &apigen.HeadObjectParams{
			Path:  "foo/bar",
			Range: &rng,
		})
		require.Nil(t, err)
		require.Equal(t, http.StatusPartialContent, resp.HTTPResponse.StatusCode)
		require.Equal(t, int64(10), resp.HTTPResponse.ContentLength)
		require.Equal(t, expectedEtag, resp.HTTPResponse.Header.Get("ETag"))
		require.Empty(t, string(resp.Body))
	})

	t.Run("head object bad byte range", func(t *testing.T) {
		rng := "bytes=380-390"
		resp, err := clt.HeadObjectWithResponse(ctx, repo, "main", &apigen.HeadObjectParams{
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

	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "ns1"), "main", false)
	if err != nil {
		t.Fatal(err)
	}

	expensiveString := "EXPENSIVE"

	buf := new(bytes.Buffer)
	buf.WriteString("this is file content made up of bytes")
	address := upload.DefaultPathProvider.NewPath()
	blob, err := upload.WriteBlob(context.Background(), deps.blocks, onBlock(deps, "ns1"), address, buf, 37, block.PutOpts{StorageClass: &expensiveString})
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
	testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", entry))

	emptyEtag := catalog.DBEntry{
		Path:            "foo/empty-etag",
		PhysicalAddress: blob.PhysicalAddress,
		CreationDate:    time.Now(),
		Size:            blob.Size,
		Checksum:        "",
		Expired:         true,
	}
	testutil.Must(t, deps.catalog.CreateEntry(ctx, repo, "main", emptyEtag))
	expectedEtag := "\"" + blob.Checksum + "\""

	t.Run("get object", func(t *testing.T) {
		resp, err := clt.GetObjectWithResponse(ctx, repo, "main", &apigen.GetObjectParams{Path: "foo/bar"})
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
		require.Equal(t, etag, expectedEtag)

		body := string(resp.Body)
		if body != "this is file content made up of bytes" {
			t.Errorf("got unexpected body: '%s'", body)
		}
	})

	t.Run("get object byte range", func(t *testing.T) {
		rng := "bytes=0-9"
		resp, err := clt.GetObjectWithResponse(ctx, repo, "main", &apigen.GetObjectParams{
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
		require.Equal(t, etag, expectedEtag)

		body := string(resp.Body)
		if body != "this is fi" {
			t.Errorf("got unexpected body: '%s'", body)
		}
	})

	t.Run("get object bad byte range", func(t *testing.T) {
		rng := "bytes=380-390"
		resp, err := clt.GetObjectWithResponse(ctx, repo, "main", &apigen.GetObjectParams{
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
		resp, err := clt.GetUnderlyingPropertiesWithResponse(ctx, repo, "main", &apigen.GetUnderlyingPropertiesParams{Path: "foo/bar"})
		if err != nil {
			t.Fatalf("expected to get underlying properties, got %v", err)
		}
		properties := resp.JSON200
		if properties == nil {
			t.Fatalf("expected to get underlying properties, status code %d", resp.StatusCode())
		}

		if apiutil.Value(properties.StorageClass) != expensiveString {
			t.Errorf("expected to get \"%s\" storage class, got %#v", expensiveString, properties)
		}
	})

	t.Run("get object returns expected response with different etag", func(t *testing.T) {
		eTagInput := "\"11ee22ff33445566778899\""
		resp, err := clt.GetObjectWithResponse(ctx, repo, "main", &apigen.GetObjectParams{
			Path:        "foo/bar",
			IfNoneMatch: &eTagInput,
		})
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.HTTPResponse.StatusCode)
		require.Equal(t, int64(37), resp.HTTPResponse.ContentLength)
		require.Equal(t, expectedEtag, resp.HTTPResponse.Header.Get("ETag"))
	})

	t.Run("get object returns not modified with same etag", func(t *testing.T) {
		resp, err := clt.GetObjectWithResponse(ctx, repo, "main", &apigen.GetObjectParams{
			Path:        "foo/bar",
			IfNoneMatch: &expectedEtag,
		})
		require.Nil(t, err)
		require.Equal(t, http.StatusNotModified, resp.HTTPResponse.StatusCode)
		require.Equal(t, int64(0), resp.HTTPResponse.ContentLength)
		require.Empty(t, resp.HTTPResponse.Header.Get("ETag"))
	})

	t.Run("get object returns expected response for empty etag", func(t *testing.T) {
		resp, err := clt.GetObjectWithResponse(ctx, repo, "main", &apigen.GetObjectParams{
			Path: "foo/empty-etag",
		})
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.HTTPResponse.StatusCode)
		require.Equal(t, int64(37), resp.HTTPResponse.ContentLength)
		require.Equal(t, "\"\"", resp.HTTPResponse.Header.Get("ETag"))
	})
	t.Run("get object with if-none-match returns expected response for empty etag", func(t *testing.T) {
		eTagInput := "\"\""
		resp, err := clt.GetObjectWithResponse(ctx, repo, "main", &apigen.GetObjectParams{
			Path:        "foo/empty-etag",
			IfNoneMatch: &eTagInput,
		})
		require.Nil(t, err)
		require.Equal(t, http.StatusNotModified, resp.HTTPResponse.StatusCode)
		require.Equal(t, int64(0), resp.HTTPResponse.ContentLength)
		require.Empty(t, resp.HTTPResponse.Header.Get("ETag"))
	})
}

func TestController_ObjectsUploadObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "bucket/prefix"), "main", false)
	if err != nil {
		t.Fatal(err)
	}

	const content = "hello world this is my awesome content"

	t.Run("upload object", func(t *testing.T) {
		resp, err := uploadObjectHelper(t, ctx, clt, "foo/bar", strings.NewReader(content), repo, "main")
		verifyResponseOK(t, resp, err)

		sizeBytes := apiutil.Value(resp.JSON201.SizeBytes)
		const expectedSize = 38
		if sizeBytes != expectedSize {
			t.Fatalf("expected %d bytes to be written, got back %d", expectedSize, sizeBytes)
		}

		// download it
		rresp, err := clt.GetObjectWithResponse(ctx, repo, "main", &apigen.GetObjectParams{Path: "foo/bar"})
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
		resp, err := uploadObjectHelper(t, ctx, clt, "foo/bar", strings.NewReader(content), repo, "mainX")
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatal("Missing branch should return not found")
		}
	})

	t.Run("upload object missing repo", func(t *testing.T) {
		resp, err := uploadObjectHelper(t, ctx, clt, "foo/bar", strings.NewReader(content), "repo55555", "main")
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Fatal("Missing repository should return not found")
		}
	})

	t.Run("upload object missing content type", func(t *testing.T) {
		const contentType = ""
		resp, err := clt.UploadObjectWithBodyWithResponse(ctx, repo, "main", &apigen.UploadObjectParams{
			Path: "foo/bar-no-content-type",
		}, contentType, strings.NewReader(content))
		testutil.Must(t, err)
		if resp.JSON201 == nil {
			t.Fatal("Missing content type should be successful, got status", resp.Status())
		}
	})

	t.Run("read-only repository", func(t *testing.T) {
		readOnlyRepo := testUniqueRepoName()
		path := "foo/bar"
		_, err := deps.catalog.CreateRepository(ctx, readOnlyRepo, onBlock(deps, "bucket/prefix"), "main", true)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := uploadObjectHelper(t, ctx, clt, path, strings.NewReader(content), readOnlyRepo, "main")
		testutil.Must(t, err)
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("Expected 403 forbidden error for UploadObject on read-only repository, got %d", resp.StatusCode())
		}

		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		contentWriter, err := w.CreateFormFile("content", filepath.Base(path))
		if err != nil {
			t.Fatal("CreateFormFile:", err)
		}
		if _, err := io.Copy(contentWriter, strings.NewReader(content)); err != nil {
			t.Fatal("CreateFormFile write content:", err)
		}
		if err := w.Close(); err != nil {
			t.Fatal("Close multipart writer:", err)
		}

		resp, err = clt.UploadObjectWithBodyWithResponse(ctx, readOnlyRepo, "main", &apigen.UploadObjectParams{
			Path: path,
		}, w.FormDataContentType(), &b)
		testutil.Must(t, err)
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("Expected 403 forbidden error for UploadObject on read-only repository, got %d instead", resp.StatusCode())
		}

		contentWriter, err = w.CreateFormFile("content", filepath.Base(path))
		if err != nil {
			t.Fatal("CreateFormFile:", err)
		}
		if _, err := io.Copy(contentWriter, strings.NewReader(content)); err != nil {
			t.Fatal("CreateFormFile write content:", err)
		}
		if err := w.Close(); err != nil {
			t.Fatal("Close multipart writer:", err)
		}
		resp, err = clt.UploadObjectWithBodyWithResponse(ctx, readOnlyRepo, "main", &apigen.UploadObjectParams{
			Path:  path,
			Force: swag.Bool(true),
		}, w.FormDataContentType(), &b)
		verifyResponseOK(t, resp, err)

		sizeBytes := apiutil.Value(resp.JSON201.SizeBytes)
		const expectedSize = 38
		if sizeBytes != expectedSize {
			t.Fatalf("expected %d bytes to be written, got back %d", expectedSize, sizeBytes)
		}

		// download it
		rresp, err := clt.GetObjectWithResponse(ctx, readOnlyRepo, "main", &apigen.GetObjectParams{Path: path})
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
}

func TestController_ObjectsStageObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	ns := onBlock(deps, "bucket/prefix")
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, ns, "main", false)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("stage object", func(t *testing.T) {
		const expectedSizeBytes = 38
		resp, err := clt.StageObjectWithResponse(ctx, repo, "main", &apigen.StageObjectParams{Path: "foo/bar"}, apigen.StageObjectJSONRequestBody{
			Checksum:        "afb0689fe58b82c5f762991453edbbec",
			PhysicalAddress: onBlock(deps, "another-bucket/some/location"),
			SizeBytes:       expectedSizeBytes,
		})
		verifyResponseOK(t, resp, err)

		sizeBytes := apiutil.Value(resp.JSON201.SizeBytes)
		if sizeBytes != expectedSizeBytes {
			t.Fatalf("expected %d bytes to be written, got back %d", expectedSizeBytes, sizeBytes)
		}

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, statResp, err)
		objectStat := statResp.JSON200
		if objectStat.PhysicalAddress != onBlock(deps, "another-bucket/some/location") {
			t.Fatalf("unexpected physical address: %s", objectStat.PhysicalAddress)
		}
	})

	t.Run("stage object in storage ns", func(t *testing.T) {
		linkResp, err := clt.GetPhysicalAddressWithResponse(ctx, repo, "main", &apigen.GetPhysicalAddressParams{Path: "foo/bar2"})
		verifyResponseOK(t, linkResp, err)
		if linkResp.JSON200 == nil {
			t.Fatalf("GetPhysicalAddress non 200 response - status code %d", linkResp.StatusCode())
		}
		const expectedSizeBytes = 38
		resp, err := clt.LinkPhysicalAddressWithResponse(ctx, repo, "main", &apigen.LinkPhysicalAddressParams{
			Path: "foo/bar2",
		}, apigen.LinkPhysicalAddressJSONRequestBody{
			Checksum:  "afb0689fe58b82c5f762991453edbbec",
			SizeBytes: expectedSizeBytes,
			Staging: apigen.StagingLocation{
				PhysicalAddress: linkResp.JSON200.PhysicalAddress,
			},
		})
		verifyResponseOK(t, resp, err)

		sizeBytes := apiutil.Value(resp.JSON200.SizeBytes)
		if sizeBytes != expectedSizeBytes {
			t.Fatalf("expected %d bytes to be written, got back %d", expectedSizeBytes, sizeBytes)
		}

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: "foo/bar2"})
		verifyResponseOK(t, statResp, err)
		if statResp.JSON200 == nil {
			t.Fatalf("StatObject non 200 - status code %d", statResp.StatusCode())
		}
		objectStat := statResp.JSON200
		if objectStat.PhysicalAddress != apiutil.Value(linkResp.JSON200.PhysicalAddress) {
			t.Fatalf("unexpected physical address: %s", objectStat.PhysicalAddress)
		}
	})

	t.Run("upload object missing branch", func(t *testing.T) {
		resp, err := clt.StageObjectWithResponse(ctx, repo, "main1234", &apigen.StageObjectParams{Path: "foo/bar"},
			apigen.StageObjectJSONRequestBody{
				Checksum:        "afb0689fe58b82c5f762991453edbbec",
				PhysicalAddress: onBlock(deps, "another-bucket/some/location"),
				SizeBytes:       38,
			})
		testutil.Must(t, err)
		require.NotNil(t, resp.JSON404)
		require.Contains(t, resp.JSON404.Message, "branch not found")
	})

	t.Run("wrong storage adapter", func(t *testing.T) {
		resp, err := clt.StageObjectWithResponse(ctx, repo, "main1234", &apigen.StageObjectParams{
			Path: "foo/bar",
		}, apigen.StageObjectJSONRequestBody{
			Checksum:        "afb0689fe58b82c5f762991453edbbec",
			PhysicalAddress: "gs://another-bucket/some/location",
			SizeBytes:       38,
		})
		testutil.Must(t, err)
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, "physical address is not valid for block adapter")
	})

	t.Run("read-only repository", func(t *testing.T) {
		readOnlyRepo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, readOnlyRepo, onBlock(deps, "bucket/prefix"), "main", true)
		if err != nil {
			t.Fatal(err)
		}
		const expectedSizeBytes = 38
		resp, err := clt.StageObjectWithResponse(ctx, readOnlyRepo, "main", &apigen.StageObjectParams{Path: "foo/bar"}, apigen.StageObjectJSONRequestBody{
			Checksum:        "afb0689fe58b82c5f762991453edbbec",
			PhysicalAddress: onBlock(deps, "another-bucket/some/location"),
			SizeBytes:       expectedSizeBytes,
		})
		testutil.Must(t, err)
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("expected 403 forbidden status for StageObject for read-only repository, got %d", resp.StatusCode())
		}

		resp, err = clt.StageObjectWithResponse(ctx, readOnlyRepo, "main", &apigen.StageObjectParams{Path: "foo/bar"}, apigen.StageObjectJSONRequestBody{
			Checksum:        "afb0689fe58b82c5f762991453edbbec",
			PhysicalAddress: onBlock(deps, "another-bucket/some/location"),
			SizeBytes:       expectedSizeBytes,
			Force:           swag.Bool(true),
		})
		verifyResponseOK(t, resp, err)

		sizeBytes := apiutil.Value(resp.JSON201.SizeBytes)
		if sizeBytes != expectedSizeBytes {
			t.Fatalf("expected %d bytes to be written, got back %d", expectedSizeBytes, sizeBytes)
		}

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, readOnlyRepo, "main", &apigen.StatObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, statResp, err)
		objectStat := statResp.JSON200
		if objectStat.PhysicalAddress != onBlock(deps, "another-bucket/some/location") {
			t.Fatalf("unexpected physical address: %s", objectStat.PhysicalAddress)
		}
	})

	t.Run("missing signature", func(t *testing.T) {
		address := fmt.Sprintf("%s/%s", ns, upload.DefaultPathProvider.NewPath())
		resp, err := clt.LinkPhysicalAddressWithResponse(ctx, repo, "main", &apigen.LinkPhysicalAddressParams{
			Path: "foo/bar",
		}, apigen.LinkPhysicalAddressJSONRequestBody{
			Checksum:  "afb0689fe58b82c5f762991453edbbec",
			SizeBytes: 38,
			Staging: apigen.StagingLocation{
				PhysicalAddress: &address,
			},
		})
		testutil.Must(t, err)
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, "address is not signed")
	})
	t.Run("malformed signature", func(t *testing.T) {
		address := fmt.Sprintf("%s/%s,someBadSig?=", ns, upload.DefaultPathProvider.NewPath())
		resp, err := clt.LinkPhysicalAddressWithResponse(ctx, repo, "main", &apigen.LinkPhysicalAddressParams{
			Path: "foo/bar",
		}, apigen.LinkPhysicalAddressJSONRequestBody{
			Checksum:  "afb0689fe58b82c5f762991453edbbec",
			SizeBytes: 38,
			Staging: apigen.StagingLocation{
				PhysicalAddress: &address,
			},
		})
		testutil.Must(t, err)
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, "malformed address signature")
	})
	t.Run("invalid signature", func(t *testing.T) {
		// create a random 64 bytes (512 bits) secret
		secret := make([]byte, 64)
		_, err := rand.Read(secret)
		require.NoError(t, err)
		sig := base64.RawURLEncoding.EncodeToString(secret)

		address := fmt.Sprintf("%s/%s,%s", ns, upload.DefaultPathProvider.NewPath(), sig)
		resp, err := clt.LinkPhysicalAddressWithResponse(ctx, repo, "main", &apigen.LinkPhysicalAddressParams{
			Path: "foo/bar",
		}, apigen.LinkPhysicalAddressJSONRequestBody{
			Checksum:  "afb0689fe58b82c5f762991453edbbec",
			SizeBytes: 38,
			Staging: apigen.StagingLocation{
				PhysicalAddress: &address,
			},
		})
		testutil.Must(t, err)
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, "invalid address signature")
	})
}

func TestController_ObjectsDeleteObjectHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	repo := testUniqueRepoName()
	const branch = "main"
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "some-bucket/prefix"), branch, false)
	if err != nil {
		t.Fatal(err)
	}

	const content = "hello world this is my awesome content"

	t.Run("delete object", func(t *testing.T) {
		resp, err := uploadObjectHelper(t, ctx, clt, "foo/bar", strings.NewReader(content), repo, branch)
		verifyResponseOK(t, resp, err)

		sizeBytes := apiutil.Value(resp.JSON201.SizeBytes)
		if sizeBytes != 38 {
			t.Fatalf("expected 38 bytes to be written, got back %d", sizeBytes)
		}

		// download it
		rresp, err := clt.GetObjectWithResponse(ctx, repo, branch, &apigen.GetObjectParams{Path: "foo/bar"})
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
		delResp, err := clt.DeleteObjectWithResponse(ctx, repo, branch, &apigen.DeleteObjectParams{Path: "foo/bar"})
		verifyResponseOK(t, delResp, err)

		// get it
		statResp, err := clt.StatObjectWithResponse(ctx, repo, branch, &apigen.StatObjectParams{Path: "foo/bar"})
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
		delResp, err := clt.DeleteObjectsWithResponse(ctx, repo, branch, &apigen.DeleteObjectsParams{}, apigen.DeleteObjectsJSONRequestBody{Paths: paths})
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
			statResp, err := clt.StatObjectWithResponse(ctx, repo, branch, &apigen.StatObjectParams{Path: p})
			testutil.Must(t, err)
			if statResp == nil {
				t.Fatalf("StatObject missing response for '%s'", p)
			}
			if statResp.JSON404 == nil {
				t.Fatalf("expected file to be gone now for '%s'", p)
			}
		}

		// delete objects again - make sure we do not fail or get any error
		delResp2, err := clt.DeleteObjectsWithResponse(ctx, repo, branch, &apigen.DeleteObjectsParams{}, apigen.DeleteObjectsJSONRequestBody{Paths: paths})
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
		delResp, err := clt.DeleteObjectsWithResponse(ctx, repo, branch, &apigen.DeleteObjectsParams{}, apigen.DeleteObjectsJSONRequestBody{Paths: paths})
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
		rules := map[string]*graveler.BranchProtectionBlockedActions{
			"*": {
				Value: []graveler.BranchProtectionBlockedAction{
					graveler.BranchProtectionBlockedAction_STAGING_WRITE,
				},
			},
		}
		err = deps.catalog.SetBranchProtectionRules(ctx, repo, &graveler.BranchProtectionRules{
			BranchPatternToBlockedActions: rules,
		}, swag.String(""))
		testutil.Must(t, err)

		// delete objects
		delResp, err := clt.DeleteObjectsWithResponse(ctx, repo, "protected", &apigen.DeleteObjectsParams{}, apigen.DeleteObjectsJSONRequestBody{Paths: paths})
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
			errPaths = append(errPaths, apiutil.Value(item.Path))
		}
		// sort both lists to match
		sort.Strings(errPaths)
		sort.Strings(paths)
		if diff := deep.Equal(paths, errPaths); diff != nil {
			t.Fatalf("DeleteObjects errors path difference: %s", diff)
		}
	})

	t.Run("read-only repository", func(t *testing.T) {
		readOnlyRepo := testUniqueRepoName()
		const branch = "main"
		_, err := deps.catalog.CreateRepository(ctx, readOnlyRepo, onBlock(deps, "some-bucket/prefix2"), branch, true)
		if err != nil {
			t.Fatal(err)
		}

		const content = "hello world this is my awesome content"
		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		contentWriter, err := w.CreateFormFile("content", filepath.Base("foo/bar"))
		if err != nil {
			t.Fatal("CreateFormFile:", err)
		}
		if _, err := io.Copy(contentWriter, strings.NewReader(content)); err != nil {
			t.Fatal("CreateFormFile write content:", err)
		}
		if err := w.Close(); err != nil {
			t.Fatal("Close multipart writer:", err)
		}

		resp, err := clt.UploadObjectWithBodyWithResponse(ctx, readOnlyRepo, "main", &apigen.UploadObjectParams{
			Path:  "foo/bar",
			Force: swag.Bool(true),
		}, w.FormDataContentType(), &b)
		verifyResponseOK(t, resp, err)

		sizeBytes := apiutil.Value(resp.JSON201.SizeBytes)
		if sizeBytes != 38 {
			t.Fatalf("expected 38 bytes to be written, got back %d", sizeBytes)
		}

		// download it
		rresp, err := clt.GetObjectWithResponse(ctx, readOnlyRepo, branch, &apigen.GetObjectParams{Path: "foo/bar"})
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
		delResp, err := clt.DeleteObjectWithResponse(ctx, readOnlyRepo, branch, &apigen.DeleteObjectParams{Path: "foo/bar"})
		testutil.Must(t, err)
		if delResp.JSON403 == nil {
			t.Fatalf("expected DeleteObject to fail with 403 forbidden, got %d", delResp.StatusCode())
		}

		delResp, err = clt.DeleteObjectWithResponse(ctx, readOnlyRepo, branch, &apigen.DeleteObjectParams{
			Path:  "foo/bar",
			Force: swag.Bool(true),
		})
		verifyResponseOK(t, delResp, err)

		// get it
		statResp, err := clt.StatObjectWithResponse(ctx, readOnlyRepo, branch, &apigen.StatObjectParams{Path: "foo/bar"})
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
	clt, _ := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("invalid_policy_effect", func(t *testing.T) {
		resp, err := clt.CreatePolicyWithResponse(ctx, apigen.CreatePolicyJSONRequestBody{
			CreationDate: apiutil.Ptr(time.Now().Unix()),
			Id:           "ValidPolicyID",
			Statement: []apigen.Statement{
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
		resp, err := clt.CreatePolicyWithResponse(ctx, apigen.CreatePolicyJSONRequestBody{
			CreationDate: apiutil.Ptr(time.Now().Unix()),
			Id:           "ValidPolicyID",
			Statement: []apigen.Statement{
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

func TestController_LogAction(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	// create repository
	name := testUniqueRepoName()
	resp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
		Name:             name,
		StorageNamespace: onBlock(deps, name),
	})
	verifyResponseOK(t, resp, err)

	// lookup and verify action metric
	idx := slices.IndexFunc(deps.collector.Metrics, func(item *stats.Metric) bool {
		return item.Class == "api_server" && item.Name == "create_repo"
	})
	if idx == -1 {
		t.Fatal("Failed to find create_repo metric")
	}
	m := deps.collector.Metrics[idx]
	if m.UserID != "admin" {
		t.Fatalf("Expected userID to be admin, got %s", m.UserID)
	}
	if m.Repository != name {
		t.Fatalf("Expected repository to be %s, got %s", name, m.Repository)
	}
	if len(m.Client) == 0 {
		t.Fatalf("Expected client to be set, got empty Client")
	}
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
		key                *apigen.AccessKeyCredentials
		expectedStatusCode int
	}{
		{
			name:               "simple",
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "accessKeyAndSecret",
			key: &apigen.AccessKeyCredentials{
				AccessKeyId:     validAccessKeyID,
				SecretAccessKey: "cetec astronomy",
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "emptyAccessKeyId",
			key: &apigen.AccessKeyCredentials{
				SecretAccessKey: "cetec astronomy",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "emptySecretKey",
			key: &apigen.AccessKeyCredentials{
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
			_, _ = clt.SetupCommPrefsWithResponse(ctx, apigen.SetupCommPrefsJSONRequestBody{
				Email:           &mockEmail,
				FeatureUpdates:  false,
				SecurityUpdates: false,
			})

			resp, err := clt.SetupWithResponse(ctx, apigen.SetupJSONRequestBody{
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
				res, err := clt.SetupWithResponse(ctx, apigen.SetupJSONRequestBody{
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
	cred := createDefaultAdminUser(t, clt)

	resp, err := clt.LoginWithResponse(context.Background(), apigen.LoginJSONRequestBody{
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

func TestController_STSLogin(t *testing.T) {
	handler, _ := setupHandler(t)
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")

	res, err := clt.StsLoginWithResponse(context.Background(), apigen.StsLoginJSONRequestBody{})
	if err != nil {
		t.Fatalf("Error login with response %v", err)
	}
	if res.StatusCode() != http.StatusNotImplemented {
		t.Errorf("sts login response should return %v but returned %v", http.StatusNotImplemented, res.StatusCode())
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
	repo := testUniqueRepoName()
	resp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
		DefaultBranch:    apiutil.Ptr("main"),
		Name:             repo,
		StorageNamespace: "mem://repo9",
	})
	verifyResponseOK(t, resp, err)
	// upload action for pre-commit
	var b bytes.Buffer
	testutil.MustDo(t, "execute action template", listRepositoryRunsActionTemplate.Execute(&b, httpServer))
	actionContent := b.String()
	uploadResp, err := uploadObjectHelper(t, ctx, clt, "_lakefs_actions/pre_commit.yaml", strings.NewReader(actionContent), repo, "main")
	verifyResponseOK(t, uploadResp, err)
	// commit
	respCommit, err := clt.CommitWithResponse(ctx, repo, "main", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "pre-commit action",
	})
	verifyResponseOK(t, respCommit, err)
	// work branch
	branchResp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
		Name:   "work",
		Source: "main",
	})
	verifyResponseOK(t, branchResp, err)
	// upload and commit content on branch
	commitIDs := []string{respCommit.JSON201.Id}
	const contentCount = 5
	for i := 0; i < contentCount; i++ {
		content := fmt.Sprintf("content-%d", i)
		uploadResp, err := uploadObjectHelper(t, ctx, clt, content, strings.NewReader(content), repo, "work")
		verifyResponseOK(t, uploadResp, err)
		respCommit, err := clt.CommitWithResponse(ctx, repo, "work", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{Message: content})
		verifyResponseOK(t, respCommit, err)
		commitIDs = append(commitIDs, respCommit.JSON201.Id)
	}

	t.Run("total", func(t *testing.T) {
		respList, err := clt.ListRepositoryRunsWithResponse(ctx, repo, &apigen.ListRepositoryRunsParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(100)),
		})
		verifyResponseOK(t, respList, err)
		runsCount := len(respList.JSON200.Results)
		if runsCount != contentCount+1 {
			t.Fatalf("ListRepositoryRuns() got %d results, expected %d+1", runsCount, contentCount)
		}
	})

	t.Run("on branch", func(t *testing.T) {
		respList, err := clt.ListRepositoryRunsWithResponse(ctx, repo, &apigen.ListRepositoryRunsParams{
			Branch: apiutil.Ptr("work"),
			Amount: apiutil.Ptr(apigen.PaginationAmount(100)),
		})
		verifyResponseOK(t, respList, err)
		runsCount := len(respList.JSON200.Results)
		if runsCount != contentCount {
			t.Fatalf("ListRepositoryRuns() on `work` branch got %d results, expected %d", runsCount, contentCount)
		}
	})

	t.Run("on branch and commit", func(t *testing.T) {
		respList, err := clt.ListRepositoryRunsWithResponse(ctx, repo, &apigen.ListRepositoryRunsParams{
			Branch: apiutil.Ptr("someBranch"),
			Commit: apiutil.Ptr("someCommit"),
			Amount: apiutil.Ptr(apigen.PaginationAmount(100)),
		})
		require.NoError(t, err)
		require.NotNil(t, respList)
		require.Equal(t, http.StatusBadRequest, respList.StatusCode())
	})

	t.Run("on deleted branch", func(t *testing.T) {
		// delete work branch and list them again
		delResp, err := clt.DeleteBranchWithResponse(ctx, repo, "work", &apigen.DeleteBranchParams{})
		verifyResponseOK(t, delResp, err)

		respList, err := clt.ListRepositoryRunsWithResponse(ctx, repo, &apigen.ListRepositoryRunsParams{
			Branch: apiutil.Ptr("work"),
			Amount: apiutil.Ptr(apigen.PaginationAmount(100)),
		})
		verifyResponseOK(t, respList, err)
		runsCount := len(respList.JSON200.Results)
		if runsCount != contentCount {
			t.Fatalf("ListRepositoryRuns() got %d results, expected %d (after delete repository)", runsCount, contentCount)
		}
	})
}

func TestController_MergeInvalidStrategy(t *testing.T) {
	clt, _ := setupClientWithAdmin(t)
	ctx := context.Background()

	repoName := testUniqueRepoName()
	repoResp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
		DefaultBranch:    apiutil.Ptr("main"),
		Name:             repoName,
		StorageNamespace: "mem://",
	})
	verifyResponseOK(t, repoResp, err)

	branchResp, err := clt.CreateBranchWithResponse(ctx, repoName, apigen.CreateBranchJSONRequestBody{Name: "work", Source: "main"})
	verifyResponseOK(t, branchResp, err)

	const content = "awesome content"
	resp, err := uploadObjectHelper(t, ctx, clt, "file1", strings.NewReader(content), repoName, "work")
	verifyResponseOK(t, resp, err)

	commitResp, err := clt.CommitWithResponse(ctx, repoName, "work", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{Message: "file 1 commit to work"})
	verifyResponseOK(t, commitResp, err)

	strategy := "bad strategy"
	mergeResp, err := clt.MergeIntoBranchWithResponse(ctx, repoName, "work", "main", apigen.MergeIntoBranchJSONRequestBody{
		Message:  apiutil.Ptr("merge work to main"),
		Strategy: &strategy,
	})
	testutil.Must(t, err)
	require.Equal(t, http.StatusBadRequest, mergeResp.StatusCode())
}

func TestController_MergeDiffWithParent(t *testing.T) {
	clt, _ := setupClientWithAdmin(t)
	ctx := context.Background()

	repoName := testUniqueRepoName()
	repoResp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
		DefaultBranch:    apiutil.Ptr("main"),
		Name:             repoName,
		StorageNamespace: "mem://",
	})
	verifyResponseOK(t, repoResp, err)

	branchResp, err := clt.CreateBranchWithResponse(ctx, repoName, apigen.CreateBranchJSONRequestBody{Name: "work", Source: "main"})
	verifyResponseOK(t, branchResp, err)

	const content = "awesome content"
	resp, err := uploadObjectHelper(t, ctx, clt, "file1", strings.NewReader(content), repoName, "work")
	verifyResponseOK(t, resp, err)

	commitResp, err := clt.CommitWithResponse(ctx, repoName, "work", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{Message: "file 1 commit to work"})
	verifyResponseOK(t, commitResp, err)

	mergeResp, err := clt.MergeIntoBranchWithResponse(ctx, repoName, "work", "main", apigen.MergeIntoBranchJSONRequestBody{
		Message: apiutil.Ptr("merge work to main"),
	})
	verifyResponseOK(t, mergeResp, err)

	diffResp, err := clt.DiffRefsWithResponse(ctx, repoName, "main~1", "main", &apigen.DiffRefsParams{})
	verifyResponseOK(t, diffResp, err)
	expectedSize := int64(len(content))
	expectedResults := []apigen.Diff{
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
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	testutil.Must(t, err)
	_, err = deps.catalog.CreateBranch(ctx, repo, "branch1", "main")
	testutil.Must(t, err)
	err = deps.catalog.CreateEntry(ctx, repo, "branch1", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"})
	testutil.Must(t, err)
	_, err = deps.catalog.Commit(ctx, repo, "branch1", "some message", DefaultUserID, nil, nil, nil, false)
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
			resp, err := clt.MergeIntoBranchWithResponse(ctx, repo, "branch1", destinationBranch, apigen.MergeIntoBranchJSONRequestBody{})
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
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	testutil.Must(t, err)
	err = deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"})
	testutil.Must(t, err)
	_, err = deps.catalog.CreateBranch(ctx, repo, "branch1", "main")
	testutil.Must(t, err)
	err = deps.catalog.CreateEntry(ctx, repo, "branch1", catalog.DBEntry{Path: "foo/bar2", PhysicalAddress: "bar2addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum2"})
	testutil.Must(t, err)
	_, err = deps.catalog.Commit(ctx, repo, "branch1", "some message", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)

	// merge branch1 to main (dirty)
	resp, err := clt.MergeIntoBranchWithResponse(ctx, repo, "branch1", "main", apigen.MergeIntoBranchJSONRequestBody{})
	testutil.MustDo(t, "perform merge into dirty branch", err)
	if resp.JSON400 == nil || resp.JSON400.Message != graveler.ErrDirtyBranch.Error() {
		t.Errorf("Merge dirty branch should fail with ErrDirtyBranch, got %+v", resp)
	}
}

func TestController_MergeBranchWithNoChanges(t *testing.T) {
	clt, _ := setupClientWithAdmin(t)
	ctx := context.Background()

	repoName := testUniqueRepoName()
	repoResp, err := clt.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
		DefaultBranch:    apiutil.Ptr("main"),
		Name:             repoName,
		StorageNamespace: "mem://",
	})
	verifyResponseOK(t, repoResp, err)

	branch1Resp, err := clt.CreateBranchWithResponse(ctx, repoName, apigen.CreateBranchJSONRequestBody{Name: "branch1", Source: "main"})
	verifyResponseOK(t, branch1Resp, err)

	branch2Resp, err := clt.CreateBranchWithResponse(ctx, repoName, apigen.CreateBranchJSONRequestBody{Name: "branch2", Source: "main"})
	verifyResponseOK(t, branch2Resp, err)

	mergeResp, err := clt.MergeIntoBranchWithResponse(ctx, repoName, "branch2", "branch1", apigen.MergeIntoBranchJSONRequestBody{
		Message: apiutil.Ptr("Merge branch2 to branch1"),
	})
	testutil.MustDo(t, "perform merge with no changes", err)
	if mergeResp.JSON400 == nil || !strings.HasSuffix(mergeResp.JSON400.Message, graveler.ErrNoChanges.Error()) {
		t.Errorf("Merge branches with no changes should fail with ErrNoChanges, got %+v", mergeResp)
	}

	mergeWithAllowEmptyFlagResp, err := clt.MergeIntoBranchWithResponse(ctx, repoName, "branch2", "branch1", apigen.MergeIntoBranchJSONRequestBody{
		Message:    apiutil.Ptr("Merge branch2 to branch1"),
		AllowEmpty: swag.Bool(true),
	})
	verifyResponseOK(t, mergeWithAllowEmptyFlagResp, err)

	mergeWithForceFlagResp, err := clt.MergeIntoBranchWithResponse(ctx, repoName, "branch2", "branch1", apigen.MergeIntoBranchJSONRequestBody{
		Message: apiutil.Ptr("Merge branch2 to branch1"),
		Force:   swag.Bool(true),
	})
	verifyResponseOK(t, mergeWithForceFlagResp, err)
}

func TestController_CreateTag(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	// setup env
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	testutil.Must(t, err)
	testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
	commit1, err := deps.catalog.Commit(ctx, repo, "main", "some message", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)

	t.Run("ref", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  "tag1",
			Ref: commit1.Reference,
		})
		verifyResponseOK(t, tagResp, err)
	})

	t.Run("branch", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  "tag2",
			Ref: "main",
		})
		verifyResponseOK(t, tagResp, err)
	})

	t.Run("branch_with_latest_modifier", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  "tag3",
			Ref: "main@",
		})
		verifyResponseOK(t, tagResp, err)
	})

	t.Run("branch_with_staging_modifier", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  "tag4",
			Ref: "main$",
		})
		testutil.Must(t, err)
		if tagResp.JSON400 == nil {
			t.Errorf("Create tag to explicit stage should fail with validation error, got (status code: %d): %s", tagResp.StatusCode(), tagResp.Body)
		}
	})

	t.Run("tag_tag", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  "tag5",
			Ref: "main",
		})
		verifyResponseOK(t, tagResp, err)
		tagTagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  "tag6",
			Ref: "tag5",
		})
		verifyResponseOK(t, tagTagResp, err)
	})

	t.Run("not_exists", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  "tag6",
			Ref: "unknown",
		})
		testutil.Must(t, err)
		if tagResp.JSON404 == nil {
			t.Errorf("Create tag to unknown ref expected 404, got %v", tagResp)
		}
	})

	t.Run("tag_with_conflicting_tag", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  "tag7",
			Ref: "main",
		})
		verifyResponseOK(t, tagResp, err)
		tagTagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  "tag7",
			Ref: "main",
		})
		testutil.Must(t, err)
		if tagTagResp.JSON409 == nil {
			t.Errorf("Create tag again should conflict, got %v", tagTagResp)
		}
	})

	t.Run("tag_with_conflicting_branch", func(t *testing.T) {
		tagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
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

		tagResp, err := clt.CreateTagWithResponse(ctx, repo, apigen.CreateTagJSONRequestBody{
			Id:  commit.Reference,
			Ref: "main",
		})
		testutil.Must(t, err)

		if tagResp.JSON409 == nil {
			t.Errorf("Create tag again should conflict, got %v", tagResp)
		}
	})

	t.Run("read-only repository", func(t *testing.T) {
		readOnlyRepo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, readOnlyRepo, onBlock(deps, readOnlyRepo), "main", true)
		testutil.Must(t, err)
		testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, readOnlyRepo, "main", catalog.DBEntry{Path: "foo/bar2", PhysicalAddress: "bar2addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}, graveler.WithForce(true)))
		commit1, err := deps.catalog.Commit(ctx, readOnlyRepo, "main", "some message", DefaultUserID, nil, nil, nil, false, graveler.WithForce(true))
		testutil.Must(t, err)
		tagResp, err := clt.CreateTagWithResponse(ctx, readOnlyRepo, apigen.CreateTagJSONRequestBody{
			Id:  "tag1",
			Ref: commit1.Reference,
		})
		testutil.Must(t, err)
		if tagResp.JSON403 == nil {
			t.Errorf("Create tag to read-only repo should fail with 403 forbidden, got (status code: %d): %s", tagResp.StatusCode(), tagResp.Body)
		}
		tagResp, err = clt.CreateTagWithResponse(ctx, readOnlyRepo, apigen.CreateTagJSONRequestBody{
			Id:    "tag1",
			Ref:   commit1.Reference,
			Force: swag.Bool(true),
		})
		verifyResponseOK(t, tagResp, err)
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
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	testutil.Must(t, err)
	testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
	_, err = deps.catalog.Commit(ctx, repo, "main", "some message", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)

	t.Run("ref", func(t *testing.T) {
		branchResp, err := clt.GetBranchWithResponse(ctx, repo, "main")
		verifyResponseOK(t, branchResp, err)
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "main", apigen.RevertBranchJSONRequestBody{Ref: branchResp.JSON200.CommitId})
		verifyResponseOK(t, revertResp, err)
	})

	t.Run("branch", func(t *testing.T) {
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "main", apigen.RevertBranchJSONRequestBody{Ref: "main"})
		verifyResponseOK(t, revertResp, err)
	})

	t.Run("committed", func(t *testing.T) {
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "main", apigen.RevertBranchJSONRequestBody{Ref: "main@"})
		verifyResponseOK(t, revertResp, err)
	})

	t.Run("staging", func(t *testing.T) {
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "main", apigen.RevertBranchJSONRequestBody{Ref: "main$"})
		testutil.Must(t, err)
		if revertResp.JSON400 == nil {
			t.Errorf("Revert should fail with stating reference, got (status code: %d): %s", revertResp.StatusCode(), revertResp.Body)
		}
	})

	t.Run("dirty_branch", func(t *testing.T) {
		// create branch with entry without a commit
		createBranch, err := deps.catalog.CreateBranch(ctx, repo, "dirty", "main")
		testutil.Must(t, err)
		err = deps.catalog.CreateEntry(ctx, repo, "dirty", catalog.DBEntry{Path: "foo/bar2", PhysicalAddress: "bar2addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum2"})
		testutil.Must(t, err)

		// revert changes should fail
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "dirty", apigen.RevertBranchJSONRequestBody{Ref: createBranch.Reference})
		testutil.Must(t, err)
		if revertResp.JSON400 == nil || revertResp.JSON400.Message != graveler.ErrDirtyBranch.Error() {
			t.Errorf("Revert dirty branch should fail with ErrDirtyBranch, got %+v", revertResp)
		}
	})

	t.Run("revert_no_parent", func(t *testing.T) {
		repo := testUniqueRepoName()
		// setup data - repo with one object committed
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.Must(t, err)
		err = deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "merge/foo/bar1", PhysicalAddress: "merge1bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"})
		testutil.Must(t, err)
		_, err = deps.catalog.Commit(ctx, repo, "main", "first", DefaultUserID, nil, nil, nil, false)
		testutil.Must(t, err)
		// create branch with one entry committed
		_, err = deps.catalog.CreateBranch(ctx, repo, "branch1", "main")
		testutil.Must(t, err)
		err = deps.catalog.CreateEntry(ctx, repo, "branch1", catalog.DBEntry{Path: "merge/foo/bar2", PhysicalAddress: "merge2bar2addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum2"})
		testutil.Must(t, err)
		_, err = deps.catalog.Commit(ctx, repo, "branch1", "second", DefaultUserID, nil, nil, nil, false)
		testutil.Must(t, err)
		// merge branch1 to main
		mergeRef, err := deps.catalog.Merge(ctx, repo, "main", "branch1", DefaultUserID, "merge to main", catalog.Metadata{}, "")
		testutil.Must(t, err)

		// revert changes should fail
		revertResp, err := clt.RevertBranchWithResponse(ctx, repo, "main", apigen.RevertBranchJSONRequestBody{Ref: mergeRef})
		testutil.Must(t, err)
		if revertResp.JSON409 == nil || revertResp.JSON409.Message != graveler.ErrRevertMergeNoParent.Error() {
			t.Errorf("Revert dirty merge no parent specified was expected, got %+v", revertResp)
		}
	})

	t.Run("read-only repository", func(t *testing.T) {
		readOnlyRepository := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, readOnlyRepository, onBlock(deps, readOnlyRepository), "main", true)
		testutil.Must(t, err)
		testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, readOnlyRepository, "main", catalog.DBEntry{Path: "foo/bar2", PhysicalAddress: "bar2addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}, graveler.WithForce(true)))
		_, err = deps.catalog.Commit(ctx, readOnlyRepository, "main", "some message", DefaultUserID, nil, nil, nil, false, graveler.WithForce(true))
		testutil.Must(t, err)
		revertResp, err := clt.RevertBranchWithResponse(ctx, readOnlyRepository, "main", apigen.RevertBranchJSONRequestBody{Ref: "main"})
		testutil.Must(t, err)
		if revertResp.JSON403 == nil {
			t.Errorf("Revert should fail with 403 forbidden for read-only repository, got (status code: %d): %s", revertResp.StatusCode(), revertResp.Body)
		}
		revertResp, err = clt.RevertBranchWithResponse(ctx, readOnlyRepository, "main", apigen.RevertBranchJSONRequestBody{Ref: "main", Force: swag.Bool(true)})
		verifyResponseOK(t, revertResp, err)
	})
}

func TestController_RevertConflict(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	// setup env
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	testutil.Must(t, err)
	testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
	firstCommit, err := deps.catalog.Commit(ctx, repo, "main", "some message", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)
	testutil.MustDo(t, "overriding entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum2"}))
	_, err = deps.catalog.Commit(ctx, repo, "main", "some other message", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)

	resp, err := clt.RevertBranchWithResponse(ctx, repo, "main", apigen.RevertBranchJSONRequestBody{Ref: firstCommit.Reference})
	testutil.Must(t, err)
	if resp.HTTPResponse.StatusCode != http.StatusConflict {
		t.Errorf("Revert with a conflict should fail with status %d got %d", http.StatusConflict, resp.HTTPResponse.StatusCode)
	}
}

func TestController_CherryPick(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	// setup env
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	testutil.Must(t, err)
	testutil.MustDo(t, "create entry bar1", deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: "foo/bar1", PhysicalAddress: "bar1addr", CreationDate: time.Now(), Size: 1, Checksum: "cksum1"}))
	_, err = deps.catalog.Commit(ctx, repo, "main", "message1", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)

	for _, name := range []string{"branch1", "branch2", "branch3", "branch4", "dest-branch1", "dest-branch2", "dest-branch3", "dest-branch4"} {
		_, err = deps.catalog.CreateBranch(ctx, repo, name, "main")
		testutil.Must(t, err)
	}

	testutil.MustDo(t, "create entry bar2", deps.catalog.CreateEntry(ctx, repo, "branch1", catalog.DBEntry{Path: "foo/bar2", PhysicalAddress: "bar2addr", CreationDate: time.Now(), Size: 2, Checksum: "cksum2"}))
	commit2, err := deps.catalog.Commit(ctx, repo, "branch1", "message2", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)

	testutil.MustDo(t, "create entry bar3", deps.catalog.CreateEntry(ctx, repo, "branch1", catalog.DBEntry{Path: "foo/bar3", PhysicalAddress: "bar3addr", CreationDate: time.Now(), Size: 3, Checksum: "cksum3"}))
	testutil.MustDo(t, "create entry bar4", deps.catalog.CreateEntry(ctx, repo, "branch1", catalog.DBEntry{Path: "foo/bar4", PhysicalAddress: "bar4addr", CreationDate: time.Now(), Size: 4, Checksum: "cksum4"}))
	_, err = deps.catalog.Commit(ctx, repo, "branch1", "message34", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)

	testutil.MustDo(t, "create entry bar6", deps.catalog.CreateEntry(ctx, repo, "branch2", catalog.DBEntry{Path: "foo/bar6", PhysicalAddress: "bar6addr", CreationDate: time.Now(), Size: 6, Checksum: "cksum6"}))
	testutil.MustDo(t, "create entry bar7", deps.catalog.CreateEntry(ctx, repo, "branch2", catalog.DBEntry{Path: "foo/bar7", PhysicalAddress: "bar7addr", CreationDate: time.Now(), Size: 7, Checksum: "cksum7"}))
	_, err = deps.catalog.Commit(ctx, repo, "branch2", "message34", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)

	testutil.MustDo(t, "create entry bar8", deps.catalog.CreateEntry(ctx, repo, "branch3", catalog.DBEntry{Path: "foo/bar8", PhysicalAddress: "bar8addr", CreationDate: time.Now(), Size: 8, Checksum: "cksum8"}))
	_, err = deps.catalog.Commit(ctx, repo, "branch3", "message8", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)

	testutil.MustDo(t, "create entry bar2", deps.catalog.CreateEntry(ctx, repo, "branch4", catalog.DBEntry{Path: "foo/bar2", PhysicalAddress: "bar2addr4", CreationDate: time.Now(), Size: 24, Checksum: "cksum24"}))
	_, err = deps.catalog.Commit(ctx, repo, "branch4", "message4", DefaultUserID, nil, nil, nil, false)
	testutil.Must(t, err)

	_, err = deps.catalog.Merge(ctx, repo, "branch3", "branch1", DefaultUserID,
		"merge message", catalog.Metadata{"foo": "bar"}, "")
	testutil.Must(t, err)

	t.Run("from branch", func(t *testing.T) {
		cherryResponse, err := clt.CherryPickWithResponse(ctx, repo, "dest-branch1", apigen.CherryPickJSONRequestBody{Ref: "branch1"})
		verifyResponseOK(t, cherryResponse, err)

		// verify that the cherry-pick worked as expected
		resp, err := clt.GetObjectWithResponse(ctx, repo, "dest-branch1", &apigen.GetObjectParams{Path: "foo/bar2"})
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Error("expected to not find object foo/bar2 in dest-branch1 branch")
		}
		respStat, err := clt.StatObjectWithResponse(ctx, repo, "dest-branch1", &apigen.StatObjectParams{Path: "foo/bar3"})
		verifyResponseOK(t, respStat, err)

		respStat, err = clt.StatObjectWithResponse(ctx, repo, "dest-branch1", &apigen.StatObjectParams{Path: "foo/bar4"})
		verifyResponseOK(t, respStat, err)
	})

	t.Run("from commit", func(t *testing.T) {
		cherryResponse, err := clt.CherryPickWithResponse(ctx, repo, "dest-branch2", apigen.CherryPickJSONRequestBody{Ref: commit2.Reference, ParentNumber: swag.Int(1)})
		verifyResponseOK(t, cherryResponse, err)

		// verify that the cherry-pick worked as expected
		resp, err := clt.StatObjectWithResponse(ctx, repo, "dest-branch2", &apigen.StatObjectParams{Path: "foo/bar2"})
		verifyResponseOK(t, resp, err)

		respStat, err := clt.StatObjectWithResponse(ctx, repo, "dest-branch2", &apigen.StatObjectParams{Path: "foo/bar3"})
		testutil.Must(t, err)
		if respStat.JSON404 == nil {
			t.Error("expected to not find object foo/bar3 in dest-branch2 branch")
		}
	})

	t.Run("invalid parent id (too big)", func(t *testing.T) {
		resp, err := clt.CherryPickWithResponse(ctx, repo, "dest-branch1", apigen.CherryPickJSONRequestBody{Ref: commit2.Reference, ParentNumber: swag.Int(2)})
		testutil.Must(t, err)
		if resp.JSON400 == nil {
			t.Error("expected to get bad request")
		}
	})

	t.Run("invalid parent id (too small)", func(t *testing.T) {
		resp, err := clt.CherryPickWithResponse(ctx, repo, "dest-branch1", apigen.CherryPickJSONRequestBody{Ref: commit2.Reference, ParentNumber: swag.Int(0)})
		testutil.Must(t, err)
		if resp.JSON400 == nil {
			t.Error("expected to get bad request")
		}
	})

	t.Run("dirty branch", func(t *testing.T) {
		// create branch with entry without a commit
		_, err := deps.catalog.CreateBranch(ctx, repo, "dirty", "main")
		testutil.Must(t, err)
		err = deps.catalog.CreateEntry(ctx, repo, "dirty", catalog.DBEntry{Path: "foo/bar5", PhysicalAddress: "bar50addr", CreationDate: time.Now(), Size: 5, Checksum: "cksum5"})
		testutil.Must(t, err)

		cherryPickResp, err := clt.CherryPickWithResponse(ctx, repo, "dirty", apigen.CherryPickJSONRequestBody{Ref: "branch1"})
		testutil.Must(t, err)
		if cherryPickResp.JSON400 == nil || cherryPickResp.JSON400.Message != graveler.ErrDirtyBranch.Error() {
			t.Errorf("Cherry-Pick dirty branch should fail with ErrDirtyBranch, got %+v", cherryPickResp)
		}
	})

	t.Run("from branch - merge commit - first parent", func(t *testing.T) {
		cherryResponse, err := clt.CherryPickWithResponse(ctx, repo, "dest-branch4", apigen.CherryPickJSONRequestBody{Ref: "branch3", ParentNumber: swag.Int(1)})
		verifyResponseOK(t, cherryResponse, err)
		// verify that the cherry-pick worked as expected

		resp, err := clt.StatObjectWithResponse(ctx, repo, "dest-branch4", &apigen.StatObjectParams{Path: "foo/bar2"})
		verifyResponseOK(t, resp, err)

		resp, err = clt.StatObjectWithResponse(ctx, repo, "dest-branch4", &apigen.StatObjectParams{Path: "foo/bar3"})
		verifyResponseOK(t, resp, err)

		resp, err = clt.StatObjectWithResponse(ctx, repo, "dest-branch4", &apigen.StatObjectParams{Path: "foo/bar4"})
		verifyResponseOK(t, resp, err)

		respStat, err := clt.StatObjectWithResponse(ctx, repo, "dest-branch4", &apigen.StatObjectParams{Path: "foo/bar8"})
		testutil.Must(t, err)
		if respStat.JSON404 == nil {
			t.Error("expected to not find object foo/bar8 in dest-branch4 branch")
		}
	})

	t.Run("from branch - merge commit - second parent", func(t *testing.T) {
		cherryResponse, err := clt.CherryPickWithResponse(ctx, repo, "dest-branch3", apigen.CherryPickJSONRequestBody{Ref: "branch3", ParentNumber: swag.Int(2)})
		verifyResponseOK(t, cherryResponse, err)

		// verify that the cherry-pick worked as expected
		resp, err := clt.StatObjectWithResponse(ctx, repo, "dest-branch3", &apigen.StatObjectParams{Path: "foo/bar2"})
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Error("expected to not find object foo/bar2 in dest-branch3 branch")
		}
		resp, err = clt.StatObjectWithResponse(ctx, repo, "dest-branch3", &apigen.StatObjectParams{Path: "foo/bar4"})
		testutil.Must(t, err)
		if resp.JSON404 == nil {
			t.Error("expected to not find object foo/bar6 in dest-branch3 branch")
		}

		resp, err = clt.StatObjectWithResponse(ctx, repo, "dest-branch3", &apigen.StatObjectParams{Path: "foo/bar8"})
		verifyResponseOK(t, resp, err)
	})

	t.Run("invalid parent id (too big)- merge commit", func(t *testing.T) {
		cherryResponse, err := clt.CherryPickWithResponse(ctx, repo, "dest-branch3", apigen.CherryPickJSONRequestBody{Ref: "branch3", ParentNumber: swag.Int(3)})
		testutil.Must(t, err)
		if cherryResponse.JSON400 == nil {
			t.Error("expected to get bad request")
		}
	})

	t.Run("conflict", func(t *testing.T) {
		resp, err := clt.CherryPickWithResponse(ctx, repo, "branch4", apigen.CherryPickJSONRequestBody{Ref: commit2.Reference})
		testutil.Must(t, err)
		if resp.JSON409 == nil {
			t.Error("expected to get a conflict")
		}
	})

	t.Run("read-only repository", func(t *testing.T) {
		readOnlyRepository := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, readOnlyRepository, onBlock(deps, readOnlyRepository), "main", true)
		testutil.Must(t, err)
		for _, name := range []string{"branch1", "dest-branch1"} {
			_, err = deps.catalog.CreateBranch(ctx, readOnlyRepository, name, "main", graveler.WithForce(true))
			testutil.Must(t, err)
		}
		testutil.MustDo(t, "create entry bar2", deps.catalog.CreateEntry(ctx, readOnlyRepository, "branch1", catalog.DBEntry{Path: "foo/bar2", PhysicalAddress: "bar2addr", CreationDate: time.Now(), Size: 2, Checksum: "cksum2"}, graveler.WithForce(true)))
		_, err = deps.catalog.Commit(ctx, readOnlyRepository, "branch1", "message2", DefaultUserID, nil, nil, nil, false, graveler.WithForce(true))
		testutil.Must(t, err)

		cherryResponse, err := clt.CherryPickWithResponse(ctx, readOnlyRepository, "dest-branch1", apigen.CherryPickJSONRequestBody{Ref: "branch1"})
		testutil.Must(t, err)
		if cherryResponse.JSON403 == nil {
			t.Fatalf("expected 403 to get forbidden error, got %d instead", cherryResponse.StatusCode())
		}
		cherryResponse, err = clt.CherryPickWithResponse(ctx, readOnlyRepository, "dest-branch1", apigen.CherryPickJSONRequestBody{Ref: "branch1", Force: swag.Bool(true)})
		verifyResponseOK(t, cherryResponse, err)
	})
}

func TestController_Policy(t *testing.T) {
	clt, _ := setupClientWithAdmin(t)
	ctx := context.Background()
	const policyID = "TestPolicy"

	// test policy
	t.Run("create", func(t *testing.T) {
		resp, err := clt.CreatePolicyWithResponse(ctx, apigen.CreatePolicyJSONRequestBody{
			CreationDate: apiutil.Ptr(time.Now().Unix()),
			Id:           policyID,
			Statement: []apigen.Statement{
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
		require.NoError(t, err)
		require.Equal(t, http.StatusNotImplemented, resp.StatusCode())
	})
	t.Run("update", func(t *testing.T) {
		resp, err := clt.UpdatePolicyWithResponse(ctx, policyID, apigen.UpdatePolicyJSONRequestBody{
			CreationDate: apiutil.Ptr(time.Now().Unix()),
			Id:           policyID,
			Statement: []apigen.Statement{
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
		require.NoError(t, err)
		require.Equal(t, http.StatusNotImplemented, resp.StatusCode())
	})

	t.Run("delete", func(t *testing.T) {
		resp, err := clt.DeletePolicyWithResponse(ctx, policyID)
		testutil.Must(t, err)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotImplemented, resp.StatusCode())
	})
}

func TestController_GetPhysicalAddress(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("physical_address_format", func(t *testing.T) {
		repo := testUniqueRepoName()
		const (
			ns     = "s3://foo-bucket1"
			branch = "main"
		)
		_, err := deps.catalog.CreateRepository(ctx, repo, ns, branch, false)
		if err != nil {
			t.Fatal(err)
		}

		var prevPartitionTime time.Time
		const links = 5
		for i := 0; i < links; i++ {
			params := &apigen.GetPhysicalAddressParams{
				Path: "get-path/obj" + strconv.Itoa(i),
			}
			resp, err := clt.GetPhysicalAddressWithResponse(ctx, repo, branch, params)
			if err != nil {
				t.Fatalf("GetPhysicalAddressWithResponse %s, failed: %s", params.Path, err)
			}
			if resp.JSON200 == nil {
				t.Fatalf("GetPhysicalAddressWithResponse %s, non JSON 200 response: %s", params.Path, resp.Status())
			}

			address := apiutil.Value(resp.JSON200.PhysicalAddress)
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

	verifyPrepareGarbageCollection := func(t *testing.T, repo string, expectedCalls int, expectUncommittedData bool) {
		t.Helper()
		var (
			calls int
			token *string
		)
		for {
			calls++
			resp, err := clt.PrepareGarbageCollectionUncommittedWithResponse(ctx, repo, apigen.PrepareGarbageCollectionUncommittedJSONRequestBody{
				ContinuationToken: token,
			})
			verifyResponseOK(t, resp, err)
			if resp.JSON201 == nil {
				t.Fatalf("PrepareGarbageCollectionUncommitted status code:%d, expected 201", resp.StatusCode())
			}
			if resp.JSON201.RunId == "" {
				t.Errorf("PrepareGarbageCollectionUncommitted empty RunID, value expected")
			}

			if expectUncommittedData {
				if resp.JSON201.GcUncommittedLocation == "" {
					t.Errorf("PrepareGarbageCollectionUncommitted empty GcUncommittedLocation, value expected")
				}
			} else {
				if resp.JSON201.GcUncommittedLocation != "" {
					t.Errorf("PrepareGarbageCollectionUncommitted empty GcUncommittedLocation, no value expected")
				}
			}

			token = resp.JSON201.ContinuationToken
			if token == nil || *token == "" {
				break
			}
		}
		if calls != expectedCalls {
			t.Fatalf("PrepareGarbageCollectionUncommitted calls=%d, expected=%d", calls, expectedCalls)
		}
	}

	t.Run("no_repository", func(t *testing.T) {
		resp, err := clt.PrepareGarbageCollectionUncommittedWithResponse(ctx, "", apigen.PrepareGarbageCollectionUncommittedJSONRequestBody{})
		if err != nil {
			t.Fatalf("PrepareGarbageCollectionUncommitted failed: %s", err)
		}
		if resp.JSON400 == nil {
			t.Fatalf("PrepareGarbageCollectionUncommitted expected BadRequest: %+v", resp)
		}
	})

	t.Run("repository_not_exists", func(t *testing.T) {
		repo := testUniqueRepoName()
		resp, err := clt.PrepareGarbageCollectionUncommittedWithResponse(ctx, repo, apigen.PrepareGarbageCollectionUncommittedJSONRequestBody{})
		if err != nil {
			t.Fatalf("PrepareGarbageCollectionUncommitted failed: %s", err)
		}
		if resp.JSON404 == nil {
			t.Fatalf("PrepareGarbageCollectionUncommitted expected NotFound: %+v", resp)
		}
	})

	t.Run("uncommitted_data", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.Must(t, err)
		const items = 3
		for i := 0; i < items; i++ {
			objPath := fmt.Sprintf("uncommitted/obj%d", i)
			uploadResp, err := uploadObjectHelper(t, ctx, clt, objPath, strings.NewReader(objPath), repo, "main")
			verifyResponseOK(t, uploadResp, err)
		}
		verifyPrepareGarbageCollection(t, repo, 1, true)
	})

	t.Run("committed_data", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.Must(t, err)
		const items = 3
		for i := 0; i < items; i++ {
			objPath := fmt.Sprintf("committed/obj%d", i)
			uploadResp, err := uploadObjectHelper(t, ctx, clt, objPath, strings.NewReader(objPath), repo, "main")
			verifyResponseOK(t, uploadResp, err)
		}
		if _, err := deps.catalog.Commit(ctx, repo, "main", "committed objects", "some_user", nil, nil, nil, false); err != nil {
			t.Fatalf("failed to commit objects: %s", err)
		}
		verifyPrepareGarbageCollection(t, repo, 1, false)
	})

	t.Run("uncommitted_copy", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.Must(t, err)
		const items = 3
		for i := 0; i < items; i++ {
			objPath := fmt.Sprintf("uncommitted/obj%d", i)
			uploadResp, err := uploadObjectHelper(t, ctx, clt, objPath, strings.NewReader(objPath), repo, "main")
			verifyResponseOK(t, uploadResp, err)

			copyResp, err := clt.CopyObjectWithResponse(ctx, repo, "main",
				&apigen.CopyObjectParams{DestPath: fmt.Sprintf("copy/obj%d", i)},
				apigen.CopyObjectJSONRequestBody{
					SrcPath: objPath,
				})
			verifyResponseOK(t, copyResp, err)
		}
		verifyPrepareGarbageCollection(t, repo, 1, true)
	})

	t.Run("read_only_repo", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", true)
		testutil.Must(t, err)
		resp, err := clt.PrepareGarbageCollectionUncommittedWithResponse(ctx, repo, apigen.PrepareGarbageCollectionUncommittedJSONRequestBody{})
		if err != nil {
			t.Fatalf("PrepareGarbageCollectionUncommitted failed: %s", err)
		}
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("PrepareGarbageCollectionUncommitted expected 403 code, got %d instead", resp.StatusCode())
		}
	})
}

func TestController_PrepareGarbageCollectionCommitted(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	t.Run("read_only_repo", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", true)
		testutil.Must(t, err)
		resp, err := clt.PrepareGarbageCollectionCommitsWithResponse(ctx, repo)
		if err != nil {
			t.Fatalf("PrepareGarbageCollectionCommits failed: %s", err)
		}
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("PrepareGarbageCollectionCommits expected 403 code, got %d instead", resp.StatusCode())
		}
	})
}

func TestController_ClientDisconnect(t *testing.T) {
	handler, deps := setupHandler(t)

	// setup lakefs
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)

	// setup repository
	ctx := context.Background()
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
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
	clt = setupClientByEndpoint(t, server.URL, cred.AccessKeyID, cred.SecretAccessKey, apigen.WithHTTPClient(httpClient))

	// upload request
	contentType, reader := writeMultipart("content", "file.data", "something special")
	_, err = clt.UploadObjectWithBodyWithResponse(ctx, repo, "main", &apigen.UploadObjectParams{
		Path: "test/file.data",
	}, contentType, reader)
	if err == nil {
		t.Fatal("Expected to request complete without error, expected to fail")
	}

	// wait for the server to identify we left and update the counter
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
		events              []apigen.StatsEvent
		expectedEventCounts map[key]int
		expectedStatusCode  int
	}{
		{
			name: "single_event_count_1",
			events: []apigen.StatsEvent{
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
			events: []apigen.StatsEvent{
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
			events: []apigen.StatsEvent{
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
			events: []apigen.StatsEvent{
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
			events: []apigen.StatsEvent{
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
			events: []apigen.StatsEvent{
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
			events: []apigen.StatsEvent{
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
			events: []apigen.StatsEvent{
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
			events: []apigen.StatsEvent{
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
			events: []apigen.StatsEvent{
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
			resp, err := clt.PostStatsEventsWithResponse(ctx, apigen.PostStatsEventsJSONRequestBody{
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
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "bucket/prefix"), "main", false)
	require.NoError(t, err)
	_, err = deps.catalog.CreateBranch(ctx, repo, "alt", "main")
	require.NoError(t, err)

	uploadContent := func(t *testing.T, repository, branch, objPath string) apigen.ObjectStats {
		t.Helper()
		const content = "hello world this is my awesome content"
		uploadResp, err := uploadObjectHelper(t, ctx, clt, objPath, strings.NewReader(content), repository, branch)
		verifyResponseOK(t, uploadResp, err)
		require.NotNil(t, uploadResp.JSON201)
		require.Equal(t, len(content), int(apiutil.Value(uploadResp.JSON201.SizeBytes)))
		return *uploadResp.JSON201
	}

	t.Run("same_branch", func(t *testing.T) {
		const (
			srcPath  = "foo/bar"
			destPath = "foo/bar-shallow-copy"
		)
		objStat := uploadContent(t, repo, "main", srcPath)
		copyResp, err := clt.CopyObjectWithResponse(ctx, repo, "main", &apigen.CopyObjectParams{
			DestPath: destPath,
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: srcPath,
		})
		verifyResponseOK(t, copyResp, err)

		// Verify the creation path, date and physical address are different
		copyStat := copyResp.JSON201
		require.NotNil(t, copyStat)
		require.NotEqual(t, objStat.PhysicalAddress, copyStat.PhysicalAddress)
		require.GreaterOrEqual(t, copyStat.Mtime, objStat.Mtime)
		require.Equal(t, destPath, copyStat.Path)

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: destPath})
		verifyResponseOK(t, statResp, err)
		require.Nil(t, deep.Equal(statResp.JSON200, copyStat))
	})

	t.Run("different_branch", func(t *testing.T) {
		const (
			srcPath  = "foo/bar2"
			destPath = "foo/bar-full-from-branch"
		)
		objStat := uploadContent(t, repo, "alt", srcPath)
		copyResp, err := clt.CopyObjectWithResponse(ctx, repo, "main", &apigen.CopyObjectParams{
			DestPath: destPath,
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: srcPath,
			SrcRef:  apiutil.Ptr("alt"),
		})
		verifyResponseOK(t, copyResp, err)

		// Verify the creation path, date and physical address are different
		copyStat := copyResp.JSON201
		require.NotNil(t, copyStat)
		require.NotEmpty(t, copyStat.PhysicalAddress)
		require.NotEqual(t, objStat.PhysicalAddress, copyStat.PhysicalAddress)
		require.GreaterOrEqual(t, copyStat.Mtime, objStat.Mtime)
		require.Equal(t, destPath, copyStat.Path)

		// Verify all else is equal
		objStat.Mtime = copyStat.Mtime
		objStat.Path = copyStat.Path
		objStat.PhysicalAddress = copyStat.PhysicalAddress
		require.Nil(t, deep.Equal(objStat, *copyStat))

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: destPath})
		verifyResponseOK(t, statResp, err)
		require.Nil(t, deep.Equal(statResp.JSON200, copyStat))
	})

	t.Run("committed", func(t *testing.T) {
		const (
			srcPath  = "foo/bar3"
			destPath = "foo/bar-full-committed"
		)
		objStat := uploadContent(t, repo, "main", srcPath)
		commitResp, err := clt.CommitWithResponse(ctx, repo, "main", &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message: "commit bar3",
		})
		verifyResponseOK(t, commitResp, err)
		require.NotNil(t, commitResp.JSON201)

		copyResp, err := clt.CopyObjectWithResponse(ctx, repo, "main", &apigen.CopyObjectParams{
			DestPath: destPath,
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: srcPath,
			SrcRef:  apiutil.Ptr("main"),
		})
		verifyResponseOK(t, copyResp, err)

		// Verify the creation path, date and physical address are different
		copyStat := copyResp.JSON201
		require.NotNil(t, copyStat)
		require.NotEmpty(t, copyStat.PhysicalAddress)
		require.NotEqual(t, objStat.PhysicalAddress, copyStat.PhysicalAddress)
		require.GreaterOrEqual(t, copyStat.Mtime, objStat.Mtime)
		require.Equal(t, destPath, copyStat.Path)

		// Verify all else is equal
		objStat.Mtime = copyStat.Mtime
		objStat.Path = copyStat.Path
		objStat.PhysicalAddress = copyStat.PhysicalAddress
		require.Nil(t, deep.Equal(objStat, *copyStat))

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: destPath})
		verifyResponseOK(t, statResp, err)
		require.Nil(t, deep.Equal(statResp.JSON200, copyStat))
	})

	t.Run("not_found", func(t *testing.T) {
		resp, err := clt.CopyObjectWithResponse(ctx, repo, "main", &apigen.CopyObjectParams{
			DestPath: "bar/foo",
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: "not/found",
			SrcRef:  apiutil.Ptr("main"),
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404)

		// without src ref
		resp, err = clt.CopyObjectWithResponse(ctx, repo, "main", &apigen.CopyObjectParams{
			DestPath: "bar/foo",
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: "not/found",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404)
	})

	t.Run("empty_destination", func(t *testing.T) {
		resp, err := clt.CopyObjectWithResponse(ctx, repo, "main", &apigen.CopyObjectParams{
			DestPath: "",
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: "foo/bar",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON400)
	})

	t.Run("read-only repository", func(t *testing.T) {
		readOnlyRepository := testUniqueRepoName()
		_, err = deps.catalog.CreateRepository(ctx, readOnlyRepository, onBlock(deps, "bucket/prefix"), "main", true)
		require.NoError(t, err)
		_, err = deps.catalog.CreateBranch(ctx, readOnlyRepository, "alt", "main", graveler.WithForce(true))
		require.NoError(t, err)

		const (
			srcPath  = "foo/bar"
			destPath = "foo/bar-shallow-copy"
		)
		uploadContentForce := func(t *testing.T, repository, branch, objPath string) apigen.ObjectStats {
			t.Helper()
			const content = "hello world this is my awesome content"
			var b bytes.Buffer
			w := multipart.NewWriter(&b)
			contentWriter, err := w.CreateFormFile("content", filepath.Base(objPath))
			if err != nil {
				t.Fatal("CreateFormFile:", err)
			}
			if _, err := io.Copy(contentWriter, strings.NewReader(content)); err != nil {
				t.Fatal("CreateFormFile write content:", err)
			}
			if err := w.Close(); err != nil {
				t.Fatal("Close multipart writer:", err)
			}

			uploadResp, err := clt.UploadObjectWithBodyWithResponse(ctx, readOnlyRepository, branch, &apigen.UploadObjectParams{
				Path:  objPath,
				Force: swag.Bool(true),
			}, w.FormDataContentType(), &b)
			verifyResponseOK(t, uploadResp, err)
			require.NotNil(t, uploadResp.JSON201)
			require.Equal(t, len(content), int(apiutil.Value(uploadResp.JSON201.SizeBytes)))
			return *uploadResp.JSON201
		}
		objStat := uploadContentForce(t, readOnlyRepository, "main", srcPath)
		copyResp, err := clt.CopyObjectWithResponse(ctx, readOnlyRepository, "main", &apigen.CopyObjectParams{
			DestPath: destPath,
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: srcPath,
		})
		testutil.Must(t, err)
		if copyResp.StatusCode() != http.StatusForbidden {
			t.Fatalf("expected 403 forbidden for CopyObject on read-only repository, got %d instead", copyResp.StatusCode())
		}
		copyResp, err = clt.CopyObjectWithResponse(ctx, readOnlyRepository, "main", &apigen.CopyObjectParams{
			DestPath: destPath,
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: srcPath,
			Force:   swag.Bool(true),
		})
		verifyResponseOK(t, copyResp, err)

		// Verify the creation path, date and physical address are different
		copyStat := copyResp.JSON201
		require.NotNil(t, copyStat)
		require.NotEqual(t, objStat.PhysicalAddress, copyStat.PhysicalAddress)
		require.GreaterOrEqual(t, copyStat.Mtime, objStat.Mtime)
		require.Equal(t, destPath, copyStat.Path)

		// get back info
		statResp, err := clt.StatObjectWithResponse(ctx, readOnlyRepository, "main", &apigen.StatObjectParams{Path: destPath})
		verifyResponseOK(t, statResp, err)
		require.Nil(t, deep.Equal(statResp.JSON200, copyStat))
	})
}

func TestController_LocalAdapter_StageObject(t *testing.T) {
	p := t.TempDir()
	forbiddenPath := "local:///not_allowed"
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeLocal)
	viper.Set("blockstore.local.path", p)
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "bucket/prefix"), "main", false)
	require.NoError(t, err)
	_, err = deps.catalog.CreateBranch(ctx, repo, "alt", "main")
	require.NoError(t, err)

	t.Run("stage_forbidden_address", func(t *testing.T) {
		resp, err := clt.StageObjectWithResponse(ctx, repo, "main", &apigen.StageObjectParams{
			Path: "some_path",
		}, apigen.StageObjectJSONRequestBody{
			PhysicalAddress: forbiddenPath,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON403)
	})
}

func TestController_BranchProtectionRules(t *testing.T) {
	adminClt, deps := setupClientWithAdmin(t)

	t.Run("admin", func(t *testing.T) {
		currCtx := context.Background()
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(currCtx, repo, onBlock(deps, repo), "main", false)
		testutil.MustDo(t, "create repository", err)

		respPreflight, err := adminClt.CreateBranchProtectionRulePreflightWithResponse(currCtx, repo)
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, respPreflight.StatusCode())

		// the result of an actual call to the endpoint should have the same result
		resp, err := adminClt.InternalCreateBranchProtectionRuleWithResponse(currCtx, repo, apigen.InternalCreateBranchProtectionRuleJSONRequestBody{
			Pattern: "main",
		})
		require.NoError(t, err)
		require.Equal(t, respPreflight.StatusCode(), resp.StatusCode())
	})

	t.Run("read-only repo", func(t *testing.T) {
		currCtx := context.Background()
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(currCtx, repo, onBlock(deps, repo), "main", true)
		testutil.MustDo(t, "create repository", err)

		resp, err := adminClt.SetBranchProtectionRulesWithResponse(currCtx, repo, &apigen.SetBranchProtectionRulesParams{}, apigen.SetBranchProtectionRulesJSONRequestBody{})
		if err != nil {
			t.Fatal(err)
		}
		if resp == nil {
			t.Fatal("SetBranchProtectionRulesWithResponse got no response")
		}
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("SetBranchProtectionRulesWithResponse expected to get 403 status code , got %d", resp.StatusCode())
		}
	})
}

func TestController_GarbageCollectionRules(t *testing.T) {
	adminClt, deps := setupClientWithAdmin(t)

	t.Run("admin", func(t *testing.T) {
		currCtx := context.Background()
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(currCtx, repo, onBlock(deps, repo), "main", false)
		testutil.MustDo(t, "create repository", err)

		respPreflight, err := adminClt.SetGarbageCollectionRulesPreflightWithResponse(currCtx, repo)
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, respPreflight.StatusCode())

		// the result of an actual call to the endpoint should have the same result
		resp, err := adminClt.SetGCRulesWithResponse(currCtx, repo, apigen.SetGCRulesJSONRequestBody{
			Branches: []apigen.GarbageCollectionRule{{BranchId: "main", RetentionDays: 1}}, DefaultRetentionDays: 5,
		})
		require.NoError(t, err)
		require.Equal(t, respPreflight.StatusCode(), resp.StatusCode())
	})

	t.Run("read-only repo", func(t *testing.T) {
		currCtx := context.Background()
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(currCtx, repo, onBlock(deps, repo), "main", true)
		testutil.MustDo(t, "create repository", err)

		resp, err := adminClt.SetGCRulesWithResponse(currCtx, repo, apigen.SetGCRulesJSONRequestBody{
			Branches: []apigen.GarbageCollectionRule{{BranchId: "main", RetentionDays: 1}}, DefaultRetentionDays: 5,
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp == nil {
			t.Fatal("SetGCRulesWithResponse got no response")
		}
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("SetGCRulesWithResponse expected to get 403 status code , got %d", resp.StatusCode())
		}
	})
}

func TestController_DumpRestoreRepository(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()

	// setup repository with some commits
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	testutil.Must(t, err)

	const commits = 3
	for i := 0; i < commits; i++ {
		n := strconv.Itoa(i + 1)
		p := "foo/bar" + n
		err := deps.catalog.CreateEntry(ctx, repo, "main", catalog.DBEntry{Path: p, PhysicalAddress: onBlock(deps, "bar"+n+"addr"), CreationDate: time.Now(), Size: int64(i) + 1, Checksum: "cksum" + n})
		testutil.MustDo(t, "create entry "+p, err)
		_, err = deps.catalog.Commit(ctx, repo, "main", "commit"+n, "tester", nil, nil, nil, false)
		testutil.MustDo(t, "commit "+p, err)
	}

	var dumpStatus *apigen.RepositoryDumpStatus

	t.Run("dump", func(t *testing.T) {
		dumpResp, err := clt.DumpSubmitWithResponse(ctx, repo)
		testutil.MustDo(t, "dump submit", err)
		if dumpResp.JSON202 == nil {
			t.Fatal("Expected 202 response")
		}

		taskID := dumpResp.JSON202.Id
		ticker := time.NewTicker(500 * time.Millisecond)
		started := time.Now()
		for range ticker.C {
			statusResp, err := clt.DumpStatusWithResponse(ctx, repo, &apigen.DumpStatusParams{TaskId: taskID})
			testutil.MustDo(t, "dump status", err)
			if statusResp.JSON200 == nil {
				t.Fatal("Expected 200 response")
			}
			if statusResp.JSON200.Done {
				dumpStatus = statusResp.JSON200
				break
			}
			if time.Since(started) > 30*time.Second {
				break
			}
		}
		ticker.Stop()

		if dumpStatus == nil {
			t.Fatal("Expected dump to complete (timed-out)")
		}
		if dumpStatus.Error != nil {
			t.Fatalf("Failed to dump repository refs: %s", *dumpStatus.Error)
		}
	})

	t.Run("dump_status_invalid_id", func(t *testing.T) {
		response, err := clt.DumpStatusWithResponse(ctx, repo, &apigen.DumpStatusParams{TaskId: "invalid"})
		testutil.MustDo(t, "dump status", err)
		if response.JSON404 == nil {
			t.Fatalf("Expected 404 (not found) response, got %s", response.Status())
		}
	})

	t.Run("restore", func(t *testing.T) {
		if dumpStatus == nil || dumpStatus.Refs == nil {
			t.Skip("Skipping restore test, dump failed")
		}

		newRepo := testUniqueRepoName()
		_, err = deps.catalog.CreateBareRepository(ctx, newRepo, onBlock(deps, repo), "main", false)
		testutil.MustDo(t, "create bare repository", err)

		submitResponse, err := clt.RestoreSubmitWithResponse(ctx, newRepo, apigen.RestoreSubmitJSONRequestBody{
			BranchesMetaRangeId: dumpStatus.Refs.BranchesMetaRangeId,
			CommitsMetaRangeId:  dumpStatus.Refs.CommitsMetaRangeId,
			TagsMetaRangeId:     dumpStatus.Refs.TagsMetaRangeId,
		})
		testutil.MustDo(t, "restore submit", err)
		if submitResponse.JSON202 == nil {
			t.Fatalf("Expected 202 response, got: %s", submitResponse.Status())
		}

		restoreStatus := pollRestoreStatus(t, clt, newRepo, submitResponse.JSON202.Id)
		if restoreStatus == nil {
			t.Fatal("Expected restore to complete (timed-out)")
		}
		if restoreStatus.Error != nil {
			t.Fatalf("Failed to restore repository refs: %s", *restoreStatus.Error)
		}
	})

	t.Run("restore_invalid_refs", func(t *testing.T) {
		// delete and recreate repository as bare for restore
		newRepo := testUniqueRepoName()
		_, err = deps.catalog.CreateBareRepository(ctx, newRepo, onBlock(deps, repo), "main", false)
		testutil.MustDo(t, "create bare repository", err)

		submitResponse, err := clt.RestoreSubmitWithResponse(ctx, newRepo, apigen.RestoreSubmitJSONRequestBody{
			BranchesMetaRangeId: "invalid",
			CommitsMetaRangeId:  "invalid",
			TagsMetaRangeId:     "invalid",
		})
		testutil.MustDo(t, "restore submit", err)
		if submitResponse.JSON202 == nil {
			t.Fatalf("Expected 202 response, got: %s", submitResponse.Status())
		}

		restoreStatus := pollRestoreStatus(t, clt, newRepo, submitResponse.JSON202.Id)
		if restoreStatus == nil {
			t.Fatal("Expected restore to complete (timed-out)")
		}
		if restoreStatus.Error == nil {
			t.Fatal("Expected restore to fail, got nil Error")
		}
		if !strings.Contains(*restoreStatus.Error, graveler.ErrNotFound.Error()) {
			t.Fatal("Expected restore to fail with not found error")
		}
	})

	t.Run("restore_status_invalid_id", func(t *testing.T) {
		response, err := clt.RestoreStatusWithResponse(ctx, repo, &apigen.RestoreStatusParams{TaskId: "invalid"})
		testutil.MustDo(t, "restore status", err)
		if response.JSON404 == nil {
			t.Fatalf("Expected 404 (not found) response, got %s", response.Status())
		}
	})
}

func TestController_CreateCommitRecord(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	// expected commit ID for this commit record
	expectedCommitID := "0c5f9d6fde638a6aa82840caf2485bdbe6ad0fc3f220ea88884df9ed99d7cf19"
	body := apigen.CreateCommitRecordJSONRequestBody{
		CommitId:     expectedCommitID,
		Committer:    "Committer",
		CreationDate: 1e9,
		Generation:   1,
		Message:      "message",
		Metadata:     &apigen.CommitRecordCreation_Metadata{AdditionalProperties: map[string]string{"key": "value"}},
		MetarangeId:  "metarangeId",
		Parents:      []string{"parent1", "parent2"},
		Version:      1,
	}

	t.Run("create commit record", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.Must(t, err)
		resp, err := clt.CreateCommitRecordWithResponse(ctx, repo, body)
		testutil.MustDo(t, "create commit record", err)
		if resp.StatusCode() != http.StatusNoContent {
			t.Fatalf("Expected 204 (no content) response, got %s", resp.Status())
		}
		commitLog, err := deps.catalog.GetCommit(ctx, repo, expectedCommitID)
		testutil.MustDo(t, "get commit", err)
		expectedCommitLog := &catalog.CommitLog{
			Reference:    expectedCommitID,
			Committer:    body.Committer,
			Message:      body.Message,
			CreationDate: time.Unix(body.CreationDate, 0).UTC(),
			Metadata:     body.Metadata.AdditionalProperties,
			MetaRangeID:  body.MetarangeId,
			Parents:      body.Parents,
			Generation:   catalog.CommitGeneration(body.Generation),
			Version:      catalog.CommitVersion(body.Version),
		}
		if diff := deep.Equal(commitLog, expectedCommitLog); diff != nil {
			t.Fatalf("Diff: %v", diff)
		}
	})

	t.Run("create commit record with wrong commitID", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.Must(t, err)
		bodyCpy := body
		bodyCpy.CommitId = "wrong"
		resp, err := clt.CreateCommitRecordWithResponse(ctx, repo, bodyCpy)
		testutil.MustDo(t, "create commit record", err)
		if resp.StatusCode() != http.StatusBadRequest {
			t.Fatalf("Expected 400 (bad request) response, got %s", resp.Status())
		}
	})

	t.Run("read only repository", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", true)
		testutil.Must(t, err)
		resp, err := clt.CreateCommitRecordWithResponse(ctx, repo, body)
		testutil.Must(t, err)
		if resp.StatusCode() != http.StatusForbidden {
			t.Fatalf("Create commit record in read-only repository should be forbidden (403), got %s", resp.Status())
		}
		body.Force = swag.Bool(true)
		resp, err = clt.CreateCommitRecordWithResponse(ctx, repo, body)
		testutil.MustDo(t, "create commit record", err)
		if resp.StatusCode() != http.StatusNoContent {
			t.Fatalf("Expected 204 response, got: %s", resp.Status())
		}
	})

	t.Run("already existing commit", func(t *testing.T) {
		repo := testUniqueRepoName()
		_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
		testutil.Must(t, err)
		resp, err := clt.CreateCommitRecordWithResponse(ctx, repo, body)
		testutil.MustDo(t, "create commit record", err)
		if resp.StatusCode() != http.StatusNoContent {
			t.Fatalf("Expected 204 (no content) response, got %s", resp.Status())
		}
		resp, err = clt.CreateCommitRecordWithResponse(ctx, repo, body)
		if resp.StatusCode() != http.StatusConflict {
			t.Fatalf("Expected 409 (conflict) response, got %s", resp.Status())
		}
	})
}

func TestController_CreatePullRequest(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	require.NoError(t, err)

	t.Run("invalid source", func(t *testing.T) {
		resp, err := clt.CreatePullRequestWithResponse(ctx, repo, apigen.CreatePullRequestJSONRequestBody{
			Description:       "My description",
			DestinationBranch: "branch_a",
			SourceBranch:      "bad$name",
			Title:             "My title",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, "src")
	})

	t.Run("invalid dest", func(t *testing.T) {
		resp, err := clt.CreatePullRequestWithResponse(ctx, repo, apigen.CreatePullRequestJSONRequestBody{
			Description:       "My description",
			DestinationBranch: "bad$name",
			SourceBranch:      "branch_a",
			Title:             "My title",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, "dest")
	})

	t.Run("no source", func(t *testing.T) {
		resp, err := clt.CreatePullRequestWithResponse(ctx, repo, apigen.CreatePullRequestJSONRequestBody{
			Description:       "My description",
			DestinationBranch: "main",
			SourceBranch:      "branch_a",
			Title:             "My title",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404)
		require.Contains(t, resp.JSON404.Message, "branch not found")
	})

	t.Run("no dest", func(t *testing.T) {
		resp, err := clt.CreatePullRequestWithResponse(ctx, repo, apigen.CreatePullRequestJSONRequestBody{
			Description:       "My description",
			DestinationBranch: "branch_a",
			SourceBranch:      "main",
			Title:             "My title",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404)
		require.Contains(t, resp.JSON404.Message, "branch not found")
	})

	t.Run("same branch", func(t *testing.T) {
		resp, err := clt.CreatePullRequestWithResponse(ctx, repo, apigen.CreatePullRequestJSONRequestBody{
			Description:       "My description",
			DestinationBranch: "main",
			SourceBranch:      "main",
			Title:             "My title",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON400)
		require.Contains(t, resp.JSON400.Message, "same branch")
	})

	t.Run("create sanity", func(t *testing.T) {
		body := apigen.CreatePullRequestJSONRequestBody{
			Description:       "My description",
			DestinationBranch: "main",
			SourceBranch:      "branch_b",
			Title:             "My title",
		}
		branchResp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
			Name:   body.SourceBranch,
			Source: "main",
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, branchResp.StatusCode())

		resp, err := clt.CreatePullRequestWithResponse(ctx, repo, body)
		require.NoError(t, err)
		require.NotNil(t, resp.JSON201)

		// Get pull request
		getResp, err := clt.GetPullRequestWithResponse(ctx, repo, resp.JSON201.Id)
		require.NoError(t, err)
		require.NotNil(t, getResp.JSON200)
		require.Equal(t, body.Title, swag.StringValue(getResp.JSON200.Title))
		require.Equal(t, body.Description, swag.StringValue(getResp.JSON200.Description))
		require.Equal(t, body.SourceBranch, getResp.JSON200.SourceBranch)
		require.Equal(t, body.DestinationBranch, getResp.JSON200.DestinationBranch)
		userResp, err := clt.GetCurrentUserWithResponse(ctx)
		require.NoError(t, err)
		require.NotNil(t, userResp.JSON200)
		require.Equal(t, userResp.JSON200.User.Id, getResp.JSON200.Author)
		require.Equal(t, "open", swag.StringValue(getResp.JSON200.Status))
		require.Equal(t, "", swag.StringValue(getResp.JSON200.MergedCommitId))
		require.True(t, time.Now().Sub(getResp.JSON200.CreationDate) < 1*time.Minute)
	})
}

func TestController_GetPullRequest(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	require.NoError(t, err)

	t.Run("invalid xid", func(t *testing.T) {
		resp, err := clt.GetPullRequestWithResponse(ctx, repo, "invalid-request-id")
		require.NoError(t, err)
		require.NotNil(t, resp.JSON400)
	})

	t.Run("not found", func(t *testing.T) {
		id := xid.New()
		resp, err := clt.GetPullRequestWithResponse(ctx, repo, id.String())
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404, resp.Status())
	})
}

func TestController_ListPullRequestsHandler(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, "foo1"), "main", false)
	require.NoError(t, err)

	t.Run("no pull requests", func(t *testing.T) {
		resp, err := clt.ListPullRequestsWithResponse(ctx, repo, &apigen.ListPullRequestsParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(-1)),
		})
		verifyResponseOK(t, resp, err)
		require.Equal(t, 0, len(resp.JSON200.Results))
	})

	t.Run("repo doesnt exist", func(t *testing.T) {
		resp, err := clt.ListPullRequestsWithResponse(ctx, "repo666", &apigen.ListPullRequestsParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(2)),
		})
		testutil.Must(t, err)
		require.NotNil(t, resp.JSON404)
	})

	t.Run("with pull requests", func(t *testing.T) {
		// Create Data
		expected := make([]catalog.PullRequest, 100)
		for i := range expected {
			expected[i].Title = fmt.Sprintf("pull_%d", i)
			expected[i].DestinationBranch = "main"
			expected[i].SourceBranch = fmt.Sprintf("src_%d", i)
			expected[i].Description = fmt.Sprintf("description_%d", i)
			expected[i].Status = "open"

			branchResp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
				Name:   expected[i].SourceBranch,
				Source: "main",
			})
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, branchResp.StatusCode())

			resp, err := clt.CreatePullRequestWithResponse(ctx, repo, apigen.CreatePullRequestJSONRequestBody{
				Description:       expected[i].Description,
				DestinationBranch: expected[i].DestinationBranch,
				SourceBranch:      expected[i].SourceBranch,
				Title:             expected[i].Title,
			})
			require.NoError(t, err)
			require.NotNil(t, resp.JSON201)
			expected[i].ID = resp.JSON201.Id
		}
		// Sort by p.ID
		sort.Slice(expected, func(i, j int) bool {
			return expected[i].ID < expected[j].ID
		})

		// list 10
		amount := 10
		resp, err := clt.ListPullRequestsWithResponse(ctx, repo, &apigen.ListPullRequestsParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(amount)),
		})
		verifyResponseOK(t, resp, err)
		require.Equal(t, amount, len(resp.JSON200.Results))
		require.True(t, resp.JSON200.Pagination.HasMore)
		require.Equal(t, expected[amount-1].ID, resp.JSON200.Pagination.NextOffset)

		for i := range amount {
			require.Equal(t, expected[i].ID, resp.JSON200.Results[i].Id)
		}

		// List all
		resp, err = clt.ListPullRequestsWithResponse(ctx, repo, &apigen.ListPullRequestsParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(-1)),
		})
		verifyResponseOK(t, resp, err)

		require.Equal(t, len(expected), len(resp.JSON200.Results))
		userResp, err := clt.GetCurrentUserWithResponse(ctx)
		require.NoError(t, err)
		require.NotNil(t, userResp.JSON200)

		for i, p := range resp.JSON200.Results {
			require.Equal(t, expected[i].ID, p.Id)
			require.Equal(t, expected[i].Title, swag.StringValue(p.Title))
			require.Equal(t, userResp.JSON200.User.Id, p.Author)
			require.Equal(t, expected[i].SourceBranch, p.SourceBranch)
			require.Equal(t, expected[i].DestinationBranch, p.DestinationBranch)
			require.Equal(t, expected[i].Status, swag.StringValue(p.Status))
		}

		require.False(t, resp.JSON200.Pagination.HasMore)
		require.Empty(t, resp.JSON200.Pagination.NextOffset)

		// Test out of bounds
		after := expected[len(expected)-1].ID
		resp, err = clt.ListPullRequestsWithResponse(ctx, repo, &apigen.ListPullRequestsParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(-1)),
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
		})
		verifyResponseOK(t, resp, err)
		require.Equal(t, 0, len(resp.JSON200.Results))

		// Test Pagination
		afterInt := 35
		after = expected[afterInt].ID
		amount = 2
		resp, err = clt.ListPullRequestsWithResponse(ctx, repo, &apigen.ListPullRequestsParams{
			After:  apiutil.Ptr[apigen.PaginationAfter](apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr[apigen.PaginationAmount](apigen.PaginationAmount(amount)),
		})
		verifyResponseOK(t, resp, err)
		require.NotNil(t, resp.JSON200)
		require.Equal(t, amount, len(resp.JSON200.Results))
		require.True(t, resp.JSON200.Pagination.HasMore)
		require.Equal(t, expected[afterInt+amount].ID, resp.JSON200.Pagination.NextOffset)
		for i := range amount {
			require.Equal(t, expected[afterInt+i+1].ID, resp.JSON200.Results[i].Id)
		}

		// TODO (niro): Add tests for open / closed after we implement update
		status := "closed"
		resp, err = clt.ListPullRequestsWithResponse(ctx, repo, &apigen.ListPullRequestsParams{
			Status: &status,
		})
		verifyResponseOK(t, resp, err)
		require.Equal(t, 0, len(resp.JSON200.Results))

		status = "open"
		resp, err = clt.ListPullRequestsWithResponse(ctx, repo, &apigen.ListPullRequestsParams{
			Status: &status,
		})
		verifyResponseOK(t, resp, err)
		require.Equal(t, len(expected), len(resp.JSON200.Results))
	})
}

func TestController_UpdatePullRequest(t *testing.T) {
	clt, deps := setupClientWithAdmin(t)
	ctx := context.Background()
	repo := testUniqueRepoName()
	_, err := deps.catalog.CreateRepository(ctx, repo, onBlock(deps, repo), "main", false)
	require.NoError(t, err)

	t.Run("invalid xid", func(t *testing.T) {
		resp, err := clt.UpdatePullRequestWithResponse(ctx, repo, "invalid-request-id", apigen.UpdatePullRequestJSONRequestBody{
			Description: swag.String("description"),
			Status:      swag.String("open"),
			Title:       swag.String("title"),
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON400)
	})

	t.Run("not found", func(t *testing.T) {
		id := xid.New()
		resp, err := clt.UpdatePullRequestWithResponse(ctx, repo, id.String(), apigen.UpdatePullRequestJSONRequestBody{
			Description: swag.String("description"),
			Status:      swag.String("open"),
			Title:       swag.String("title"),
		})
		require.NoError(t, err)
		require.NotNil(t, resp.JSON404, resp.Status())
	})

	t.Run("exists", func(t *testing.T) {
		body := apigen.CreatePullRequestJSONRequestBody{
			Description:       "My description",
			DestinationBranch: "main",
			SourceBranch:      "branch_b",
			Title:             "My title",
		}
		branchResp, err := clt.CreateBranchWithResponse(ctx, repo, apigen.CreateBranchJSONRequestBody{
			Name:   body.SourceBranch,
			Source: "main",
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, branchResp.StatusCode())

		resp, err := clt.CreatePullRequestWithResponse(ctx, repo, body)
		require.NoError(t, err)
		require.NotNil(t, resp.JSON201)

		// Update with wrong status
		updateResp, err := clt.UpdatePullRequestWithResponse(ctx, repo, resp.JSON201.Id, apigen.UpdatePullRequestJSONRequestBody{
			Description: swag.String("description"),
			Status:      swag.String("invalid"),
			Title:       swag.String("title"),
		})
		require.NoError(t, err)
		require.NotNil(t, updateResp.JSON400, updateResp.Status())

		// Get pull request
		getResp, err := clt.GetPullRequestWithResponse(ctx, repo, resp.JSON201.Id)
		require.NoError(t, err)
		require.NotNil(t, getResp.JSON200)

		pr := getResp.JSON200

		// Partial update
		expected := &apigen.PullRequest{
			PullRequestBasic: apigen.PullRequestBasic{
				Description: swag.String("new description"),
				Status:      pr.Status,
				Title:       pr.Title,
			},
			Author:            pr.Author,
			CreationDate:      pr.CreationDate,
			DestinationBranch: pr.DestinationBranch,
			Id:                pr.Id,
			MergedCommitId:    pr.MergedCommitId,
			SourceBranch:      pr.SourceBranch,
		}
		updateResp, err = clt.UpdatePullRequestWithResponse(ctx, repo, resp.JSON201.Id, apigen.UpdatePullRequestJSONRequestBody{
			Description: expected.Description,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, updateResp.StatusCode())

		// Verify update
		getResp, err = clt.GetPullRequestWithResponse(ctx, repo, resp.JSON201.Id)
		require.NoError(t, err)
		require.NotNil(t, getResp.JSON200)
		if diff := deep.Equal(expected, getResp.JSON200); diff != nil {
			t.Error("updated value not as expected", diff)
		}

		// Update all
		expected.Description = swag.String("Other description")
		expected.Title = swag.String("New title")
		expected.Status = swag.String("closed")
		updateResp, err = clt.UpdatePullRequestWithResponse(ctx, repo, resp.JSON201.Id, apigen.UpdatePullRequestJSONRequestBody{
			Description: expected.Description,
			Status:      expected.Status,
			Title:       expected.Title,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, updateResp.StatusCode(), string(updateResp.Body))

		// Verify update
		getResp, err = clt.GetPullRequestWithResponse(ctx, repo, resp.JSON201.Id)
		require.NoError(t, err)
		require.NotNil(t, getResp.JSON200)
		if diff := deep.Equal(expected, getResp.JSON200); diff != nil {
			t.Error("updated value not as expected", diff)
		}
	})
}

// pollRestoreStatus polls the restore status endpoint until the restore is complete or times out.
// test will fail in case of error.
// will return nil in case of timeout.
func pollRestoreStatus(t *testing.T, clt apigen.ClientWithResponsesInterface, repo string, taskID string) *apigen.RepositoryRestoreStatus {
	t.Helper()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	started := time.Now()
	for range ticker.C {
		statusResponse, err := clt.RestoreStatusWithResponse(context.Background(), repo, &apigen.RestoreStatusParams{TaskId: taskID})
		testutil.MustDo(t, "restore status", err)
		if statusResponse.JSON200 == nil {
			t.Fatalf("Expected 200 response, got: %s", statusResponse.Status())
		}
		if statusResponse.JSON200.Done {
			return statusResponse.JSON200
		}
		if time.Since(started) > 30*time.Second {
			break
		}
	}
	return nil
}
