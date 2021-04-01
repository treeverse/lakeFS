package nessie

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
)

func TestSanityAPI(t *testing.T) {
	ctx, log, repo := setupTest(t)

	log.Debug("list entries")
	entries := listRepositoryObjects(ctx, t, repo, masterBranch)
	require.Len(t, entries, 0, "expected no entries")

	log.Debug("upload some files")
	const numOfFiles = 5
	paths := make([]string, numOfFiles)
	contents := make([]string, numOfFiles)
	for i := 0; i < numOfFiles; i++ {
		paths[i] = fmt.Sprintf("file%d", i)
		_, contents[i] = uploadFileRandomData(ctx, t, repo, masterBranch, paths[i], true)
	}

	log.Debug("verify upload content")
	for i, p := range paths {
		resp, err := client.GetObjectWithResponse(ctx, repo, masterBranch, &api.GetObjectParams{Path: p})
		require.NoError(t, err, "get object for", p)
		require.Equal(t, http.StatusOK, resp.StatusCode())
		content := string(resp.Body)
		require.Equal(t, contents[i], content, "content should be the same", p)
	}

	log.Debug("list uncommitted files")
	entries = listRepositoryObjects(ctx, t, repo, masterBranch)
	require.Len(t, entries, numOfFiles, "repository should have files")

	log.Debug("commit changes")
	commitResp, err := client.CommitWithResponse(ctx, repo, masterBranch, api.CommitJSONRequestBody{
		Message: "first commit",
	})
	require.NoError(t, err, "initial commit")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	log.Debug("list files on master")
	entries = listRepositoryObjects(ctx, t, repo, masterBranch)
	require.Len(t, entries, numOfFiles, "repository should have files")

	log.Debug("create 'branch1' based on 'master'")
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   "branch1",
		Source: masterBranch,
	})
	require.NoError(t, err, "failed to create branch1 from master")
	require.Equal(t, http.StatusCreated, createBranchResp.StatusCode())
	branchRef := string(createBranchResp.Body)
	require.NotEmpty(t, branchRef, "reference to new branch")

	log.Debug("list branches")
	branchesResp, err := client.ListBranchesWithResponse(ctx, repo, &api.ListBranchesParams{})
	require.NoError(t, err, "list branches")
	require.Equal(t, http.StatusOK, branchesResp.StatusCode())

	payload := branchesResp.JSON200
	var branches []string
	for _, ref := range payload.Results {
		branch := ref.Id
		commitID := ref.CommitId
		require.NotEmpty(t, commitID, "branch should have commit ID")
		require.NotEqual(t, branch, commitID, "commit ID should not be the branch name")
		// collect the branch names
		branches = append(branches, branch)
	}
	require.ElementsMatch(t, branches, []string{masterBranch, "branch1"},
		"match existing branches")

	log.Debug("branch1 - change file0")
	_, _ = uploadFileRandomData(ctx, t, repo, "branch1", "file0", true)

	log.Debug("branch1 - delete file1")
	deleteResp, err := client.DeleteObjectWithResponse(ctx, repo, "branch1", &api.DeleteObjectParams{Path: "file1"})
	require.NoError(t, err, "delete object")
	require.Equal(t, http.StatusNoContent, deleteResp.StatusCode())

	log.Debug("branch1 - add fileX")
	_, _ = uploadFileRandomData(ctx, t, repo, "branch1", "fileX", true)

	log.Debug("master - list files")
	masterObjects := listRepositoryObjects(ctx, t, repo, "master")
	masterPaths := make([]string, len(masterObjects))
	for i, obj := range masterObjects {
		masterPaths[i] = obj.Path
	}
	require.EqualValues(t, masterPaths, paths)

	log.Debug("branch1 - list objects")
	branch1Objects := listRepositoryObjects(ctx, t, repo, "branch1")
	for i := range branch1Objects {
		masterPaths[i] = branch1Objects[i].Path
	}
	pathsBranch1 := make([]string, len(paths))
	copy(pathsBranch1, paths)
	pathsBranch1 = append(append(paths[:1], paths[2:]...), "fileX")
	require.EqualValues(t, pathsBranch1, masterPaths)

	log.Debug("branch1 - diff changes with master")
	diffResp, err := client.DiffRefsWithResponse(ctx, repo, "branch1", masterBranch, &api.DiffRefsParams{})
	require.NoError(t, err, "diff between branch1 and master")
	require.Equal(t, http.StatusOK, diffResp.StatusCode())
	require.Len(t, diffResp.JSON200.Results, 0, "no changes should be found as we didn't commit anything")

	log.Debug("branch1 - commit changes")
	commitResp, err = client.CommitWithResponse(ctx, repo, "branch1", api.CommitJSONRequestBody{
		Message: "3 changes",
	})
	require.NoError(t, err, "commit 3 changes")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	log.Debug("branch1 - diff changes with master")
	diffResp, err = client.DiffRefsWithResponse(ctx, repo, "branch1", masterBranch, &api.DiffRefsParams{
		Amount: api.PaginationAmountPtr(-1),
	})
	require.NoError(t, err, "diff between branch1 and master")
	require.Equal(t, http.StatusOK, diffResp.StatusCode())
	require.ElementsMatch(t, diffResp.JSON200.Results, []api.Diff{
		{Path: "file0", PathType: "object", Type: "changed"},
		{Path: "file1", PathType: "object", Type: "removed"},
		{Path: "fileX", PathType: "object", Type: "added"},
	})

	log.Debug("branch1 - merge changes to master")
	mergeResp, err := client.MergeIntoBranchWithResponse(ctx, repo, "branch1", masterBranch, api.MergeIntoBranchJSONRequestBody{})
	require.NoError(t, err, "merge branch1 to master")
	require.Equal(t, http.StatusOK, mergeResp.StatusCode())
	require.NotEmpty(t, mergeResp.JSON200.Reference, "merge should return a commit reference")

	log.Debug("branch1 - diff after merge")
	diffResp, err = client.DiffRefsWithResponse(ctx, repo, "branch1", masterBranch, &api.DiffRefsParams{})
	require.NoError(t, err, "diff between branch1 and master")
	require.Equal(t, http.StatusOK, diffResp.StatusCode())
	require.Len(t, diffResp.JSON200.Results, 0, "no diff between branch1 and master")

	log.Debug("master - diff with branch1")
	diffResp, err = client.DiffRefsWithResponse(ctx, repo, masterBranch, "branch1", &api.DiffRefsParams{})
	require.NoError(t, err, "diff between master and branch1")
	require.Equal(t, http.StatusOK, diffResp.StatusCode())
	require.Len(t, diffResp.JSON200.Results, 0, "no diff between master and branch1")

	log.Debug("delete test repository")
	deleteRepoResp, err := client.DeleteRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "failed to delete repository")
	require.Equal(t, http.StatusNoContent, deleteRepoResp.StatusCode())
}
