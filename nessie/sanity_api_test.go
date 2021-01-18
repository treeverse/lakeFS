package nessie

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/client/refs"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/api/gen/models"
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
		_, contents[i] = uploadFileRandomData(ctx, t, repo, masterBranch, paths[i])
	}

	log.Debug("verify upload content")
	for i, p := range paths {
		var buf bytes.Buffer
		_, err := client.Objects.GetObject(objects.NewGetObjectParamsWithContext(ctx).
			WithRepository(repo).
			WithRef(masterBranch).
			WithPath(p), nil, &buf)
		require.NoError(t, err, "get object for", p)
		content := buf.String()
		require.Equal(t, contents[i], content, "content should be the same", p)
	}

	log.Debug("list uncommitted files")
	entries = listRepositoryObjects(ctx, t, repo, masterBranch)
	require.Len(t, entries, numOfFiles, "repository should have files")

	log.Debug("commit changes")
	_, err := client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithCommit(&models.CommitCreation{
		Message: swag.String("first commit"),
	}), nil)
	require.NoError(t, err, "initial commit")

	log.Debug("list committed files on master")
	entries = listRepositoryObjects(ctx, t, repo, masterBranch+":HEAD")
	require.Len(t, entries, numOfFiles, "repository should have files")

	log.Debug("create 'branch1' based on 'master'")
	ref, err := client.Branches.CreateBranch(
		branches.NewCreateBranchParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(&models.BranchCreation{
				Name:   swag.String("branch1"),
				Source: swag.String(masterBranch),
			}), nil)
	require.NoError(t, err, "failed to create branch1 from master")
	require.NotEmpty(t, ref, "reference to new branch")

	log.Debug("list branches")
	branchesResp, err := client.Branches.ListBranches(
		branches.NewListBranchesParamsWithContext(ctx).WithRepository(repo),
		nil)
	require.NoError(t, err, "list branches")

	var branches []string
	for _, ref := range branchesResp.Payload.Results {
		branch := swag.StringValue(ref.ID)
		commitID := swag.StringValue(ref.CommitID)
		require.NotEmpty(t, commitID, "branch should have commit ID")
		require.NotEqual(t, branch, commitID, "commit ID should not be the branch name")
		// collect the branch names
		branches = append(branches, branch)
	}
	require.ElementsMatch(t, branches, []string{masterBranch, "import-from-inventory", "branch1"},
		"match existing branches")

	log.Debug("branch1 - change file0")
	_, _ = uploadFileRandomData(ctx, t, repo, "branch1", "file0")

	log.Debug("branch1 - delete file1")
	_, err = client.Objects.DeleteObject(objects.NewDeleteObjectParamsWithContext(ctx).WithRepository(repo).WithBranch("branch1").WithPath("file1"), nil)
	require.NoError(t, err, "delete object")

	log.Debug("branch1 - add fileX")
	_, _ = uploadFileRandomData(ctx, t, repo, "branch1", "fileX")

	log.Debug("master - list committed files")
	masterObjects := listRepositoryObjects(ctx, t, repo, "master:HEAD")
	masterPaths := make([]string, len(masterObjects))
	for i := range masterObjects {
		masterPaths[i] = masterObjects[i].Path
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
	diffResp, err := client.Refs.DiffRefs(refs.NewDiffRefsParamsWithContext(ctx).
		WithRepository(repo).
		WithLeftRef("branch1").
		WithRightRef(masterBranch), nil)
	require.NoError(t, err, "diff between branch1 and master")
	require.Len(t, diffResp.Payload.Results, 0, "no changes should be found as we didn't commit anything")

	log.Debug("branch1 - commit changes")
	_, err = client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch("branch1").WithCommit(&models.CommitCreation{
		Message: swag.String("3 changes"),
	}), nil)
	require.NoError(t, err, "commit 3 changes")

	log.Debug("branch1 - diff changes with master")
	diffResp, err = client.Refs.DiffRefs(refs.NewDiffRefsParamsWithContext(ctx).
		WithRepository(repo).
		WithLeftRef("branch1").
		WithRightRef(masterBranch).
		WithAmount(swag.Int64(-1)), nil)
	require.NoError(t, err, "diff between branch1 and master")
	require.ElementsMatch(t, diffResp.Payload.Results, []*models.Diff{
		{Path: "file0", PathType: "object", Type: "changed"},
		{Path: "file1", PathType: "object", Type: "removed"},
		{Path: "fileX", PathType: "object", Type: "added"},
	})

	log.Debug("branch1 - merge changes to master")
	mergeResp, err := client.Refs.MergeIntoBranch(refs.NewMergeIntoBranchParamsWithContext(ctx).
		WithRepository(repo).
		WithSourceRef("branch1").
		WithDestinationRef(masterBranch), nil)
	require.NoError(t, err, "merge branch1 to master")
	require.NotEmpty(t, mergeResp.Payload.Reference, "merge should return a commit reference")
	require.Equal(t, mergeResp.Payload.Summary, &models.MergeResultSummary{Added: 1, Changed: 1, Conflict: 0, Removed: 1}, "merge summary")

	log.Debug("branch1 - diff after merge")
	diffResp, err = client.Refs.DiffRefs(refs.NewDiffRefsParamsWithContext(ctx).
		WithRepository(repo).
		WithLeftRef("branch1").
		WithRightRef(masterBranch), nil)
	require.NoError(t, err, "diff between branch1 and master")
	require.Len(t, diffResp.Payload.Results, 0, "no diff between branch1 and master")

	log.Debug("master - diff with branch1")
	diffResp, err = client.Refs.DiffRefs(refs.NewDiffRefsParamsWithContext(ctx).
		WithRepository(repo).
		WithLeftRef(masterBranch).
		WithRightRef("branch1"), nil)
	require.NoError(t, err, "diff between master and branch1")
	require.Len(t, diffResp.Payload.Results, 0, "no diff between master and branch1")

	log.Debug("delete test repository")
	_, err = client.Repositories.DeleteRepository(
		repositories.NewDeleteRepositoryParamsWithContext(ctx).WithRepository(repo), nil)
	require.NoError(t, err, "failed to delete repository")
}
