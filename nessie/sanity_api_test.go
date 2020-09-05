package nessie

import (
	"bytes"
	"context"
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
	ctx := context.Background()
	// create repository
	repo := createRepositoryUnique(ctx, t)

	// list entries - should be none
	entries := listRepositoryObjects(ctx, t, repo, MasterBranch)
	require.Len(t, entries, 0, "expected no entries")

	// upload some files
	const numOfFiles = 5
	paths := make([]string, numOfFiles)
	contents := make([]string, numOfFiles)
	for i := 0; i < numOfFiles; i++ {
		paths[i] = fmt.Sprintf("file%d", i)
		_, contents[i] = uploadFileRandomData(ctx, t, repo, MasterBranch, paths[i])
	}

	// verify upload content
	for i, p := range paths {
		var buf bytes.Buffer
		_, err := client.Objects.GetObject(objects.NewGetObjectParamsWithContext(ctx).
			WithRepository(repo).
			WithRef(MasterBranch).
			WithPath(p), nil, &buf)
		require.NoError(t, err, "get object for", p)
		content := buf.String()
		require.Equal(t, contents[i], content, "content should be the same", p)
	}

	// list uncommitted files
	entries = listRepositoryObjects(ctx, t, repo, MasterBranch)
	require.Len(t, entries, numOfFiles, "repository should have files")

	// commit changes
	_, err := client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch(MasterBranch).WithCommit(&models.CommitCreation{
		Message: swag.String("first commit"),
	}), nil)
	require.NoError(t, err, "initial commit")

	// list committed files in
	entries = listRepositoryObjects(ctx, t, repo, MasterBranch+":HEAD")
	require.Len(t, entries, numOfFiles, "repository should have files")

	// create branch1 based on master
	ref, err := client.Branches.CreateBranch(
		branches.NewCreateBranchParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(&models.BranchCreation{
				Name:   swag.String("branch1"),
				Source: swag.String(MasterBranch),
			}), nil)
	require.NoError(t, err, "failed to create branch1 from master")
	require.NotEmpty(t, ref, "reference to new branch")

	// list branches
	branchesResp, err := client.Branches.ListBranches(
		branches.NewListBranchesParamsWithContext(ctx).WithRepository(repo),
		nil)
	require.NoError(t, err, "list branches")
	require.ElementsMatch(t, branchesResp.Payload.Results, []string{MasterBranch, "branch1"}, "match existing branches")

	// branch1 - change file0
	_, _ = uploadFileRandomData(ctx, t, repo, "branch1", "file0")

	// branch1 - delete file1
	_, err = client.Objects.DeleteObject(objects.NewDeleteObjectParamsWithContext(ctx).WithRepository(repo).WithBranch("branch1").WithPath("file1"), nil)
	require.NoError(t, err, "delete object")

	// branch1 - add file.x
	_, _ = uploadFileRandomData(ctx, t, repo, "branch1", "fileX")

	// list master committed files
	masterObjects := listRepositoryObjects(ctx, t, repo, "master:HEAD")
	masterPaths := make([]string, len(masterObjects))
	for i := range masterObjects {
		masterPaths[i] = masterObjects[i].Path
	}
	require.EqualValues(t, masterPaths, paths)

	// list branch1 - make sure we have
	branch1Objects := listRepositoryObjects(ctx, t, repo, "branch1")
	for i := range branch1Objects {
		masterPaths[i] = branch1Objects[i].Path
	}
	pathsBranch1 := make([]string, len(paths))
	copy(pathsBranch1, paths)
	pathsBranch1 = append(append(paths[:1], paths[2:]...), "fileX")
	require.EqualValues(t, pathsBranch1, masterPaths)

	// branch1 - diff changes with master
	diffResp, err := client.Refs.DiffRefs(refs.NewDiffRefsParamsWithContext(ctx).
		WithRepository(repo).
		WithLeftRef("branch1").
		WithRightRef(MasterBranch), nil)
	require.NoError(t, err, "diff between branch1 and master")
	require.Len(t, diffResp.Payload.Results, 0, "no changes should be found as we didn't commit anything")

	// branch1 - commit changes
	_, err = client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch("branch1").WithCommit(&models.CommitCreation{
		Message: swag.String("3 changes"),
	}), nil)
	require.NoError(t, err, "commit 3 changes")

	// branch1 - diff changes with master
	diffResp, err = client.Refs.DiffRefs(refs.NewDiffRefsParamsWithContext(ctx).
		WithRepository(repo).
		WithLeftRef("branch1").
		WithRightRef(MasterBranch), nil)
	require.NoError(t, err, "diff between branch1 and master")
	require.ElementsMatch(t, diffResp.Payload.Results, []*models.Diff{
		{Path: "file0", PathType: "object", Type: "changed"},
		{Path: "file1", PathType: "object", Type: "removed"},
		{Path: "fileX", PathType: "object", Type: "added"},
	})

	// branch1 - merge to master
	mergeResp, err := client.Refs.MergeIntoBranch(refs.NewMergeIntoBranchParamsWithContext(ctx).
		WithRepository(repo).
		WithSourceRef("branch1").
		WithDestinationRef(MasterBranch), nil)
	require.NoError(t, err, "merge branch1 to master")
	require.ElementsMatch(t, mergeResp.Payload.Results, []*models.MergeResult{
		{Path: "file0", PathType: "object", Type: "changed"},
		{Path: "file1", PathType: "object", Type: "removed"},
		{Path: "fileX", PathType: "object", Type: "added"},
	})

	// branch1 - diff after merge (should be empty)
	diffResp, err = client.Refs.DiffRefs(refs.NewDiffRefsParamsWithContext(ctx).
		WithRepository(repo).
		WithLeftRef("branch1").
		WithRightRef(MasterBranch), nil)
	require.NoError(t, err, "diff between branch1 and master")
	require.Len(t, diffResp.Payload.Results, 0, "no diff between branch1 and master")

	// master - diff with branch1 (should be empty too)
	diffResp, err = client.Refs.DiffRefs(refs.NewDiffRefsParamsWithContext(ctx).
		WithRepository(repo).
		WithLeftRef(MasterBranch).
		WithRightRef("branch1"), nil)
	require.NoError(t, err, "diff between master and branch1")
	require.Len(t, diffResp.Payload.Results, 0, "no diff between master and branch1")

	// delete repository - keep it clean
	_, err = client.Repositories.DeleteRepository(
		repositories.NewDeleteRepositoryParamsWithContext(ctx).WithRepository(repo), nil)
	require.NoError(t, err, "failed to delete repository")
}
