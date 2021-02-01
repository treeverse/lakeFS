package nessie

import (
	"fmt"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/client/refs"
	"github.com/treeverse/lakefs/api/gen/models"
)

func TestMergeAndList(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	const branch = "feature-1"

	logger.WithField("branch", masterBranch).Info("Upload initial content")
	checksum, content := uploadFileRandomData(ctx, t, repo, masterBranch, "README")
	checksums := map[string]string{
		checksum: content,
	}

	logger.WithField("branch", masterBranch).Info("Commit initial content")
	_, err := client.Commits.Commit(
		commits.NewCommitParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(masterBranch).
			WithCommit(&models.CommitCreation{Message: swag.String("Initial content")}),
		nil)
	require.NoError(t, err, "failed to commit initial content")

	logger.WithField("branch", branch).Info("Create branch")
	ref, err := client.Branches.CreateBranch(
		branches.NewCreateBranchParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(&models.BranchCreation{
				Name:   swag.String(branch),
				Source: swag.String(masterBranch),
			}), nil)
	require.NoError(t, err, "failed to create branch")
	logger.WithField("branchRef", ref).Info("Branch created")

	const addedFiles = 10
	for i := 0; i < addedFiles; i++ {
		p := fmt.Sprintf("%d.txt", i)
		logger.WithField("path", p).Info("Upload content to branch")
		checksum, content := uploadFileRandomData(ctx, t, repo, branch, p)
		checksums[checksum] = content
	}
	const totalFiles = addedFiles + 1

	logger.Info("Commit uploaded files")
	_, err = client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).
		WithRepository(repo).WithBranch(branch).WithCommit(&models.CommitCreation{
		Message: swag.String(fmt.Sprintf("Adding %d files", addedFiles)),
	}), nil)
	require.NoError(t, err, "failed to commit changes")

	mergeRes, err := client.Refs.MergeIntoBranch(
		refs.NewMergeIntoBranchParamsWithContext(ctx).WithRepository(repo).WithTheirsBranch(masterBranch).WithOursRef(branch), nil)
	require.NoError(t, err, "failed to merge branches")
	logger.WithField("mergeResult", mergeRes).Info("Merged successfully")

	resp, err := client.Objects.ListObjects(objects.NewListObjectsParamsWithContext(ctx).WithRepository(repo).WithRef(masterBranch).WithAmount(swag.Int64(100)), nil)
	require.NoError(t, err, "failed to list objects")
	payload := resp.GetPayload()
	objs := payload.Results
	pagin := payload.Pagination
	require.False(t, *pagin.HasMore, "pagination shouldn't have more items")
	require.Equal(t, int64(totalFiles), *pagin.Results)
	require.Equal(t, totalFiles, len(objs))
	logger.WithField("objs", objs).WithField("pagin", pagin).Info("Listed successfully")

	for _, obj := range objs {
		_, ok := checksums[obj.Checksum]
		require.True(t, ok, "file exists in master but shouldn't, obj: %s", *obj)
	}
}
