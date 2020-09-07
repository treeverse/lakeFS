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
	branch := "feature-1"

	ref, err := client.Branches.CreateBranch(
		branches.NewCreateBranchParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(&models.BranchCreation{
				Name:   swag.String(branch),
				Source: swag.String(masterBranch),
			}), nil)
	require.NoError(t, err, "failed to create branch")
	logger.WithField("branchRef", ref).Info("Created branch, committing files")

	numberOfFiles := 10
	checksums := map[string]string{}
	for i := 0; i < numberOfFiles; i++ {
		checksum, content := uploadFileRandomData(ctx, t, repo, branch, fmt.Sprintf("%d.txt", i))
		checksums[checksum] = content
	}

	_, err = client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).
		WithRepository(repo).WithBranch(branch).WithCommit(&models.CommitCreation{
		Message: swag.String(fmt.Sprintf("Adding %d files", numberOfFiles)),
	}), nil)
	require.NoError(t, err, "failed to commit changes")

	mergeRes, err := client.Refs.MergeIntoBranch(
		refs.NewMergeIntoBranchParamsWithContext(ctx).WithRepository(repo).WithDestinationRef(masterBranch).WithSourceRef(branch), nil)
	require.NoError(t, err, "failed to merge branches")
	logger.WithField("mergeResult", mergeRes).Info("Merged successfully")

	resp, err := client.Objects.ListObjects(objects.NewListObjectsParamsWithContext(ctx).WithRepository(repo).WithRef(masterBranch).WithAmount(swag.Int64(100)), nil)
	require.NoError(t, err, "failed to list objects")
	payload := resp.GetPayload()
	objs := payload.Results
	pagin := payload.Pagination
	require.False(t, *pagin.HasMore, "pagination shouldn't have more items")
	require.Equal(t, int64(numberOfFiles), *pagin.Results)
	require.Equal(t, numberOfFiles, len(objs))
	logger.WithField("objs", objs).WithField("pagin", pagin).Info("Listed successfully")

	for _, obj := range objs {
		_, ok := checksums[obj.Checksum]
		require.True(t, ok, "file exists in master but shouldn't, obj: %s", *obj)
	}
}
