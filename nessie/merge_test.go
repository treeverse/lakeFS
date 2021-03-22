package nessie

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
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
	commitResp, err := client.CommitWithResponse(ctx, repo, masterBranch, api.CommitJSONRequestBody{Message: "Initial content"})
	require.NoError(t, err, "failed to commit initial content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	logger.WithField("branch", branch).Info("Create branch")
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: masterBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, createBranchResp.StatusCode())
	ref := string(createBranchResp.Body)
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
	commitResp, err = client.CommitWithResponse(ctx, repo, branch, api.CommitJSONRequestBody{
		Message: fmt.Sprintf("Adding %d files", addedFiles),
	})
	require.NoError(t, err, "failed to commit changes")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	mergeRes, err := client.MergeIntoBranchWithResponse(ctx, repo, branch, masterBranch, api.MergeIntoBranchJSONRequestBody{})
	require.NoError(t, err, "failed to merge branches")
	require.Equal(t, http.StatusOK, mergeRes.StatusCode())
	logger.WithField("mergeResult", mergeRes).Info("Merged successfully")

	resp, err := client.ListObjectsWithResponse(ctx, repo, masterBranch, &api.ListObjectsParams{Amount: api.PaginationAmountPtr(100)})
	require.NoError(t, err, "failed to list objects")
	require.Equal(t, http.StatusOK, resp.StatusCode())
	payload := resp.JSON200
	objs := payload.Results
	pagin := payload.Pagination
	require.False(t, pagin.HasMore, "pagination shouldn't have more items")
	require.Equal(t, int64(totalFiles), pagin.Results)
	require.Equal(t, totalFiles, len(objs))
	logger.WithField("objs", objs).WithField("pagin", pagin).Info("Listed successfully")

	for _, obj := range objs {
		_, ok := checksums[obj.Checksum]
		require.True(t, ok, "file exists in master but shouldn't, obj: %s", obj)
	}
}
