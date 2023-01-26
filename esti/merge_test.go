package esti

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestMergeAndList(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	defer tearDownTest(repo)
	const branch = "feature-1"

	logger.WithField("branch", mainBranch).Info("Upload initial content")
	checksum, content := uploadFileRandomData(ctx, t, repo, mainBranch, "README", false)
	checksums := map[string]string{
		checksum: content,
	}

	logger.WithField("branch", mainBranch).Info("Commit initial content")
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "Initial content"})
	require.NoError(t, err, "failed to commit initial content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	logger.WithField("branch", branch).Info("Create branch")
	createBranchResp, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: mainBranch,
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, createBranchResp.StatusCode())
	ref := string(createBranchResp.Body)
	logger.WithField("branchRef", ref).Info("Branch created")

	objs := doMergeAndListIteration(t, logger, ctx, repo, branch, checksums, 1)
	for _, obj := range objs {
		_, ok := checksums[obj.Checksum]
		require.True(t, ok, "file doesn't exist in main but should, obj: %s", obj)
	}
	objs = doMergeAndListIteration(t, logger, ctx, repo, branch, checksums, 2)
	for _, obj := range objs {
		_, ok := checksums[obj.Checksum]
		require.True(t, ok, "file doesn't exist in main but should, obj: %s", obj)
	}
}

func doMergeAndListIteration(t *testing.T, logger logging.Logger, ctx context.Context, repo string, branch string, checksums map[string]string, iteration int) []api.ObjectStats {
	const addedFiles = 10
	for i := 0; i < addedFiles; i++ {
		p := fmt.Sprintf("%d.txt", i)
		logger.WithFields(logging.Fields{"iteration": iteration, "path": p}).Info("Upload content to branch")
		checksum, content := uploadFileRandomData(ctx, t, repo, branch, p, false)
		checksums[checksum] = content
	}
	const totalFiles = addedFiles + 1

	logger.WithField("iteration", iteration).Info("Commit uploaded files")
	commitResp, err := client.CommitWithResponse(ctx, repo, branch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: fmt.Sprintf("Adding %d files", addedFiles),
	})
	require.NoError(t, err, "failed to commit changes")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	mergeRes, err := client.MergeIntoBranchWithResponse(ctx, repo, branch, mainBranch, api.MergeIntoBranchJSONRequestBody{})
	require.NoError(t, err, "failed to merge branches")
	require.Equal(t, http.StatusOK, mergeRes.StatusCode())
	logger.WithFields(logging.Fields{"iteration": iteration, "mergeResult": mergeRes}).Info("Merged successfully")
	resp, err := client.ListObjectsWithResponse(ctx, repo, mainBranch, &api.ListObjectsParams{Amount: api.PaginationAmountPtr(100)})
	require.NoError(t, err, "failed to list objects")
	require.Equal(t, http.StatusOK, resp.StatusCode())
	payload := resp.JSON200
	objs := payload.Results
	pagin := payload.Pagination
	require.False(t, pagin.HasMore, "pagination shouldn't have more items")
	require.Len(t, objs, totalFiles)
	require.Equal(t, totalFiles, pagin.Results)
	logger.WithFields(logging.Fields{"iteration": iteration, "objs": objs, "pagin": pagin}).Info("Listed successfully")
	return objs
}
