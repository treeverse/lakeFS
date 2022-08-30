package esti

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
)

func found(ctx context.Context, repo, ref, path string) (bool, error) {
	res, err := client.GetObjectWithResponse(ctx, repo, ref, &api.GetObjectParams{Path: path})
	if err == nil && res.HTTPResponse.StatusCode == http.StatusOK {
		return true, nil
	}
	// err is not really an objects.GetObjectNotFound, scan for its message
	if res != nil && res.HTTPResponse.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("get object to check if found: %w", err)
}

func TestDeleteStaging(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	objPath := "1.txt"

	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, objPath, false)

	f, err := found(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.True(t, f, "uploaded object found")

	resp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &api.DeleteObjectParams{Path: objPath})
	require.NoError(t, err, "failed to delete object")
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	f, err = found(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.False(t, f, "deleted object found")
}

func TestDeleteCommitted(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	objPath := "1.txt"

	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, objPath, false)

	f, err := found(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.True(t, f, "uploaded object found")

	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "singleCommit"})
	require.NoError(t, err, "commit changes")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	getResp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &api.DeleteObjectParams{Path: objPath})
	require.NoError(t, err, "failed to delete object")
	require.Equal(t, http.StatusNoContent, getResp.StatusCode())

	f, err = found(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.False(t, f, "deleted object found")
}

func TestCommitDeleteCommitted(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	objPath := "1.txt"

	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, objPath, false)

	f, err := found(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.True(t, f, "uploaded object found")

	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "singleCommit",
	})
	require.NoError(t, err, "commit new file")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	deleteResp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &api.DeleteObjectParams{Path: objPath})
	require.NoError(t, err, "failed to delete object")
	require.Equal(t, http.StatusNoContent, deleteResp.StatusCode())

	commitResp, err = client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "deleteCommit",
	})
	require.NoError(t, err, "commit delete file")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	f, err = found(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.False(t, f, "deleted object found")
}
