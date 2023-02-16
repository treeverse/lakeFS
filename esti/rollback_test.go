package esti

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
)

func TestResetAll(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	objPath := "1.txt"

	// upload file
	_, objContent := uploadFileRandomData(ctx, t, repo, mainBranch, objPath, false)
	f, err := found(ctx, repo, mainBranch, objPath)
	require.NoError(t, err)
	require.True(t, f, "uploaded object found")

	// commit file
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "resetAll",
	})
	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, verifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repo, mainBranch)

	// delete file
	deleteResp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &api.DeleteObjectParams{
		Path: objPath,
	})
	require.NoError(t, err, "failed to delete file")
	require.NoErrorf(t, verifyResponse(deleteResp.HTTPResponse, deleteResp.Body),
		"failed to delete file %s repo %s branch %s", objPath, repo, mainBranch)

	// reset
	reset := api.ResetCreation{
		Type: "reset",
	}
	resetResp, err := client.ResetBranchWithResponse(ctx, repo, mainBranch, api.ResetBranchJSONRequestBody(reset))
	require.NoError(t, err, "failed to reset")
	require.NoErrorf(t, verifyResponse(resetResp.HTTPResponse, resetResp.Body),
		"failed to reset commit %s repo %s branch %s", repo, mainBranch)

	// read file
	getObjResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &api.GetObjectParams{Path: objPath})
	require.NoError(t, err, "failed to get object")
	require.NoErrorf(t, verifyResponse(getObjResp.HTTPResponse, getObjResp.Body),
		"failed to get object repo %s branch %s path %s", repo, mainBranch, objPath)

	// assert file content
	body := string(getObjResp.Body)
	require.Equal(t, objContent, body, fmt.Sprintf("path: %s, expected: %s, actual:%s", objPath, objContent, body))
}

func TestResetPath(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	objPath1 := "prefix/1.txt"
	objPath2 := "2.txt"

	// upload files
	_, objContent1 := uploadFileRandomData(ctx, t, repo, mainBranch, objPath1, false)
	f, err := found(ctx, repo, mainBranch, objPath1)
	require.NoError(t, err)
	require.True(t, f, "uploaded object found")

	uploadFileRandomData(ctx, t, repo, mainBranch, objPath2, false)
	f, err = found(ctx, repo, mainBranch, objPath2)
	require.NoError(t, err)
	require.True(t, f, "uploaded object found")

	// commit files
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "resetPath",
	})
	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, verifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repo, mainBranch)

	// delete files
	deleteResp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &api.DeleteObjectParams{
		Path: objPath1,
	})
	require.NoError(t, err, "failed to delete file")
	require.NoErrorf(t, verifyResponse(deleteResp.HTTPResponse, deleteResp.Body),
		"failed to delete file %s repo %s branch %s", objPath1, repo, mainBranch)

	deleteResp, err = client.DeleteObjectWithResponse(ctx, repo, mainBranch, &api.DeleteObjectParams{
		Path: objPath2,
	})
	require.NoError(t, err, "failed to delete file")
	require.NoErrorf(t, verifyResponse(deleteResp.HTTPResponse, deleteResp.Body),
		"failed to delete file %s repo %s branch %s", objPath2, repo, mainBranch)

	// reset only file1 under the prefix
	prefix := "prefix"
	reset := api.ResetCreation{
		Path: &prefix,
		Type: "common_prefix",
	}
	resetResp, err := client.ResetBranchWithResponse(ctx, repo, mainBranch, api.ResetBranchJSONRequestBody(reset))
	require.NoError(t, err, "failed to reset")
	require.NoErrorf(t, verifyResponse(resetResp.HTTPResponse, resetResp.Body),
		"failed to reset prefix %s repo %s branch %s", prefix, repo, mainBranch)

	// read file1
	getObjResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &api.GetObjectParams{Path: objPath1})
	require.NoError(t, err, "failed to get object")
	require.NoErrorf(t, verifyResponse(getObjResp.HTTPResponse, getObjResp.Body),
		"failed to get object repo %s branch %s path %s", repo, mainBranch, objPath1)

	// assert file1 content
	body := string(getObjResp.Body)
	require.Equal(t, objContent1, body, fmt.Sprintf("path: %s, expected: %s, actual:%s", objPath1, objContent1, body))

	// assert file2 doesn't exists
	f, err = found(ctx, repo, mainBranch, objPath2)
	require.NoError(t, err)
	require.False(t, f, "object not found")
}

func TestResetObject(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	objPath1 := "1.txt"
	objPath2 := "2.txt"

	// upload files
	_, objContent1 := uploadFileRandomData(ctx, t, repo, mainBranch, objPath1, false)
	f, err := found(ctx, repo, mainBranch, objPath1)
	require.NoError(t, err)
	require.True(t, f, "uploaded object found")

	uploadFileRandomData(ctx, t, repo, mainBranch, objPath2, false)
	f, err = found(ctx, repo, mainBranch, objPath2)
	require.NoError(t, err)
	require.True(t, f, "uploaded object found")

	// commit files
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "resetObject",
	})
	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, verifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repo, mainBranch)

	// delete files
	deleteResp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &api.DeleteObjectParams{
		Path: objPath1,
	})
	require.NoError(t, err, "failed to delete file")
	require.NoErrorf(t, verifyResponse(deleteResp.HTTPResponse, deleteResp.Body),
		"failed to delete file %s repo %s branch %s", objPath1, repo, mainBranch)

	deleteResp, err = client.DeleteObjectWithResponse(ctx, repo, mainBranch, &api.DeleteObjectParams{
		Path: objPath2,
	})
	require.NoError(t, err, "failed to delete file")
	require.NoErrorf(t, verifyResponse(deleteResp.HTTPResponse, deleteResp.Body),
		"failed to delete file %s repo %s branch %s", objPath2, repo, mainBranch)

	// reset only file1
	reset := api.ResetCreation{
		Path: &objPath1,
		Type: "object",
	}
	resetResp, err := client.ResetBranchWithResponse(ctx, repo, mainBranch, api.ResetBranchJSONRequestBody(reset))
	require.NoError(t, err, "failed to reset")
	require.NoErrorf(t, verifyResponse(resetResp.HTTPResponse, resetResp.Body),
		"failed to reset object %s repo %s branch %s", objPath1, repo, mainBranch)

	// assert file1 exists
	getObjResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &api.GetObjectParams{Path: objPath1})
	require.NoError(t, err, "failed to get object")
	require.NoErrorf(t, verifyResponse(getObjResp.HTTPResponse, getObjResp.Body),
		"failed to get object repo %s branch %s path %s", repo, mainBranch, objPath1)

	// assert file content
	body := string(getObjResp.Body)
	require.Equal(t, objContent1, body, fmt.Sprintf("path: %s, expected: %s, actual:%s", objPath1, objContent1, body))

	// assert file2 doesn't exists
	f, err = found(ctx, repo, mainBranch, objPath2)
	require.NoError(t, err)
	require.False(t, f, "object not found")
}

func TestRevert(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	objPath1 := "1.txt"
	objPath2 := "2.txt"

	// upload file1
	uploadFileRandomData(ctx, t, repo, mainBranch, objPath1, false)
	f, err := found(ctx, repo, mainBranch, objPath1)
	require.NoError(t, err)
	require.True(t, f, "uploaded object found")

	// commit file1
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "singleCommit",
	})

	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, verifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repo, mainBranch)

	commitId := commitResp.JSON201.Id

	// upload file2
	_, objContent2 := uploadFileRandomData(ctx, t, repo, mainBranch, objPath2, false)
	f, err = found(ctx, repo, mainBranch, objPath2)
	require.NoError(t, err)
	require.True(t, f, "uploaded object found")

	// commit file2
	commitResp, err = client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message: "revert",
	})
	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, verifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repo, mainBranch)

	// revert to commit file1
	revertResp, err := client.RevertBranchWithResponse(ctx, repo, mainBranch, api.RevertBranchJSONRequestBody{
		Ref: commitId,
	})
	require.NoError(t, err, "failed to revert")
	require.NoErrorf(t, verifyResponse(revertResp.HTTPResponse, revertResp.Body),
		"failed to revert commit %s repo %s branch %s", commitId, repo, mainBranch)

	// assert file1 doesn't exist
	f, err = found(ctx, repo, mainBranch, objPath1)
	require.NoError(t, err)
	require.False(t, f, "object not found")

	// assert file2 exists
	getObjResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &api.GetObjectParams{Path: objPath2})
	require.NoError(t, err, "failed to get object")
	require.NoErrorf(t, verifyResponse(getObjResp.HTTPResponse, getObjResp.Body),
		"failed to get object repo %s branch %s path %s", repo, mainBranch, objPath2)

	// assert file2 content
	body := string(getObjResp.Body)
	require.Equal(t, objContent2, body, fmt.Sprintf("path: %s, expected: %s, actual:%s", objPath2, objContent2, body))
}
