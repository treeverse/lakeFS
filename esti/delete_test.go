package esti

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

func objectFound(ctx context.Context, repo, ref, path string) (bool, error) {
	res, err := client.GetObjectWithResponse(ctx, repo, ref, &apigen.GetObjectParams{Path: path})
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

	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, objPath)

	f, err := objectFound(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.True(t, f, "uploaded object found")

	resp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &apigen.DeleteObjectParams{Path: objPath})
	require.NoError(t, err, "failed to delete object")
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	f, err = objectFound(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.False(t, f, "deleted object found")
}

func TestDeleteCommitted(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	objPath := "1.txt"

	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, objPath)

	f, err := objectFound(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.True(t, f, "uploaded object found")

	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{Message: "singleCommit"})
	require.NoError(t, err, "commit changes")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	getResp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &apigen.DeleteObjectParams{Path: objPath})
	require.NoError(t, err, "failed to delete object")
	require.Equal(t, http.StatusNoContent, getResp.StatusCode())

	f, err = objectFound(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.False(t, f, "deleted object found")
}

func TestDeleteObjectsReadOnlyRepository(t *testing.T) {
	ctx := context.Background()
	name := strings.ToLower(t.Name())
	storageNamespace := generateUniqueStorageNamespace(name)
	repoName := makeRepositoryName(name)
	resp, err := client.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
		DefaultBranch:    apiutil.Ptr(mainBranch),
		Name:             repoName,
		StorageNamespace: storageNamespace,
		ReadOnly:         swag.Bool(true),
	})
	require.NoErrorf(t, err, "failed to create repository '%s', storage '%s'", name, storageNamespace)
	require.NoErrorf(t, verifyResponse(resp.HTTPResponse, resp.Body),
		"create repository '%s', storage '%s'", name, storageNamespace)
	defer tearDownTest(repoName)

	const objPath = "1.txt"
	objContent := randstr.String(randomDataContentLength)
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	contentWriter, err := w.CreateFormFile("content", filepath.Base(objPath))
	require.NoError(t, err, "failed to upload file", repoName, mainBranch, objPath)
	_, err = contentWriter.Write([]byte(objContent))
	require.NoError(t, err, "failed to upload file", repoName, mainBranch, objPath)
	err = w.Close()
	require.NoError(t, err, "failed to upload file", repoName, mainBranch, objPath)
	uploadResp, err := client.UploadObjectWithBodyWithResponse(ctx, repoName, mainBranch, &apigen.UploadObjectParams{
		Path:  objPath,
		Force: swag.Bool(true),
	}, w.FormDataContentType(), &b)
	require.NoError(t, err, "failed to upload file", repoName, mainBranch, objPath)
	err = verifyResponse(uploadResp.HTTPResponse, uploadResp.Body)
	require.NoError(t, err, "failed to upload file", repoName, mainBranch, objPath)

	deleteResp, err := client.DeleteObjectWithResponse(ctx, repoName, mainBranch, &apigen.DeleteObjectParams{Path: objPath})
	require.NoError(t, err, "failed to delete object")
	if deleteResp.JSON403 == nil {
		t.Fatalf("expected 403 forbidden error for trying to delete an object from a read-only repository, got %d instead", deleteResp.StatusCode())
	}

	deleteResp, err = client.DeleteObjectWithResponse(ctx, repoName, mainBranch, &apigen.DeleteObjectParams{Path: objPath, Force: swag.Bool(true)})
	require.NoError(t, err, "failed to delete object")
	require.Equal(t, http.StatusNoContent, deleteResp.StatusCode())

	f, err := objectFound(ctx, repoName, mainBranch, objPath)
	assert.NoError(t, err)
	assert.False(t, f, "deleted object found")
}

func TestCommitDeleteCommitted(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	objPath := "1.txt"

	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, objPath)

	f, err := objectFound(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.True(t, f, "uploaded object found")

	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "singleCommit",
	})
	require.NoError(t, err, "commit new file")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	deleteResp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &apigen.DeleteObjectParams{Path: objPath})
	require.NoError(t, err, "failed to delete object")
	require.Equal(t, http.StatusNoContent, deleteResp.StatusCode())

	commitResp, err = client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "deleteCommit",
	})
	require.NoError(t, err, "commit delete file")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	f, err = objectFound(ctx, repo, mainBranch, objPath)
	assert.NoError(t, err)
	assert.False(t, f, "deleted object found")
}
