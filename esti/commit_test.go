package esti

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

func TestCommitSingle(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	const objPath = "1.txt"
	_, objContent := UploadFileRandomData(ctx, t, repo, mainBranch, objPath, nil)
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "singleCommit",
	})
	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, VerifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repo, mainBranch)

	getObjResp, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &apigen.GetObjectParams{Path: objPath})
	require.NoError(t, err, "failed to get object")
	require.NoErrorf(t, VerifyResponse(getObjResp.HTTPResponse, getObjResp.Body),
		"failed to get object repo %s branch %s path %s", repo, mainBranch, objPath)

	body := string(getObjResp.Body)
	require.Equalf(t, objContent, body, "path: %s, expected: %s, actual:%s", objPath, objContent, body)
}

// genNames generates n consecutive filenames starting with prefix.
func genNames(n int, prefix string) []string {
	ret := make([]string, n)
	for i := range ret {
		ret[i] = fmt.Sprint(prefix, i+1)
	}
	return ret
}

type Upload struct {
	Repo, Branch, Path string
}

// upload uploads random file data for uploads.
func upload(ctx context.Context, uploads chan Upload) error {
	for u := range uploads {
		_, _, err := UploadFileRandomDataAndReport(ctx, u.Repo, u.Branch, u.Path, false, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestCommitInMixedOrder(t *testing.T) {
	const (
		parallelism = 5
		size        = 100
	)

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	names1 := genNames(size, "run2/foo")
	uploads := make(chan Upload, size)
	wg := sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			if err := upload(ctx, uploads); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	for _, name := range names1 {
		uploads <- Upload{Repo: repo, Branch: mainBranch, Path: name}
	}
	close(uploads)
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "mixedOrderCommit1",
	})
	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, VerifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repo, mainBranch)

	names2 := genNames(size, "run1/foo")
	uploads = make(chan Upload, size)
	wg = sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			if err := upload(ctx, uploads); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	for _, name := range names2 {
		uploads <- Upload{Repo: repo, Branch: mainBranch, Path: name}
	}
	close(uploads)
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	commitResp, err = client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "mixedOrderCommit2",
	})
	require.NoError(t, err, "failed to commit second set of changes")
	require.NoErrorf(t, VerifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit second set of changes repo %s branch %s", repo, mainBranch)
}

// Verify panic fix when committing with nil tombstone over KV
func TestCommitWithTombstone(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	origObjPathLow := "objb.txt"
	origObjPathHigh := "objc.txt"
	UploadFileRandomData(ctx, t, repo, mainBranch, origObjPathLow, nil)
	UploadFileRandomData(ctx, t, repo, mainBranch, origObjPathHigh, nil)
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "First commit",
	})
	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, VerifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repo, mainBranch)

	tombstoneObjPath := "obja.txt"
	newObjPath := "objd.txt"
	UploadFileRandomData(ctx, t, repo, mainBranch, tombstoneObjPath, nil)
	UploadFileRandomData(ctx, t, repo, mainBranch, newObjPath, nil)

	// Turning tombstoneObjPath to tombstone
	resp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &apigen.DeleteObjectParams{Path: tombstoneObjPath})
	require.NoError(t, err, "failed to delete object")
	require.Equal(t, http.StatusNoContent, resp.StatusCode())

	commitResp, err = client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "Commit with tombstone",
	})
	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, VerifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repo, mainBranch)
}

func TestCommitReadOnlyRepo(t *testing.T) {
	ctx := context.Background()
	name := strings.ToLower(t.Name())
	storageNamespace := GenerateUniqueStorageNamespace(name)
	repoName := MakeRepositoryName(name)
	resp, err := client.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody{
		DefaultBranch:    apiutil.Ptr(mainBranch),
		Name:             repoName,
		StorageNamespace: storageNamespace,
		ReadOnly:         swag.Bool(true),
	})
	require.NoErrorf(t, err, "failed to create repository '%s', storage '%s'", name, storageNamespace)
	require.NoErrorf(t, VerifyResponse(resp.HTTPResponse, resp.Body),
		"create repository '%s', storage '%s'", name, storageNamespace)
	defer tearDownTest(repoName)

	commitResp, _ := client.CommitWithResponse(ctx, repoName, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "singleCommit",
	})
	if commitResp.StatusCode() != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for committing to read-only repo, got %d instead", resp.StatusCode())
	}

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
	err = VerifyResponse(uploadResp.HTTPResponse, uploadResp.Body)
	require.NoError(t, err, "failed to upload file", repoName, mainBranch, objPath)

	commitResp, err = client.CommitWithResponse(ctx, repoName, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "singleCommit",
		Force:   swag.Bool(true),
	})
	require.NoError(t, err, "failed to commit changes")
	require.NoErrorf(t, VerifyResponse(commitResp.HTTPResponse, commitResp.Body),
		"failed to commit changes repo %s branch %s", repoName, mainBranch)

	getObjResp, err := client.GetObjectWithResponse(ctx, repoName, mainBranch, &apigen.GetObjectParams{Path: objPath})
	require.NoError(t, err, "failed to get object")
	require.NoErrorf(t, VerifyResponse(getObjResp.HTTPResponse, getObjResp.Body),
		"failed to get object repo %s branch %s path %s", repoName, mainBranch, objPath)

	body := string(getObjResp.Body)
	require.Equalf(t, objContent, body, "path: %s, expected: %s, actual:%s", objPath, objContent, body)

}
