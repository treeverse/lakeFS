package nessie

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/client/refs"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/logging"
)

const (
	masterBranch = "master"
)

func TestSingleCommit(t *testing.T) {
	ctx, _, repo := setupTest(t)
	objPath := "1.txt"

	_, objContent := uploadFile(ctx, t, repo, masterBranch, objPath)
	_, err := client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithCommit(&models.CommitCreation{
		Message: swag.String("nessie:singleCommit"),
	}), nil)
	require.NoError(t, err, "failed to commit changes")

	var b bytes.Buffer
	_, err = client.Objects.GetObject(objects.NewGetObjectParamsWithContext(ctx).WithRepository(repo).WithRef(masterBranch).WithPath(objPath), nil, &b)
	require.NoError(t, err, "failed to get object")

	require.Equal(t, objContent, b.String(), fmt.Sprintf("path: %s, expected: %s, actual:%s", objPath, objContent, b.String()))
}

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
		checksum, content := uploadFile(ctx, t, repo, branch, fmt.Sprintf("%d.txt", i))
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

func setupTest(t *testing.T) (context.Context, logging.Logger, string) {
	ctx := context.Background()
	logger := logger.WithField("testName", t.Name())
	repo := createRepo(ctx, t)
	logger.WithField("repo", repo).Info("Created repository")
	return ctx, logger, repo
}

func createRepo(ctx context.Context, t *testing.T) string {
	name := strings.ToLower(t.Name())
	storageNamespace := viper.GetString("storage_namespace")
	repoStorage := storageNamespace + "/" + name
	logger.WithFields(logging.Fields{
		"repository":        name,
		"storage_namespace": repoStorage,
		"name":              name,
	}).Debug("Create repository for test")
	_, err := client.Repositories.CreateRepository(repositories.NewCreateRepositoryParamsWithContext(ctx).
		WithRepository(&models.RepositoryCreation{
			DefaultBranch:    masterBranch,
			ID:               swag.String(name),
			StorageNamespace: swag.String(repoStorage),
		}), nil)
	require.NoError(t, err, "failed to create repo")
	return name
}

func uploadFile(ctx context.Context, t *testing.T, repo, branch, objPath string) (checksum, content string) {
	const contentLength = 16
	objContent := randstr.Hex(contentLength)
	contentReader := runtime.NamedReader("content", strings.NewReader(objContent))
	stats, err := client.Objects.UploadObject(
		objects.NewUploadObjectParamsWithContext(ctx).
			WithRepository(repo).
			WithBranch(branch).
			WithPath(objPath).
			WithContent(contentReader), nil)

	require.NoError(t, err, "failed to upload file")
	payload := stats.GetPayload()
	return payload.Checksum, objContent
}
