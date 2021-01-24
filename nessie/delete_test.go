package nessie

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/models"
)

func found(ctx context.Context, repo, ref, path string) (bool, error) {
	var b bytes.Buffer
	res, err := client.Objects.GetObject(objects.NewGetObjectParamsWithContext(ctx).WithRepository(repo).WithRef(masterBranch).WithPath(path), nil, &b)

	if res != nil {
		return true, nil
	}
	// err is not really an objects.GetObjectNotFound, scan for its message
	if strings.Contains(err.Error(), "getObjectNotFound") {
		return false, nil
	}
	return false, fmt.Errorf("get object to check if found: %w", err)
}

func TestDeleteStaging(t *testing.T) {
	ctx, _, repo := setupTest(t)
	objPath := "1.txt"

	_, _ = uploadFileRandomData(ctx, t, repo, masterBranch, objPath)

	f, err := found(ctx, repo, masterBranch, objPath)
	assert.NoError(t, err)
	assert.True(t, f, "uploaded object found")

	_, err = client.Objects.DeleteObject(objects.NewDeleteObjectParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithPath(objPath), nil)
	require.NoError(t, err, "failed to delete object")

	f, err = found(ctx, repo, masterBranch, objPath)
	assert.NoError(t, err)
	assert.False(t, f, "deleted object found")
}

func TestDeleteCommitted(t *testing.T) {
	ctx, _, repo := setupTest(t)
	objPath := "1.txt"

	_, _ = uploadFileRandomData(ctx, t, repo, masterBranch, objPath)

	f, err := found(ctx, repo, masterBranch, objPath)
	assert.NoError(t, err)
	assert.True(t, f, "uploaded object found")

	_, err = client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithCommit(&models.CommitCreation{
		Message: swag.String("nessie:singleCommit"),
	}), nil)
	require.NoError(t, err, "commit changes")

	_, err = client.Objects.DeleteObject(objects.NewDeleteObjectParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithPath(objPath), nil)
	require.NoError(t, err, "failed to delete object")

	f, err = found(ctx, repo, masterBranch, objPath)
	assert.NoError(t, err)
	assert.False(t, f, "deleted object found")
}

func TestCommitDeleteCommitted(t *testing.T) {
	ctx, _, repo := setupTest(t)
	objPath := "1.txt"

	_, _ = uploadFileRandomData(ctx, t, repo, masterBranch, objPath)

	f, err := found(ctx, repo, masterBranch, objPath)
	assert.NoError(t, err)
	assert.True(t, f, "uploaded object found")

	_, err = client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithCommit(&models.CommitCreation{
		Message: swag.String("nessie:singleCommit"),
	}), nil)
	require.NoError(t, err, "commit new file")

	_, err = client.Objects.DeleteObject(objects.NewDeleteObjectParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithPath(objPath), nil)
	require.NoError(t, err, "failed to delete object")

	_, err = client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch(masterBranch).WithCommit(&models.CommitCreation{
		Message: swag.String("nessie:deleteCommit"),
	}), nil)
	require.NoError(t, err, "commit delet file")

	f, err = found(ctx, repo, masterBranch, objPath)
	assert.NoError(t, err)
	assert.False(t, f, "deleted object found")
}
