package nessie

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/client/objects"
	"github.com/treeverse/lakefs/api/gen/models"
)

func TestCommitSingle(t *testing.T) {
	ctx, _, repo := setupTest(t)
	objPath := "1.txt"

	_, objContent := uploadFileRandomData(ctx, t, repo, MasterBranch, objPath)
	_, err := client.Commits.Commit(commits.NewCommitParamsWithContext(ctx).WithRepository(repo).WithBranch(MasterBranch).WithCommit(&models.CommitCreation{
		Message: swag.String("nessie:singleCommit"),
	}), nil)
	require.NoError(t, err, "failed to commit changes")

	var b bytes.Buffer
	_, err = client.Objects.GetObject(objects.NewGetObjectParamsWithContext(ctx).WithRepository(repo).WithRef(MasterBranch).WithPath(objPath), nil, &b)
	require.NoError(t, err, "failed to get object")

	require.Equal(t, objContent, b.String(), fmt.Sprintf("path: %s, expected: %s, actual:%s", objPath, objContent, b.String()))
}
