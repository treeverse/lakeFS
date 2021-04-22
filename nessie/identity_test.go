package nessie

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/api"

	"github.com/stretchr/testify/require"
)

func TestIdentity(t *testing.T) {
	for _, direct := range testDirectDataAccess {
		name := "indirect"
		if direct {
			name = "direct"
		}
		t.Run(name, func(t *testing.T) {
			ctx, _, repo := setupTest(t)
			objPath := "1.txt"
			branch1 := "feature-1"
			branch2 := "feature-2"

			_, err := client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
				Name:   branch1,
				Source: mainBranch,
			})
			require.NoError(t, err, "failed creating branch1")

			checksum, objContent, err := uploadFileRandomDataAndReport(ctx, repo, branch1, objPath, direct)
			require.NoError(t, err, "failed uploading file")
			commitResp, err := client.CommitWithResponse(ctx, repo, branch1, api.CommitJSONRequestBody{
				Message: "commit on branch1",
			})
			require.NoError(t, err, "failed to commit changes")
			require.NoErrorf(t, verifyResponse(commitResp.HTTPResponse, commitResp.Body),
				"failed to commit changes repo %s branch %s", repo, mainBranch)

			// upload the same content again to a different branch
			_, err = client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{
				Name:   branch2,
				Source: mainBranch,
			})
			require.NoError(t, err, "failed creating branch2")
			checksumNew, err := uploadFileAndReport(ctx, repo, mainBranch, objPath, objContent, direct)
			require.Equal(t, checksum, checksumNew, "Same file uploaded to committed branch, expected no checksum difference")

			diff, err := client.DiffRefsWithResponse(ctx, repo, branch1, branch2, &api.DiffRefsParams{})
			require.NoError(t, err, "Diff refs failed")
			require.Empty(t, diff.JSON200.Results, "Expected no diff files")
		})
	}
}
