package esti

import (
	"net/http"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

func TestShallowCopyNotImplemented(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	const srcPath = "shallow-src"
	const objContent = "shallow copy test content"
	uploadResp, err := UploadContent(ctx, repo, mainBranch, srcPath, objContent, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, uploadResp.StatusCode())

	copyResp, err := client.CopyObjectWithResponse(ctx, repo, mainBranch, &apigen.CopyObjectParams{
		DestPath: "shallow-dest",
	}, apigen.CopyObjectJSONRequestBody{
		SrcPath: srcPath,
		Shallow: swag.Bool(true),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusNotImplemented, copyResp.StatusCode(),
		"shallow copy should return 501 Not Implemented in OSS")
}
