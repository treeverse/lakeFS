package esti

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
)

const (
	s3ImportPath       = "s3://esti-system-testing-data/import-test-data/"
	gsImportPath       = "gs://esti-system-testing-data/import-test-data/"
	azureImportPath    = "https://esti.blob.core.windows.net/esti-system-testing-data/import-test-data/"
	importTargetPrefix = "imported/new-prefix/"
	importBranchName   = "ingestion"
)

func TestImport(t *testing.T) {
	importPath := ""
	blockstoreType := viper.GetString(config.BlockstoreTypeKey)
	switch blockstoreType {
	case block.BlockstoreTypeS3:
		importPath = s3ImportPath
	case block.BlockstoreTypeGS:
		importPath = gsImportPath
	case block.BlockstoreTypeAzure:
		importPath = azureImportPath
	default:
		t.Skip("import isn't supported for non-production block adapters")
	}

	ctx, _, repoName := setupTest(t)
	defer tearDownTest(repoName)

	importFilesToCheck := []string{
		"nested/prefix-1/file002005",
		"nested/prefix-2/file001894",
		"nested/prefix-3/file000005",
		"nested/prefix-4/file000645",
		"nested/prefix-5/file001566",
		"nested/prefix-6/file002011",
		"nested/prefix-7/file000101",
		"prefix-1/file002100",
		"prefix-2/file000568",
		"prefix-3/file001331",
		"prefix-4/file001888",
		"prefix-5/file000987",
		"prefix-6/file001556",
		"prefix-7/file000001",
	}

	t.Run("default", func(t *testing.T) {
		testImport(t, ctx, repoName, importPath)
		verifyImportObjects(t, ctx, repoName, importTargetPrefix, importFilesToCheck)
	})

	t.Run("parent", func(t *testing.T) {
		// import without the directory separator as suffix to include the parent directory
		importPathParent := strings.TrimSuffix(importPath, "/")
		testImport(t, ctx, repoName, importPathParent)
		verifyImportObjects(t, ctx, repoName, importTargetPrefix+"import-test-data/", importFilesToCheck)
	})
}

func verifyImportObjects(t *testing.T, ctx context.Context, repoName string, prefix string, importFilesToCheck []string) {
	const expectedContentLength = 1024
	for _, k := range importFilesToCheck {
		// try to read some values from that ingested branch
		objPath := prefix + k
		objResp, err := client.GetObjectWithResponse(ctx, repoName, importBranchName, &api.GetObjectParams{
			Path: objPath,
		})
		require.NoError(t, err, "get object failed: %s", objPath)
		require.Equal(t, http.StatusOK, objResp.StatusCode(), "get object %s", objPath)
		require.Equal(t, expectedContentLength, int(objResp.HTTPResponse.ContentLength), "object content length %s", objPath)
	}
}

func testImport(t *testing.T, ctx context.Context, repoName string, importPath string) {
	var (
		after  string
		token  *string
		ranges []api.RangeMetadata
	)
	for {
		resp, err := client.IngestRangeWithResponse(ctx, repoName, api.IngestRangeJSONRequestBody{
			After:             after,
			ContinuationToken: token,
			FromSourceURI:     importPath,
			Prepend:           importTargetPrefix,
		})
		require.NoError(t, err, "failed to ingest range")
		require.Equal(t, http.StatusCreated, resp.StatusCode())
		require.NotNil(t, resp.JSON201)
		ranges = append(ranges, *resp.JSON201.Range)
		if !resp.JSON201.Pagination.HasMore {
			break
		}
		after = resp.JSON201.Pagination.LastKey
		token = resp.JSON201.Pagination.ContinuationToken
	}

	metarangeResp, err := client.CreateMetaRangeWithResponse(ctx, repoName, api.CreateMetaRangeJSONRequestBody{
		Ranges: ranges,
	})

	require.NoError(t, err, "failed to create metarange")
	require.Equal(t, http.StatusCreated, metarangeResp.StatusCode())
	require.NotNil(t, metarangeResp.JSON201.Id)

	_, err = client.CreateBranchWithResponse(ctx, repoName, api.CreateBranchJSONRequestBody{
		Name:   importBranchName,
		Source: "main",
	})
	require.NoError(t, err, "failed to create branch")

	commitResp, err := client.CommitWithResponse(ctx, repoName, importBranchName, &api.CommitParams{
		SourceMetarange: metarangeResp.JSON201.Id,
	}, api.CommitJSONRequestBody{
		Message: "created by import",
		Metadata: &api.CommitCreation_Metadata{
			AdditionalProperties: map[string]string{"created_by": "import"},
		},
	})
	require.NoError(t, err, "failed to commit")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())
}
