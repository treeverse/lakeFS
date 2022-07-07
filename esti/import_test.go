package esti

import (
	"net/http"
	"testing"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/block"

	"github.com/stretchr/testify/require"

	"github.com/treeverse/lakefs/pkg/api"
)

const (
	s3ImportPath    = "s3://esti-system-testing-data/import-test-data/"
	gsImportPath    = "gs://esti-system-testing-data/import-test-data/"
	azureImportPath = "https://esti.blob.core.windows.net/esti-system-testing-data/import-test-data/"
	prefixImport    = "imported/new-prefix/"
)

var importFilesToCheck = []string{
	"prefix-1/file002100",
	"prefix-2/file000568",
	"prefix-3/file001331",
	"prefix-4/file001888",
	"prefix-5/file000987",
	"prefix-6/file001556",
	"prefix-7/file000001",
	"nested/prefix-1/file002005",
	"nested/prefix-2/file001894",
	"nested/prefix-3/file000005",
	"nested/prefix-4/file000645",
	"nested/prefix-5/file001566",
	"nested/prefix-6/file002011",
	"nested/prefix-7/file000101",
}

func TestImport(t *testing.T) {
	t.Skip("Flaky test, skipping.  See https://github.com/treeverse/lakeFS/issues/3428")
	importPath := ""
	switch viper.GetViper().GetString("blockstore_type") {
	case block.BlockstoreTypeS3:
		importPath = s3ImportPath
	case block.BlockstoreTypeGS:
		importPath = gsImportPath
	case block.BlockstoreTypeAzure:
		importPath = azureImportPath
	default:
		// import isn't supported for non-production block adapters
		t.Skip()
	}

	ctx, log, repoName := setupTest(t)

	hasMore := true
	after := ""
	var token *string

	var ranges []api.RangeMetadata
	for hasMore {
		resp, err := client.IngestRangeWithResponse(ctx, repoName, api.IngestRangeJSONRequestBody{
			After:             after,
			ContinuationToken: token,
			FromSourceURI:     importPath,
			Prepend:           prefixImport,
		})
		require.NoError(t, err, "failed to ingest range")
		log.Info("Ingest range response body: %s", resp.Body)
		require.Equal(t, http.StatusCreated, resp.StatusCode())

		require.NotNil(t, resp.JSON201)
		ranges = append(ranges, *resp.JSON201.Range)
		hasMore = resp.JSON201.Pagination.HasMore
		after = resp.JSON201.Pagination.LastKey
		token = resp.JSON201.Pagination.ContinuationToken
	}

	metarangeResp, err := client.CreateMetaRangeWithResponse(ctx, repoName, api.CreateMetaRangeJSONRequestBody{
		Ranges: ranges,
	})

	require.NoError(t, err, "failed to create metarange")
	require.Equal(t, http.StatusCreated, metarangeResp.StatusCode())
	require.NotNil(t, metarangeResp.JSON201.Id)

	const ingestionBranch = "ingestion"
	_, err = client.CreateBranchWithResponse(ctx, repoName, api.CreateBranchJSONRequestBody{
		Name:   ingestionBranch,
		Source: "main",
	})
	require.NoError(t, err, "failed to create branch")

	commitResp, err := client.CommitWithResponse(ctx, repoName, ingestionBranch, &api.CommitParams{
		SourceMetarange: metarangeResp.JSON201.Id,
	}, api.CommitJSONRequestBody{
		Message: "created by UI import",
		Metadata: &api.CommitCreation_Metadata{
			AdditionalProperties: map[string]string{"created_by": "import"},
		},
	})
	require.NoError(t, err, "failed to commit")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	for _, k := range importFilesToCheck {
		// try to read some values from that ingested branch
		objResp, err := client.GetObjectWithResponse(ctx, repoName, ingestionBranch, &api.GetObjectParams{
			Path: prefixImport + k,
		})
		require.NoError(t, err, "failed to get object")
		require.Equal(t, http.StatusOK, objResp.StatusCode())
		require.Equal(t, 1024, int(objResp.HTTPResponse.ContentLength))
	}
}
