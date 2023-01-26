package esti

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/config"
)

const (
	s3CopyDataPath            = "s3://esti-system-testing-data/copy-test-data/"
	gsCopyDataPath            = "gs://esti-system-testing-data/copy-test-data/"
	azureCopyDataPathTemplate = "https://%s.blob.core.windows.net/esti-system-testing-data/copy-test-data/"
	azureAbortAccount         = "esti4multipleaccounts"
	ingestionBranch           = "test-data"
	largeObject               = "squash.tar"
)

var azureCopyDataPath = ""

func TestCopyObject(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	t.Run("copy_large_size_file", func(t *testing.T) {
		blockstoreType := viper.GetString(config.BlockstoreTypeKey)
		// Copying from same account occurs immediately even for large files (async)
		if blockstoreType == block.BlockstoreTypeAzure { // Extract storage account
			resp, err := client.GetRepositoryWithResponse(ctx, repo)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode())
			u, err := url.Parse(resp.JSON200.StorageNamespace)
			require.NoError(t, err)
			accountName, err := azure.ExtractStorageAccount(u)
			require.NoError(t, err)
			azureCopyDataPath = fmt.Sprintf(azureCopyDataPathTemplate, accountName)
		}

		importTestData(t, ctx, client, repo)
		res, err := client.StatObjectWithResponse(ctx, repo, ingestionBranch, &api.StatObjectParams{
			Path: largeObject,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())

		objStat := res.JSON200
		destPath := "foo"
		srcBranch := ingestionBranch
		copyResp, err := client.CopyObjectWithResponse(ctx, repo, "main", &api.CopyObjectParams{
			DestPath: destPath,
		}, api.CopyObjectJSONRequestBody{
			SrcPath: largeObject,
			SrcRef:  &srcBranch,
		})
		require.NoError(t, err, "failed to copy")
		require.Equal(t, http.StatusCreated, copyResp.StatusCode())

		// Verify creation path, date and physical address are different
		copyStat := copyResp.JSON201
		require.NotNil(t, copyStat)
		require.NotEqual(t, objStat.PhysicalAddress, copyStat.PhysicalAddress)
		require.GreaterOrEqual(t, copyStat.Mtime, objStat.Mtime)
		require.Equal(t, destPath, copyStat.Path)

		// Verify all else is equal
		objStat.Mtime = copyStat.Mtime
		objStat.Path = copyStat.Path
		objStat.PhysicalAddress = copyStat.PhysicalAddress
		require.Nil(t, deep.Equal(objStat, copyStat))

		// get back info
		statResp, err := client.StatObjectWithResponse(ctx, repo, "main", &api.StatObjectParams{Path: destPath})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, statResp.StatusCode())
		require.Nil(t, deep.Equal(statResp.JSON200, copyStat))
	})

	// Copying different accounts takes more time and allows us to abort the copy in the middle
	t.Run("copy_large_size_file_abort", func(t *testing.T) {
		requireBlockstoreType(t, block.BlockstoreTypeAzure)
		azureCopyDataPath = fmt.Sprintf(azureCopyDataPathTemplate, azureAbortAccount)
		importTestData(t, ctx, client, repo)
		var err error
		res, err := client.StatObjectWithResponse(ctx, repo, ingestionBranch, &api.StatObjectParams{
			Path: largeObject,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())

		destPath := "bar"
		srcBranch := ingestionBranch
		cancelCtx, cancel := context.WithCancel(ctx)
		var (
			wg       sync.WaitGroup
			copyResp *api.CopyObjectResponse
		)
		// Run copy object async and cancel context after 5 seconds
		go func() {
			wg.Add(1)
			copyResp, err = client.CopyObjectWithResponse(cancelCtx, repo, "main", &api.CopyObjectParams{
				DestPath: destPath,
			}, api.CopyObjectJSONRequestBody{
				SrcPath: largeObject,
				SrcRef:  &srcBranch,
			})
			defer wg.Done()
		}()

		time.Sleep(5 * time.Second)
		cancel()
		wg.Wait()
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, copyResp)

		// Verify object doesn't exist

		// get back info
		statResp, err := client.StatObjectWithResponse(ctx, repo, "main", &api.StatObjectParams{Path: destPath})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, statResp.StatusCode())
	})
}

func importTestData(t *testing.T, ctx context.Context, client api.ClientWithResponsesInterface, repoName string) {
	t.Helper()
	importPath := ""
	blockstoreType := viper.GetString(config.BlockstoreTypeKey)
	switch blockstoreType {
	case block.BlockstoreTypeS3:
		importPath = s3CopyDataPath
	case block.BlockstoreTypeGS:
		importPath = gsCopyDataPath
	case block.BlockstoreTypeAzure:
		importPath = azureCopyDataPath
	default:
		t.Skip("import isn't supported for non-production block adapters")
	}
	var (
		after  = ""
		token  *string
		ranges []api.RangeMetadata
	)
	for {
		resp, err := client.IngestRangeWithResponse(ctx, repoName, api.IngestRangeJSONRequestBody{
			After:             after,
			ContinuationToken: token,
			FromSourceURI:     importPath,
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
		Name:   ingestionBranch,
		Source: "main",
	})
	require.NoError(t, err, "failed to create branch")

	commitResp, err := client.CommitWithResponse(ctx, repoName, ingestionBranch, &api.CommitParams{
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
