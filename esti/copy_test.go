package esti

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
)

const (
	s3CopyDataPath    = "s3://esti-system-testing-data/copy-test-data/"
	gsCopyDataPath    = "gs://esti-system-testing-data/copy-test-data/"
	azureCopyDataPath = "https://esti.blob.core.windows.net/esti-system-testing-data/copy-test-data/"
	azureAbortAccount = "esti4multipleaccounts"
	largeObject       = "squash.tar"
)

func TestCopyObject(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	t.Run("copy_large_size_file", func(t *testing.T) {
		importPath := getImportPath(t)

		const ingestionBranch = "test-copy"

		_ = testImportNew(t, ctx, repo, ingestionBranch,
			[]apigen.ImportLocation{{Path: importPath, Type: "common_prefix"}},
			map[string]string{"created_by": "import"},
			false,
		)

		res, err := client.StatObjectWithResponse(ctx, repo, ingestionBranch, &apigen.StatObjectParams{
			Path: largeObject,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())

		objStat := res.JSON200
		destPath := "foo"
		srcBranch := ingestionBranch
		copyResp, err := client.CopyObjectWithResponse(ctx, repo, "main", &apigen.CopyObjectParams{
			DestPath: destPath,
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: largeObject,
			SrcRef:  &srcBranch,
		})
		require.NoError(t, err, "failed to copy")
		require.NotNil(t, copyResp.JSON201)

		// Verify the creation path, date and physical address are different
		copyStat := copyResp.JSON201
		require.NotEqual(t, objStat.PhysicalAddress, copyStat.PhysicalAddress)
		require.GreaterOrEqual(t, copyStat.Mtime, objStat.Mtime)
		require.Equal(t, destPath, copyStat.Path)

		// Verify all else is equal
		objStat.Mtime = copyStat.Mtime
		objStat.Path = copyStat.Path
		objStat.PhysicalAddress = copyStat.PhysicalAddress
		require.Nil(t, deep.Equal(objStat, copyStat))

		// get back info
		statResp, err := client.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: destPath})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, statResp.StatusCode())
		require.Nil(t, deep.Equal(statResp.JSON200, copyStat))
	})

	// Copying different accounts takes more time and allows us to abort the copy in the middle
	t.Run("copy_large_size_file_abort", func(t *testing.T) {
		RequireBlockstoreType(t, block.BlockstoreTypeAzure)
		importPath := strings.Replace(azureCopyDataPath, "esti", azureAbortAccount, 1)
		const ingestionBranch = "test-copy-abort"
		_ = testImportNew(t, ctx, repo, ingestionBranch,
			[]apigen.ImportLocation{{Path: importPath, Type: "common_prefix"}},
			map[string]string{"created_by": "import"},
			false,
		)

		res, err := client.StatObjectWithResponse(ctx, repo, ingestionBranch, &apigen.StatObjectParams{
			Path: largeObject,
		})
		require.NoError(t, err)
		require.NotNil(t, res.JSON200)

		destPath := "bar"
		srcBranch := ingestionBranch
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		var (
			wg       sync.WaitGroup
			copyResp *apigen.CopyObjectResponse
			copyErr  error
		)
		// Run copy object async and cancel context after 5 seconds
		wg.Add(1)
		go func() {
			defer wg.Done()
			copyResp, copyErr = client.CopyObjectWithResponse(cancelCtx, repo, "main", &apigen.CopyObjectParams{
				DestPath: destPath,
			}, apigen.CopyObjectJSONRequestBody{
				SrcPath: largeObject,
				SrcRef:  &srcBranch,
			})
		}()

		time.Sleep(5 * time.Second)
		cancel()
		wg.Wait()
		require.ErrorIs(t, copyErr, context.Canceled)
		require.Nil(t, copyResp)

		// Verify object doesn't exist
		statResp, err := client.StatObjectWithResponse(ctx, repo, "main", &apigen.StatObjectParams{Path: destPath})
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, statResp.StatusCode())
	})

	t.Run("read-only repository", func(t *testing.T) {
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

		importPath := getImportPath(t)

		const ingestionBranch = "test-copy"

		_ = testImportNew(t, ctx, repoName, ingestionBranch,
			[]apigen.ImportLocation{{Path: importPath, Type: "common_prefix"}},
			map[string]string{"created_by": "import"},
			true,
		)

		res, err := client.StatObjectWithResponse(ctx, repoName, ingestionBranch, &apigen.StatObjectParams{
			Path: largeObject,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())

		objStat := res.JSON200
		destPath := "foo"
		srcBranch := ingestionBranch
		copyResp, err := client.CopyObjectWithResponse(ctx, repoName, "main", &apigen.CopyObjectParams{
			DestPath: destPath,
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: largeObject,
			SrcRef:  &srcBranch,
		})
		require.NoError(t, err, "failed to copy")
		if copyResp.StatusCode() != http.StatusForbidden {
			t.Fatalf("expected 403 forbidden error for CopyObject on read-only repository, got %d status code instead", copyResp.StatusCode())
		}

		copyResp, err = client.CopyObjectWithResponse(ctx, repoName, "main", &apigen.CopyObjectParams{
			DestPath: destPath,
		}, apigen.CopyObjectJSONRequestBody{
			SrcPath: largeObject,
			SrcRef:  &srcBranch,
			Force:   swag.Bool(true),
		})
		require.NoError(t, err, "failed to copy")
		require.NotNil(t, copyResp.JSON201)

		// Verify the creation path, date and physical address are different
		copyStat := copyResp.JSON201
		require.NotEqual(t, objStat.PhysicalAddress, copyStat.PhysicalAddress)
		require.GreaterOrEqual(t, copyStat.Mtime, objStat.Mtime)
		require.Equal(t, destPath, copyStat.Path)

		// Verify all else is equal
		objStat.Mtime = copyStat.Mtime
		objStat.Path = copyStat.Path
		objStat.PhysicalAddress = copyStat.PhysicalAddress
		require.Nil(t, deep.Equal(objStat, copyStat))

		// get back info
		statResp, err := client.StatObjectWithResponse(ctx, repoName, "main", &apigen.StatObjectParams{Path: destPath})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, statResp.StatusCode())
		require.Nil(t, deep.Equal(statResp.JSON200, copyStat))
	})
}

func getImportPath(t *testing.T) string {
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
	return importPath
}
