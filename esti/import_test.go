package esti

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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
	importBranchBase   = "ingestion"
)

func TestImport(t *testing.T) {
	const defaultExpectedContentLength = 1024

	importPath := ""
	expectedContentLength := defaultExpectedContentLength
	blockstoreType := viper.GetString(config.BlockstoreTypeKey)
	switch blockstoreType {
	case block.BlockstoreTypeS3:
		importPath = s3ImportPath
	case block.BlockstoreTypeGS:
		importPath = gsImportPath
	case block.BlockstoreTypeAzure:
		importPath = azureImportPath
	case block.BlockstoreTypeLocal:
		importPath = setupLocalImportPath(t)
		expectedContentLength = 0
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
		importBranch := fmt.Sprintf("%s-%s", importBranchBase, "default")
		testImport(t, ctx, repoName, importPath, importBranch)
		verifyImportObjects(t, ctx, repoName, importTargetPrefix, importBranch, importFilesToCheck, expectedContentLength)
	})

	t.Run("parent", func(t *testing.T) {
		importBranch := fmt.Sprintf("%s-%s", importBranchBase, "parent")
		if blockstoreType == block.BlockstoreTypeLocal {
			t.Skip("local always assumes import path is dir")
		}
		// import without the directory separator as suffix to include the parent directory
		importPathParent := strings.TrimSuffix(importPath, "/")
		testImport(t, ctx, repoName, importPathParent, importBranch)
		verifyImportObjects(t, ctx, repoName, importTargetPrefix+"import-test-data/", importBranch, importFilesToCheck, expectedContentLength)
	})
}

func setupLocalImportPath(t *testing.T) string {
	const dirPerm = 0o755
	importDir := filepath.Join(t.TempDir(), "import-test-data")
	if err := os.Mkdir(importDir, dirPerm); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 7; i++ {
		prefix := "prefix-" + strconv.Itoa(i+1)
		// make dirs once
		dirs := []string{
			filepath.Join(importDir, prefix),
			filepath.Join(importDir, "nested", prefix),
		}
		for _, dir := range dirs {
			if err := os.MkdirAll(dir, dirPerm); err != nil {
				t.Fatal(err)
			}
		}
		for n := 0; n < 2100; n++ {
			name := fmt.Sprintf("file%06d", n+1)
			const filePerm = 0o644
			for _, dir := range dirs {
				fn := filepath.Join(dir, name)
				if err := os.WriteFile(fn, []byte(fn), filePerm); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	return "local://" + importDir
}

func verifyImportObjects(t *testing.T, ctx context.Context, repoName, prefix, importBranch string, importFilesToCheck []string, expectedContentLength int) {
	for _, k := range importFilesToCheck {
		// try to read some values from that ingested branch
		objPath := prefix + k
		objResp, err := client.GetObjectWithResponse(ctx, repoName, importBranch, &api.GetObjectParams{
			Path: objPath,
		})
		require.NoError(t, err, "get object failed: %s", objPath)
		require.Equal(t, http.StatusOK, objResp.StatusCode(), "get object %s", objPath)
		if expectedContentLength > 0 {
			require.Equal(t, expectedContentLength, int(objResp.HTTPResponse.ContentLength), "object content length %s", objPath)
		}
	}
	hasMore := true
	after := api.PaginationAfter("")
	for hasMore {
		listResp, err := client.ListObjectsWithResponse(ctx, repoName, importBranch, &api.ListObjectsParams{After: &after})
		require.NoError(t, err, "list objects failed")
		require.NotNil(t, listResp.JSON200)

		hasMore = listResp.JSON200.Pagination.HasMore
		after = api.PaginationAfter(listResp.JSON200.Pagination.NextOffset)
		for _, obj := range listResp.JSON200.Results {
			require.True(t, strings.HasPrefix(obj.Path, prefix), "obj with wrong prefix imported", obj.Path, prefix)
		}
	}
}

func testImport(t *testing.T, ctx context.Context, repoName, importPath, importBranch string) {
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

	createResp, err := client.CreateBranchWithResponse(ctx, repoName, api.CreateBranchJSONRequestBody{
		Name:   importBranch,
		Source: "main",
	})
	require.NoError(t, err, "failed to create branch")
	require.Equal(t, http.StatusCreated, createResp.StatusCode())

	commitResp, err := client.CommitWithResponse(ctx, repoName, importBranch, &api.CommitParams{
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

func TestAzureDataLakeV2(t *testing.T) {
	importPrefix := viper.GetString("adls_import_base_url")
	if importPrefix == "" {
		t.Skip("No Azure data lake storage path prefix was given")
	}

	ctx, _, repoName := setupTest(t)
	defer tearDownTest(repoName)

	tests := []struct {
		name         string
		filesToCheck []string
	}{
		{
			name:         "empty-folders",
			filesToCheck: []string{},
		},
		{
			name: "prefix-item-order",
			filesToCheck: []string{
				"aaa",
				"helloworld.csv",
				"zero",
				"helloworld/myfile.csv",
			},
		},
	}

	for _, tt := range tests {
		importBranch := fmt.Sprintf("%s-%s", importBranchBase, tt.name)
		// each test is a folder under the prefix import
		t.Run(tt.name, func(t *testing.T) {
			importPath, err := url.JoinPath(importPrefix, tt.name)
			if err != nil {
				t.Fatal("Import URL", err)
			}
			testImport(t, ctx, repoName, importPath, importBranch)
			if len(tt.filesToCheck) == 0 {
				resp, err := client.ListObjectsWithResponse(ctx, repoName, importBranch, &api.ListObjectsParams{})
				require.NoError(t, err)
				require.NotNil(t, resp.JSON200)
				require.Empty(t, resp.JSON200.Results)
			} else {
				verifyImportObjects(t, ctx, repoName, importTargetPrefix, importBranch, tt.filesToCheck, 0)
			}
		})
	}
}
