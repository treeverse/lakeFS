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

	"github.com/rs/xid"
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
	adlsTestImportPath = "import-test-cases"
)

var importFilesToCheck = []string{
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

func verifyImportObjects(t testing.TB, ctx context.Context, repoName, prefix, importBranch string, importFilesToCheck []string, expectedContentLength int) {
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
	count := 0
	for hasMore {
		listResp, err := client.ListObjectsWithResponse(ctx, repoName, importBranch, &api.ListObjectsParams{After: &after})
		require.NoError(t, err, "list objects failed")
		require.NotNil(t, listResp.JSON200)

		hasMore = listResp.JSON200.Pagination.HasMore
		after = api.PaginationAfter(listResp.JSON200.Pagination.NextOffset)
		prev := api.ObjectStats{}
		for _, obj := range listResp.JSON200.Results {
			count++
			require.True(t, strings.HasPrefix(obj.Path, prefix), "obj with wrong prefix imported", obj.Path, prefix)
			require.True(t, strings.Compare(prev.Path, obj.Path) < 0, "Wrong listing order", prev.Path, obj.Path)
			prev = obj
		}
	}
	t.Log("Total objects imported:", count)
}

func ingestRange(t testing.TB, ctx context.Context, repoName, importPath string) []api.RangeMetadata {
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
	return ranges
}

func testImport(t testing.TB, ctx context.Context, repoName, importPath, importBranch string) {
	ranges := ingestRange(t, ctx, repoName, importPath)

	metarangeResp, err := client.CreateMetaRangeWithResponse(ctx, repoName, api.CreateMetaRangeJSONRequestBody{
		Ranges: ranges,
	})

	require.NoError(t, err, "failed to create metarange")
	require.Equal(t, http.StatusCreated, metarangeResp.StatusCode())
	require.NotNil(t, metarangeResp.JSON201.Id, "failed to create metarange")

	createResp, err := client.CreateBranchWithResponse(ctx, repoName, api.CreateBranchJSONRequestBody{
		Name:   importBranch,
		Source: "main",
	})
	require.NoError(t, err, "failed to create branch", importBranch)
	require.Equal(t, http.StatusCreated, createResp.StatusCode(), "failed to create branch", importBranch)

	commitResp, err := client.CommitWithResponse(ctx, repoName, importBranch, &api.CommitParams{
		SourceMetarange: metarangeResp.JSON201.Id,
	}, api.CommitJSONRequestBody{
		Message: "created by import",
		Metadata: &api.CommitCreation_Metadata{
			AdditionalProperties: map[string]string{"created_by": "import"},
		},
	})
	require.NoError(t, err, "failed to commit")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode(), "failed to commit")
}

func TestAzureDataLakeV2(t *testing.T) {
	//importPrefix := viper.GetString("adls_import_base_url")
	importPrefix := "https://esti4hns.adls.core.windows.net/esti-system-testing-data/"
	//importPrefix := "https://esti4hns.blob.core.windows.net/esti-system-testing-data/"
	//importPrefix := "https://esti4hns.adls.core.windows.net/esti-system-testing-data/adls-big-import"
	if importPrefix == "" {
		t.Skip("No Azure data lake storage path prefix was given")
	}

	ctx, _, repoName := setupTest(t)
	defer tearDownTest(repoName)

	tests := []struct {
		name         string
		prefix       string
		filesToCheck []string
	}{
		{
			name:         "adls-big-import",
			prefix:       "",
			filesToCheck: []string{},
			//filesToCheck: importFilesToCheck,
		},
		//{
		//	name:         "empty-folders",
		//	prefix:       adlsTestImportPath,
		//	filesToCheck: []string{},
		////},
		//{
		//	name:   "prefix-item-order",
		//	prefix: adlsTestImportPath,
		//	filesToCheck: []string{
		//		"aaa",
		//		"helloworld.csv",
		//		"zero",
		//		"helloworld/myfile.csv",
		//	},
		//},
	}

	for _, tt := range tests {
		importBranch := fmt.Sprintf("%s-%s", importBranchBase, tt.name)
		// each test is a folder under the prefix import
		t.Run(tt.name, func(t *testing.T) {
			//importPath, err := url.JoinPath(importPrefix, tt.prefix)
			importPath, err := url.JoinPath(importPrefix, tt.prefix, tt.name)
			if err != nil {
				t.Fatal("Import URL", err)
			}
			testImport(t, ctx, repoName, importPath, importBranch)
			//if len(tt.filesToCheck) == 0 {
			//	resp, err := client.ListObjectsWithResponse(ctx, repoName, importBranch, &api.ListObjectsParams{})
			//	require.NoError(t, err)
			//	require.NotNil(t, resp.JSON200)
			//	require.Empty(t, resp.JSON200.Results)
			//} else {
			verifyImportObjects(t, ctx, repoName, importTargetPrefix, importBranch, tt.filesToCheck, 0)
			//}
		})
	}
}

func BenchmarkIngest_Azure(b *testing.B) {
	requireBlockstoreType(b, block.BlockstoreTypeAzure)
	ctx, _, repoName := setupTest(b)
	defer tearDownTest(repoName)

	b.Run("alds_gen2_ingest", func(b *testing.B) {
		importPrefix := viper.GetString("adls_import_base_url")
		if importPrefix == "" {
			b.Skip("No Azure data lake storage path prefix was given")
		}
		importPath, err := url.JoinPath(importPrefix, "import-test-data/")
		if err != nil {
			b.Fatal("Import URL", err)
		}
		benchmarkIngest(b, ctx, repoName, importPath)
	})

	b.Run("blob_storage_ingest", func(b *testing.B) {
		benchmarkIngest(b, ctx, repoName, azureImportPath)
		//var importFilesToCheck []string
		//verifyImportObjects(b, ctx, repoName, importTargetPrefix, importBranch, importFilesToCheck, 0) == 29400 objects
	})
}

func benchmarkIngest(b *testing.B, ctx context.Context, repoName, importPath string) {
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ingestRange(b, ctx, repoName, importPath)
	}
}

func BenchmarkImport_Azure(b *testing.B) {
	requireBlockstoreType(b, block.BlockstoreTypeAzure)
	ctx, _, repoName := setupTest(b)
	defer tearDownTest(repoName)

	b.Run("alds_gen2_import", func(b *testing.B) {
		importPrefix := viper.GetString("adls_import_base_url")
		if importPrefix == "" {
			b.Skip("No Azure data lake storage path prefix was given")
		}
		importBranch := fmt.Sprintf("%s-%s", importBranchBase, makeRepositoryName(b.Name()))
		importPath, err := url.JoinPath(importPrefix, "import-test-data/")
		if err != nil {
			b.Fatal("Import URL", err)
		}
		benchmarkImport(b, ctx, repoName, importPath, importBranch)
	})

	b.Run("blob_storage_import", func(b *testing.B) {
		importBranch := fmt.Sprintf("%s-%s", importBranchBase, makeRepositoryName(b.Name()))
		benchmarkImport(b, ctx, repoName, azureImportPath, importBranch)
		//var importFilesToCheck []string
		//verifyImportObjects(b, ctx, repoName, importTargetPrefix, importBranch, importFilesToCheck, 0) == 29400 objects
	})
}

func benchmarkImport(b *testing.B, ctx context.Context, repoName, importPath, importBranch string) {
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		testImport(b, ctx, repoName, importPath, fmt.Sprintf("%s-%s", importBranch, xid.New().String()))
	}
}
