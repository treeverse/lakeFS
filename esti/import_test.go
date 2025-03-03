package esti

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
)

const (
	defaultExpectedContentLength = 1024

	s3ImportPath       = "s3://esti-system-testing-data/import-test-data/"
	gsImportPath       = "gs://esti-system-testing-data/import-test-data/"
	azureImportPath    = "https://esti.blob.core.windows.net/esti-system-testing-data/import-test-data/"
	importTargetPrefix = "imported/new-prefix/"
	importBranchBase   = "ingestion"
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

func setupImportByBlockstoreType(t testing.TB) (string, string, int) {
	t.Helper()
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
	return blockstoreType, importPath, expectedContentLength
}

func setupLocalImportPath(t testing.TB) string {
	const dirPerm = 0o755
	importDir := filepath.Join(t.TempDir(), "import-test-data") + "/"
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
		objResp, err := client.GetObjectWithResponse(ctx, repoName, importBranch, &apigen.GetObjectParams{
			Path: objPath,
		})
		require.NoError(t, err, "get object failed: %s", objPath)
		require.Equal(t, http.StatusOK, objResp.StatusCode(), "get object %s", objPath)
		if expectedContentLength > 0 {
			require.Equal(t, expectedContentLength, int(objResp.HTTPResponse.ContentLength), "object content length %s", objPath)
		}
	}
	hasMore := true
	after := apigen.PaginationAfter("")
	count := 0
	for hasMore {
		listResp, err := client.ListObjectsWithResponse(ctx, repoName, importBranch, &apigen.ListObjectsParams{After: &after})
		require.NoError(t, err, "list objects failed")
		require.NotNil(t, listResp.JSON200)

		hasMore = listResp.JSON200.Pagination.HasMore
		after = apigen.PaginationAfter(listResp.JSON200.Pagination.NextOffset)
		prev := apigen.ObjectStats{}
		for _, obj := range listResp.JSON200.Results {
			count++
			require.True(t, strings.HasPrefix(obj.Path, prefix), "obj with wrong prefix imported", obj.Path, prefix)
			require.True(t, strings.Compare(prev.Path, obj.Path) < 0, "Wrong listing order", prev.Path, obj.Path)
			prev = obj
		}
	}
	t.Log("Total objects imported:", count)
}

func TestImport(t *testing.T) {
	blockstoreType, importPath, expectedContentLength := setupImportByBlockstoreType(t)
	metadata := map[string]string{"created_by": "import"}

	t.Run("default", func(t *testing.T) {
		ctx, _, repoName := setupTest(t)
		defer tearDownTest(repoName)
		branch := fmt.Sprintf("%s-%s", importBranchBase, "default")
		paths := []apigen.ImportLocation{{
			Destination: importTargetPrefix,
			Path:        importPath,
			Type:        catalog.ImportPathTypePrefix,
		}}
		importID := testImportNew(t, ctx, repoName, branch, paths, nil, false)
		verifyImportObjects(t, ctx, repoName, importTargetPrefix, branch, importFilesToCheck, expectedContentLength)

		// Verify we cannot cancel a completed import
		cancelResp, err := client.ImportCancelWithResponse(ctx, repoName, branch, &apigen.ImportCancelParams{
			Id: importID,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, cancelResp.StatusCode())

		statusResp, err := client.ImportStatusWithResponse(ctx, repoName, branch, &apigen.ImportStatusParams{
			Id: importID,
		})
		require.NoError(t, err)
		require.NotNil(t, statusResp.JSON200, "failed to get import status", err)
		require.Nil(t, statusResp.JSON200.Error)

		// Check import metadata was created on repository
		metadataResp, err := client.GetRepositoryMetadataWithResponse(ctx, repoName)
		require.NoError(t, err)
		require.NotNil(t, metadataResp.JSON200)

		repoMetadata := metadataResp.JSON200.AdditionalProperties
		require.NotNil(t, repoMetadata)
		importMetadata, ok := repoMetadata[graveler.MetadataKeyLastImportTimeStamp]
		require.True(t, ok)
		require.Equal(t, strconv.FormatInt(statusResp.JSON200.Commit.CreationDate, 10), importMetadata)
	})

	t.Run("parent", func(t *testing.T) {
		ctx, _, repoName := setupTest(t)
		defer tearDownTest(repoName)
		branch := fmt.Sprintf("%s-%s", importBranchBase, "parent")
		if blockstoreType == block.BlockstoreTypeLocal {
			t.Skip("local cannot import by prefix, only directory or object")
		}
		// import without the directory separator as suffix to include the parent directory
		importPathParent := strings.TrimSuffix(importPath, "/")
		paths := []apigen.ImportLocation{{
			Destination: importTargetPrefix,
			Path:        importPathParent,
			Type:        catalog.ImportPathTypePrefix,
		}}
		_ = testImportNew(t, ctx, repoName, branch, paths, metadata, false)
		verifyImportObjects(t, ctx, repoName, importTargetPrefix+"import-test-data/", branch, importFilesToCheck, expectedContentLength)
	})

	t.Run("several_paths", func(t *testing.T) {
		ctx, _, repoName := setupTest(t)
		defer tearDownTest(repoName)
		branch := fmt.Sprintf("%s-%s", importBranchBase, "several-paths")
		var paths []apigen.ImportLocation
		for i := 1; i < 8; i++ {
			prefix := fmt.Sprintf("prefix-%d/", i)
			paths = append(paths, apigen.ImportLocation{
				Destination: importTargetPrefix + prefix,
				Path:        importPath + prefix,
				Type:        catalog.ImportPathTypePrefix,
			})
		}
		paths = append(paths, apigen.ImportLocation{
			Destination: importTargetPrefix + "nested",
			Path:        importPath + "nested/",
			Type:        catalog.ImportPathTypePrefix,
		})

		_ = testImportNew(t, ctx, repoName, branch, paths, metadata, false)
		verifyImportObjects(t, ctx, repoName, importTargetPrefix, branch, importFilesToCheck, expectedContentLength)
	})

	t.Run("prefixes_and_objects", func(t *testing.T) {
		ctx, _, repoName := setupTest(t)
		defer tearDownTest(repoName)
		branch := fmt.Sprintf("%s-%s", importBranchBase, "prefixes-and-objects")
		var paths []apigen.ImportLocation
		for i := 1; i < 8; i++ {
			prefix := fmt.Sprintf("prefix-%d/", i)
			paths = append(paths, apigen.ImportLocation{
				Destination: importTargetPrefix + prefix,
				Path:        importPath + prefix,
				Type:        catalog.ImportPathTypePrefix,
			})
		}
		for _, p := range importFilesToCheck {
			if strings.HasPrefix(p, "nested") {
				paths = append(paths, apigen.ImportLocation{
					Destination: importTargetPrefix + p,
					Path:        importPath + p,
					Type:        catalog.ImportPathTypeObject,
				})
			}
		}
		_ = testImportNew(t, ctx, repoName, branch, paths, nil, false)
		verifyImportObjects(t, ctx, repoName, importTargetPrefix, branch, importFilesToCheck, expectedContentLength)
	})
}

func testImportNew(t testing.TB, ctx context.Context, repoName, importBranch string, paths []apigen.ImportLocation, metadata map[string]string, force bool) string {
	createResp, err := client.CreateBranchWithResponse(ctx, repoName, apigen.CreateBranchJSONRequestBody{
		Name:   importBranch,
		Source: "main",
		Force:  swag.Bool(force),
	})
	require.NoError(t, err, "failed to create branch", importBranch)
	require.Equal(t, http.StatusCreated, createResp.StatusCode(), "failed to create branch", importBranch)

	body := apigen.ImportStartJSONRequestBody{
		Commit: apigen.CommitCreation{
			Message: "created by import",
		},
		Paths: paths,
		Force: swag.Bool(force),
	}
	if len(metadata) > 0 {
		body.Commit.Metadata = &apigen.CommitCreation_Metadata{AdditionalProperties: metadata}
	}

	importResp, err := client.ImportStartWithResponse(ctx, repoName, importBranch, body)
	require.NoError(t, err, "failed to start import", importBranch)
	require.NotNil(t, importResp.JSON202, "failed to start import", importResp.Status())
	require.NotNil(t, importResp.JSON202.Id, "missing import ID")

	var (
		statusResp *apigen.ImportStatusResponse
		updateTime time.Time
	)
	importID := importResp.JSON202.Id

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		statusResp, err = client.ImportStatusWithResponse(ctx, repoName, importBranch, &apigen.ImportStatusParams{
			Id: importID,
		})
		require.NoError(t, err, "failed to get import status", importID)
		require.NotNil(t, statusResp.JSON200, "failed to get import status", err)
		status := statusResp.JSON200
		require.Nil(t, status.Error, "import failed", err)
		require.NotEqual(t, updateTime, status.UpdateTime)
		updateTime = status.UpdateTime
		t.Log("Import progress:", *status.IngestedObjects, importID)
		if status.Completed {
			break
		}
	}
	t.Log("Import completed:", importID)
	return importID
}

func TestImportCancel(t *testing.T) {
	_, importPath, _ := setupImportByBlockstoreType(t)
	ctx, _, repoName := setupTest(t)
	defer tearDownTest(repoName)
	branch := fmt.Sprintf("%s-%s", importBranchBase, "canceled")
	createResp, err := client.CreateBranchWithResponse(ctx, repoName, apigen.CreateBranchJSONRequestBody{
		Name:   branch,
		Source: "main",
	})
	require.NoError(t, err, "failed to create branch", branch)
	require.Equal(t, http.StatusCreated, createResp.StatusCode(), "failed to create branch", branch)

	importResp, err := client.ImportStartWithResponse(ctx, repoName, branch, apigen.ImportStartJSONRequestBody{
		Commit: apigen.CommitCreation{
			Message:  "created by import",
			Metadata: &apigen.CommitCreation_Metadata{AdditionalProperties: map[string]string{"created_by": "import"}},
		},
		Paths: []apigen.ImportLocation{{
			Destination: importTargetPrefix,
			Path:        importPath,
			Type:        catalog.ImportPathTypePrefix,
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, importResp.JSON202, "failed to start import", err)

	// Wait 1 second and cancel request
	time.Sleep(1 * time.Second)
	cancelResp, err := client.ImportCancelWithResponse(ctx, repoName, branch, &apigen.ImportCancelParams{
		Id: importResp.JSON202.Id,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, cancelResp.StatusCode())

	// Check status is canceled
	var updateTime time.Time
	timer := time.NewTimer(0)
	for range timer.C {
		statusResp, err := client.ImportStatusWithResponse(ctx, repoName, branch, &apigen.ImportStatusParams{
			Id: importResp.JSON202.Id,
		})
		require.NoError(t, err)
		require.NotNil(t, statusResp.JSON200, "failed to get import status", err)
		require.Contains(t, statusResp.JSON200.Error.Message, catalog.ImportCanceled)

		if updateTime.IsZero() {
			updateTime = statusResp.JSON200.UpdateTime
		} else {
			require.Equal(t, updateTime, statusResp.JSON200.UpdateTime)
			break
		}
		timer.Reset(3 * time.Second) // Server updates status every 1 second - unless operation was canceled successfully
	}
}
