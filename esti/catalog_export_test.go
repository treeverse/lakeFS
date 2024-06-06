package esti

import (
	"bytes"
	"context"
	"embed"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cenkalti/backoff/v4"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/block/params"
	lakefscfg "github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v3"
)

//go:embed all:export_hooks_files
var exportHooksFiles embed.FS

const catalogExportTestMaxElapsed = 30 * time.Second

type schemaField struct {
	Name       string            `yaml:"name"`
	Type       string            `yaml:"type"`
	IsNullable bool              `yaml:"nullable"`
	Metadata   map[string]string `yaml:"metadata,omitempty"`
}
type tableSchema struct {
	Type   string        `yaml:"type"`
	Fields []schemaField `yaml:"fields"`
}

type hiveTableSpec struct {
	Name             string      `yaml:"name"`
	Type             string      `yaml:"type"`
	Path             string      `yaml:"path"`
	PartitionColumns []string    `yaml:"partition_columns"`
	Schema           tableSchema `yaml:"schema"`
}

type exportHooksTestData struct {
	Repository            string
	SymlinkActionPath     string
	SymlinkScriptPath     string
	TableDescriptorPath   string
	GlueActionPath        string
	GlueScriptPath        string
	Branch                string
	GlueDB                string
	AWSAccessKeyID        string
	AWSSecretAccessKey    string
	AWSRegion             string
	AzureAccessKey        string
	AzureStorageAccount   string
	OverrideCommitID      string
	LakeFSAccessKeyID     string
	LakeFSSecretAccessKey string
	TableSpec             *hiveTableSpec
}

func renderTplFileAsStr(t *testing.T, tplData any, rootDir fs.FS, path string) string {
	t.Helper()
	tpl, err := template.ParseFS(rootDir, path)
	require.NoError(t, err, "rendering template")
	var doc bytes.Buffer
	err = tpl.Execute(&doc, tplData)
	require.NoError(t, err)
	return doc.String()
}

func setupGlueClient(ctx context.Context, accessKeyID, secretAccessKey, region string) (*glue.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
	)
	if err != nil {
		return nil, err
	}
	return glue.NewFromConfig(cfg), nil
}

func uploadAndCommitObjects(t *testing.T, ctx context.Context, repo, branch string, objectsGroups ...map[string]string) *apigen.Commit {
	t.Helper()
	for _, objects := range objectsGroups {
		for path, obj := range objects {
			resp, err := uploadContent(ctx, repo, branch, path, obj)
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, resp.StatusCode())
		}
	}

	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "Table Data",
	})
	require.NoErrorf(t, err, "failed commiting uploaded objects to lakefs://%s/%s", repo, branch)
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())
	return commitResp.JSON201
}

// genCSVData will create n+rows (with header) each cell value is rowNum+colNum
func genCSVData(t *testing.T, columns []string, n int) string {
	t.Helper()
	// Create a new CSV writer.
	buf := new(bytes.Buffer)
	writer := csv.NewWriter(buf)
	// Write the header row.
	headerRow := columns
	err := writer.Write(headerRow)
	require.NoError(t, err, "failed writing CSV headers to buffer")

	for rowNum := 0; rowNum < n; rowNum++ {
		dataRow := []string{}
		for colNum := range columns {
			dataRow = append(dataRow, fmt.Sprintf("%d", rowNum+colNum))
		}
		err = writer.Write(dataRow)
		require.NoError(t, err, "failed writing CSV row to buffer")
	}
	writer.Flush()
	csvContent := buf.String()
	return csvContent
}

func testSymlinkS3Exporter(t *testing.T, ctx context.Context, repo string, tmplDir fs.FS, tablePaths map[string]string, testData *exportHooksTestData) (string, string) {
	t.Helper()

	tableYaml, err := yaml.Marshal(&testData.TableSpec)

	if err != nil {
		require.NoError(t, err, "failed marshaling table spec to YAML")
	}

	hookFiles := map[string]string{
		testData.SymlinkScriptPath:   renderTplFileAsStr(t, testData, tmplDir, testData.SymlinkScriptPath),
		testData.TableDescriptorPath: string(tableYaml),
		testData.SymlinkActionPath:   renderTplFileAsStr(t, testData, tmplDir, testData.SymlinkActionPath),
	}

	// upload all files (hook, lua, table data)
	commit := uploadAndCommitObjects(t, ctx, repo, mainBranch, tablePaths, hookFiles)

	// wait until actions finish running
	runs := waitForListRepositoryRunsLen(ctx, t, repo, commit.Id, 1)
	require.Equal(t, "completed", runs.Results[0].Status, "symlink action result not finished")

	// list symlink.txt files from blockstore

	repoResponse, err := client.GetRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "could not get repository information")
	require.Equal(t, repoResponse.StatusCode(), http.StatusOK, "could not get repository information")
	namespace := repoResponse.JSON200.StorageNamespace

	symlinksPrefix := fmt.Sprintf("%s/_lakefs/exported/%s/%s/animals/", namespace, mainBranch, commit.Id[:6])
	storageURL, err := url.Parse(symlinksPrefix)
	require.NoError(t, err, "failed extracting bucket name")

	key := testData.AWSAccessKeyID
	secret := testData.AWSSecretAccessKey
	endpoint := "https://s3.amazonaws.com"
	forcePathStyle := viper.GetBool("force_path_style")
	s3Client, err := testutil.SetupTestS3Client(endpoint, key, secret, forcePathStyle)

	require.NoError(t, err, "failed creating s3 client")

	// we need to retry for the result
	var symlinkLocations []string

	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = 5 * time.Second
	bo.MaxElapsedTime = catalogExportTestMaxElapsed
	listS3Func := func() error {
		listResp, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(storageURL.Host),
			Prefix: aws.String(storageURL.Path[1:]), // trim leading slash
		})
		require.NoErrorf(t, err, "listing symlink files in storage: %s", symlinksPrefix)
		if len(listResp.Contents) == 0 {
			return errors.New("no symlink files found in blockstore")
		}
		for _, f := range listResp.Contents {
			symlinkLocations = append(symlinkLocations, aws.ToString(f.Key))
		}
		return nil
	}

	err = backoff.Retry(listS3Func, bo)

	require.NoErrorf(t, err, "failed listing symlink files %s", symlinksPrefix)
	require.NotEmptyf(t, symlinkLocations, "no symlink files found in blockstore: %s", symlinksPrefix)

	// get the symlink files and compare their physical address to lakefs result
	storagePhysicalAddrs := map[string]bool{}
	for _, symlinkFileKey := range symlinkLocations {
		objRes, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(storageURL.Host),
			Key:    aws.String(symlinkFileKey),
		})
		require.NoErrorf(t, err, "getting symlink file content bucket=%s key=%s", storageURL.Host, symlinkFileKey)
		body, err := io.ReadAll(objRes.Body)
		require.NoError(t, err, "fail reading object data")
		require.NoError(t, objRes.Body.Close())

		for _, addr := range strings.Split(string(body), "\n") {
			if addr != "" { // split returns last \n as empty string
				storagePhysicalAddrs[addr] = true
			}
		}
	}

	lakeFSObjs, err := client.ListObjectsWithResponse(ctx, repo, commit.Id, &apigen.ListObjectsParams{
		Prefix: apiutil.Ptr(apigen.PaginationPrefix(testData.TableSpec.Path)),
	})

	require.NoError(t, err, "failed listing lakefs objects")

	// test that all lakeFS entries are exported and represented correctly in symlink files
	lakefsPhysicalAddrs := map[string]bool{}
	for _, entry := range lakeFSObjs.JSON200.Results {
		// add file to the expected result if it's not hidden and not marker directory file
		if !strings.Contains(entry.Path, "_hidden") && aws.ToInt64(entry.SizeBytes) > 0 {
			lakefsPhysicalAddrs[entry.PhysicalAddress] = true
		}
	}

	require.Equal(t, lakefsPhysicalAddrs, storagePhysicalAddrs, "mismatch between lakefs exported objects in symlink files")

	return commit.Id, symlinksPrefix
}

// TestAWSCatalogExport will verify that symlinks are exported correctly and then in a sequential test verify that the glue exporter works well.
// The setup in this test includes:
// Symlinks export: lua script, table in _lakefs_tables, action file, mock table data in CSV form
// Glue export: lua script, table in _lakefs_tables, action file
func TestAWSCatalogExport(t *testing.T) {
	// skip if blockstore is not not s3
	requireBlockstoreType(t, block.BlockstoreTypeS3)
	// skip if the following args are not provided
	accessKeyID := viper.GetString("aws_access_key_id")
	secretAccessKey := viper.GetString("aws_secret_access_key")
	glueDB := viper.GetString("glue_export_hooks_database")
	glueRegion := viper.GetString("glue_export_region")
	requiredArgs := []string{accessKeyID, secretAccessKey, glueDB, glueRegion}
	if slices.Contains(requiredArgs, "") {
		t.Skip("One of the required Args empty")
	}

	var (
		glueTable     *types.Table
		commitID      string
		symlinkPrefix string
	)
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	tmplDir, _ := fs.Sub(exportHooksFiles, "export_hooks_files/glue")
	testData := &exportHooksTestData{
		Branch:              mainBranch,
		SymlinkActionPath:   "_lakefs_actions/symlink_export.yaml",
		SymlinkScriptPath:   "scripts/symlink_exporter.lua",
		GlueScriptPath:      "scripts/glue_exporter.lua",
		GlueActionPath:      "_lakefs_actions/glue_export.yaml",
		TableDescriptorPath: "_lakefs_tables/animals.yaml",
		GlueDB:              glueDB,
		AWSRegion:           glueRegion,
		AWSAccessKeyID:      accessKeyID,
		AWSSecretAccessKey:  secretAccessKey,
		TableSpec: &hiveTableSpec{
			Name:             "animals",
			Type:             "hive",
			Path:             "tables/animals",
			PartitionColumns: []string{"type", "weight"},
			Schema: tableSchema{
				Type: "struct",
				Fields: []schemaField{
					{Name: "type", Type: "string", Metadata: map[string]string{"comment": "axolotl, cat, dog, fish etc"}},
					{Name: "weight", Type: "integer"},
					{Name: "name", Type: "string"},
					{Name: "color", Type: "string", IsNullable: true},
				},
			},
		},
	}

	t.Run("symlink_exporter", func(t *testing.T) {
		var columns = []string{"name", "color"}
		csvData := genCSVData(t, columns, 3)
		tablePaths := map[string]string{
			testData.TableSpec.Path + "/type=axolotl/weight=22/b.csv": csvData,
			testData.TableSpec.Path + "/type=axolotl/weight=22/a.csv": csvData,
			// empty file simulates directory marker
			testData.TableSpec.Path + "/type=axolotl/weight=22/":        "",
			testData.TableSpec.Path + "/type=axolotl/weight=22/c.csv":   csvData,
			testData.TableSpec.Path + "/type=axolotl/weight=12/a.csv":   csvData,
			testData.TableSpec.Path + "/type=axolotl/weight=12/_hidden": "blob",
			testData.TableSpec.Path + "/type=cat/weight=33/a.csv":       csvData,
			testData.TableSpec.Path + "/type=dog/weight=10/b.csv":       csvData,
			testData.TableSpec.Path + "/type=dog/weight=10/a.csv":       csvData,
		}
		commitID, symlinkPrefix = testSymlinkS3Exporter(t, ctx, repo, tmplDir, tablePaths, testData)
		t.Logf("commit id %s symlinks prefix %s", commitID, symlinkPrefix)
	})
	t.Run("glue_exporter", func(t *testing.T) {
		// override commit ID to make the export table point to the previous commit of data
		testData.OverrideCommitID = commitID
		headCommit := uploadAndCommitObjects(t, ctx, repo, mainBranch, map[string]string{
			testData.GlueScriptPath: renderTplFileAsStr(t, testData, tmplDir, testData.GlueScriptPath),
			testData.GlueActionPath: renderTplFileAsStr(t, testData, tmplDir, testData.GlueActionPath),
		})

		// wait for action to finish
		runs := waitForListRepositoryRunsLen(ctx, t, repo, headCommit.Id, 1)
		require.Equal(t, "completed", runs.Results[0].Status, "glue action result not finished")

		// create glue client

		glueClient, err := setupGlueClient(ctx, testData.AWSAccessKeyID, testData.AWSSecretAccessKey, testData.AWSRegion)
		require.NoError(t, err, "creating glue client")

		// wait until table is ready
		tableName := fmt.Sprintf("%s_%s_%s_%s", testData.TableSpec.Name, repo, mainBranch, commitID[:6])
		bo := backoff.NewExponentialBackOff()
		bo.MaxInterval = 5 * time.Second
		bo.MaxElapsedTime = catalogExportTestMaxElapsed
		getGlueTable := func() error {
			t.Logf("[retry] get table %s ", tableName)
			resp, err := glueClient.GetTable(ctx, &glue.GetTableInput{
				DatabaseName: aws.String(testData.GlueDB),
				Name:         aws.String(tableName),
			})
			if err != nil {
				return fmt.Errorf("glue getTable %s/%s: %w", testData.GlueDB, tableName, err)
			}
			glueTable = resp.Table
			return nil
		}
		err = backoff.Retry(getGlueTable, bo)
		require.NoError(t, err, "glue table not created in time")

		// delete table when the test is over

		t.Cleanup(func() {
			_, err = glueClient.DeleteTable(ctx, &glue.DeleteTableInput{
				DatabaseName: aws.String(testData.GlueDB),
				Name:         aws.String(tableName),
			})
			if err != nil {
				t.Errorf("failed cleanup of glue table %s/%s ", testData.GlueDB, tableName)
			}
		})

		// verify table partitions
		require.Equal(t, len(testData.TableSpec.PartitionColumns), len(glueTable.PartitionKeys), "partitions created in glue doesnt match in size")
		partitionsSet := map[string]bool{}
		for _, partCol := range glueTable.PartitionKeys {
			name := aws.ToString(partCol.Name)
			require.Contains(t, testData.TableSpec.PartitionColumns, name, "glue partition not in table spec")
			partitionsSet[name] = true
		}

		// verify table columns
		glueCols := map[string]bool{}
		for _, col := range glueTable.StorageDescriptor.Columns {
			glueCols[aws.ToString(col.Name)] = true
		}
		for _, expCol := range testData.TableSpec.Schema.Fields {
			// if not a partition, regular column compare
			if !partitionsSet[expCol.Name] {
				require.Contains(t, glueCols, expCol.Name, "column not found in glue table schema")
			}
		}

		// verify table storage properties

		require.Equal(t, "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat", aws.ToString(glueTable.StorageDescriptor.InputFormat), "wrong table input format")
		require.Equal(t, "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", aws.ToString(glueTable.StorageDescriptor.OutputFormat), "wrong table output format")
		require.Equal(t, symlinkPrefix, aws.ToString(glueTable.StorageDescriptor.Location)+"/", "wrong s3 location in glue table")
	})
}

func setupCatalogExportTestByStorageType(t *testing.T, testData *exportHooksTestData) string {
	blockstoreType := viper.GetString(lakefscfg.BlockstoreTypeKey)

	switch blockstoreType {
	case block.BlockstoreTypeS3:
		testData.AWSAccessKeyID = viper.GetString("aws_access_key_id")
		testData.AWSSecretAccessKey = viper.GetString("aws_secret_access_key")
		testData.AWSRegion = "us-east-1"

	case block.BlockstoreTypeAzure:
		testData.AzureStorageAccount = viper.GetString("azure_storage_account")
		testData.AzureAccessKey = viper.GetString("azure_storage_access_key")

	default:
		t.Skip("unsupported block adapter: ", blockstoreType)
	}

	return blockstoreType
}

func validateExportTestByStorageType(t *testing.T, ctx context.Context, commit string, testData *exportHooksTestData, blockstoreType string) {
	resp, err := client.GetRepositoryWithResponse(ctx, testData.Repository)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	namespaceURL, err := url.Parse(resp.JSON200.StorageNamespace)
	require.NoError(t, err)
	keyTempl := "%s/_lakefs/exported/%s/%s/test_table/_delta_log/00000000000000000000.json"
	tableStat, err := client.StatObjectWithResponse(ctx, testData.Repository, mainBranch, &apigen.StatObjectParams{
		Path: "tables/test-table/test partition/0-845b8a42-579e-47ee-9935-921dd8d2ba7d-0.parquet",
	})
	require.NoError(t, err)
	require.NotNil(t, tableStat.JSON200)
	expectedPath, err := url.Parse(tableStat.JSON200.PhysicalAddress)
	require.NoError(t, err)

	var reader io.ReadCloser
	switch blockstoreType {
	case block.BlockstoreTypeS3:
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(testData.AWSRegion),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testData.AWSAccessKeyID, testData.AWSSecretAccessKey, "")),
		)
		require.NoError(t, err)

		clt := s3.NewFromConfig(cfg)
		key := fmt.Sprintf(keyTempl, strings.TrimPrefix(namespaceURL.Path, "/"), mainBranch, commit[:6])
		readResp, err := clt.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(namespaceURL.Host),
			Key:    aws.String(key)})
		require.NoError(t, err)
		reader = readResp.Body

	case block.BlockstoreTypeAzure:
		azClient, err := azure.BuildAzureServiceClient(params.Azure{
			StorageAccount:   testData.AzureStorageAccount,
			StorageAccessKey: testData.AzureAccessKey,
		})
		require.NoError(t, err)

		containerName, prefix, _ := strings.Cut(namespaceURL.Path, uri.PathSeparator)
		key := fmt.Sprintf(keyTempl, strings.TrimPrefix(prefix, "/"), mainBranch, commit[:6])
		readResp, err := azClient.NewContainerClient(containerName).NewBlockBlobClient(key).DownloadStream(ctx, nil)
		require.NoError(t, err)
		reader = readResp.Body

	default:
		t.Fatal("validation failed on unsupported block adapter")
	}

	defer func() {
		err := reader.Close()
		require.NoError(t, err)
	}()
	contents, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Contains(t, string(contents), expectedPath.String())
}

func TestDeltaCatalogExport(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	testData := &exportHooksTestData{
		Repository:            repo,
		Branch:                mainBranch,
		LakeFSAccessKeyID:     accessKeyID,
		LakeFSSecretAccessKey: secretAccessKey,
	}
	blockstore := setupCatalogExportTestByStorageType(t, testData)

	tmplDir, err := fs.Sub(exportHooksFiles, "export_hooks_files/delta")
	require.NoError(t, err)
	err = fs.WalkDir(tmplDir, "data", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			buf, err := fs.ReadFile(tmplDir, path)
			if err != nil {
				return err
			}
			uploadResp, err := uploadContent(ctx, repo, mainBranch, strings.TrimPrefix(path, "data/"), string(buf))
			if err != nil {
				return err
			}
			require.Equal(t, http.StatusCreated, uploadResp.StatusCode())
		}
		return nil
	})
	require.NoError(t, err)

	headCommit := uploadAndCommitObjects(t, ctx, repo, mainBranch, map[string]string{
		"_lakefs_actions/delta_export.yaml": renderTplFileAsStr(t, testData, tmplDir, fmt.Sprintf("%s/_lakefs_actions/delta_export.yaml", blockstore)),
	})

	runs := waitForListRepositoryRunsLen(ctx, t, repo, headCommit.Id, 1)
	run := runs.Results[0]
	require.Equal(t, "completed", run.Status)

	amount := apigen.PaginationAmount(1)
	tasks, err := client.ListRunHooksWithResponse(ctx, repo, run.RunId, &apigen.ListRunHooksParams{
		Amount: &amount,
	})
	require.NoError(t, err)
	require.NotNil(t, tasks.JSON200)
	require.Equal(t, 1, len(tasks.JSON200.Results))
	require.Equal(t, "delta_exporter", tasks.JSON200.Results[0].HookId)
	validateExportTestByStorageType(t, ctx, headCommit.Id, testData, blockstore)
}

func TestDeltaCatalogImportExport(t *testing.T) {
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	requireBlockstoreType(t, block.BlockstoreTypeS3)
	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	testData := &exportHooksTestData{
		Repository:            repo,
		Branch:                mainBranch,
		LakeFSAccessKeyID:     accessKeyID,
		LakeFSSecretAccessKey: secretAccessKey,
	}
	blockstore := setupCatalogExportTestByStorageType(t, testData)
	tmplDir, err := fs.Sub(exportHooksFiles, "export_hooks_files/delta")
	require.NoError(t, err)
	err = fs.WalkDir(tmplDir, "data", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			buf, err := fs.ReadFile(tmplDir, path)
			if err != nil {
				return err
			}
			uploadToPhysicalAddress(t, ctx, repo, mainBranch, strings.TrimPrefix(path, "data/"), string(buf))
		}
		return nil
	})
	require.NoError(t, err)

	headCommit := uploadAndCommitObjects(t, ctx, repo, mainBranch, map[string]string{
		"_lakefs_actions/delta_export.yaml": renderTplFileAsStr(t, testData, tmplDir, fmt.Sprintf("%s/_lakefs_actions/delta_export.yaml", blockstore)),
	})

	runs := waitForListRepositoryRunsLen(ctx, t, repo, headCommit.Id, 1)
	run := runs.Results[0]
	require.Equal(t, "completed", run.Status)

	amount := apigen.PaginationAmount(1)
	tasks, err := client.ListRunHooksWithResponse(ctx, repo, run.RunId, &apigen.ListRunHooksParams{
		Amount: &amount,
	})
	require.NoError(t, err)
	require.NotNil(t, tasks.JSON200)
	require.Equal(t, 1, len(tasks.JSON200.Results))
	require.Equal(t, "delta_exporter", tasks.JSON200.Results[0].HookId)
	validateExportTestByStorageType(t, ctx, headCommit.Id, testData, blockstore)
}

func uploadToPhysicalAddress(t *testing.T, ctx context.Context, repo, branch, objPath, objContent string) {
	t.Helper()
	physicalAddress, err := url.Parse(getStorageNamespace(t, ctx, repo))
	require.NoError(t, err)
	physicalAddress = physicalAddress.JoinPath("data", objPath)

	adapter, err := NewAdapter(physicalAddress.Scheme)
	require.NoError(t, err)

	stats, err := adapter.Upload(ctx, physicalAddress, strings.NewReader(objContent))
	require.NoError(t, err)

	mtime := stats.MTime.Unix()
	unescapedAddress, err := url.PathUnescape(physicalAddress.String()) // catch: https://github.com/treeverse/lakeFS/issues/7460
	require.NoError(t, err)

	resp, err := client.StageObjectWithResponse(ctx, repo, branch, &apigen.StageObjectParams{
		Path: objPath,
	}, apigen.StageObjectJSONRequestBody{
		Checksum:        stats.ETag,
		Mtime:           &mtime,
		PhysicalAddress: unescapedAddress,
		SizeBytes:       stats.Size,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON201)
}

func getStorageNamespace(t *testing.T, ctx context.Context, repo string) string {
	t.Helper()
	resp, err := client.GetRepositoryWithResponse(ctx, repo)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	return resp.JSON200.StorageNamespace
}

func TestDeltaCatalogExportAbfss(t *testing.T) {
	requireBlockstoreType(t, block.BlockstoreTypeAzure)
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	accessKeyID := viper.GetString("access_key_id")
	secretAccessKey := viper.GetString("secret_access_key")
	testData := &exportHooksTestData{
		Repository:            repo,
		Branch:                mainBranch,
		LakeFSAccessKeyID:     accessKeyID,
		LakeFSSecretAccessKey: secretAccessKey,
		AzureStorageAccount:   viper.GetString("azure_storage_account"),
		AzureAccessKey:        viper.GetString("azure_storage_access_key"),
	}

	tmplDir, err := fs.Sub(exportHooksFiles, "export_hooks_files/delta")
	require.NoError(t, err)
	err = fs.WalkDir(tmplDir, "data", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			buf, err := fs.ReadFile(tmplDir, path)
			if err != nil {
				return err
			}
			uploadResp, err := uploadContent(ctx, repo, mainBranch, strings.TrimPrefix(path, "data/"), string(buf))
			if err != nil {
				return err
			}
			require.Equal(t, http.StatusCreated, uploadResp.StatusCode())
		}
		return nil
	})
	require.NoError(t, err)

	headCommit := uploadAndCommitObjects(t, ctx, repo, mainBranch, map[string]string{
		"_lakefs_actions/delta_export.yaml": renderTplFileAsStr(t, testData, tmplDir, "azure_adls/_lakefs_actions/delta_export.yaml"),
	})

	runs := waitForListRepositoryRunsLen(ctx, t, repo, headCommit.Id, 1)
	run := runs.Results[0]
	require.Equal(t, "completed", run.Status)

	amount := apigen.PaginationAmount(1)
	tasks, err := client.ListRunHooksWithResponse(ctx, repo, run.RunId, &apigen.ListRunHooksParams{
		Amount: &amount,
	})
	require.NoError(t, err)
	require.NotNil(t, tasks.JSON200)
	require.Equal(t, 1, len(tasks.JSON200.Results))
	require.Equal(t, "delta_exporter", tasks.JSON200.Results[0].HookId)
	validateExportAbfss(t, ctx, headCommit.Id, testData)
}

func validateExportAbfss(t *testing.T, ctx context.Context, commit string, testData *exportHooksTestData) {
	resp, err := client.GetRepositoryWithResponse(ctx, testData.Repository)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	namespaceURL, err := url.Parse(resp.JSON200.StorageNamespace)
	require.NoError(t, err)
	keyTempl := "%s/_lakefs/exported/%s/%s/test_table/_delta_log/00000000000000000000.json"
	tableStat, err := client.StatObjectWithResponse(ctx, testData.Repository, mainBranch, &apigen.StatObjectParams{
		Path: "tables/test-table/test partition/0-845b8a42-579e-47ee-9935-921dd8d2ba7d-0.parquet",
	})
	require.NoError(t, err)
	require.NotNil(t, tableStat.JSON200)
	u, err := url.Parse(tableStat.JSON200.PhysicalAddress)
	require.NoError(t, err)
	storageAccount, _, found := strings.Cut(u.Host, ".")
	require.True(t, found)
	container, path, found := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
	require.True(t, found)
	expectedPath := fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/%s", container, storageAccount, path)
	var reader io.ReadCloser
	azClient, err := azure.BuildAzureServiceClient(params.Azure{
		StorageAccount:   testData.AzureStorageAccount,
		StorageAccessKey: testData.AzureAccessKey,
	})
	require.NoError(t, err)

	containerName, prefix, _ := strings.Cut(namespaceURL.Path, uri.PathSeparator)
	key := fmt.Sprintf(keyTempl, strings.TrimPrefix(prefix, "/"), mainBranch, commit[:6])
	readResp, err := azClient.NewContainerClient(containerName).NewBlockBlobClient(key).DownloadStream(ctx, nil)
	require.NoError(t, err)
	reader = readResp.Body

	defer func() {
		err := reader.Close()
		require.NoError(t, err)
	}()
	contents, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Contains(t, string(contents), expectedPath)
}
