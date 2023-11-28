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
	"github.com/treeverse/lakefs/pkg/testutil"
	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v3"
)

//go:embed export_hooks_files/*/*
var exportHooksFiles embed.FS

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
	SymlinkActionPath   string
	SymlinkScriptPath   string
	TableDescriptorPath string
	GlueActionPath      string
	GlueScriptPath      string
	Branch              string
	GlueDB              string
	AccessKeyId         string
	SecretAccessKey     string
	Region              string
	OverrideCommitID    string
	TableSpec           *hiveTableSpec
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

	s3Client, err := testutil.SetupTestS3Client("https://s3.amazonaws.com", testData.AccessKeyId, testData.SecretAccessKey)

	require.NoError(t, err, "failed creating s3 client")

	// we need to retry for the result
	var symlinkLocations []string

	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = 5 * time.Second
	bo.MaxElapsedTime = 30 * time.Second
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
		defer objRes.Body.Close()
		body, err := io.ReadAll(objRes.Body)
		require.NoError(t, err, "fail reading object data")
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
	// skip if blockstore is not s3
	requireBlockstoreType(t, block.BlockstoreTypeS3)
	// skip of the following args are not provided
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

	tmplDir, _ := fs.Sub(exportHooksFiles, "export_hooks_files")
	testData := &exportHooksTestData{
		Branch:              mainBranch,
		SymlinkActionPath:   "_lakefs_actions/symlink_export.yaml",
		SymlinkScriptPath:   "scripts/symlink_exporter.lua",
		GlueScriptPath:      "scripts/glue_exporter.lua",
		GlueActionPath:      "_lakefs_actions/glue_export.yaml",
		TableDescriptorPath: "_lakefs_tables/animals.yaml",
		GlueDB:              glueDB,
		Region:              glueRegion,
		AccessKeyId:         accessKeyID,
		SecretAccessKey:     secretAccessKey,
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

		glueClient, err := setupGlueClient(ctx, testData.AccessKeyId, testData.SecretAccessKey, testData.Region)
		require.NoError(t, err, "creating glue client")

		// wait until table is ready
		tableName := fmt.Sprintf("%s_%s_%s_%s", testData.TableSpec.Name, repo, mainBranch, commitID[:6])
		bo := backoff.NewExponentialBackOff()
		bo.MaxInterval = 5 * time.Second
		bo.MaxElapsedTime = 30 * time.Second
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
