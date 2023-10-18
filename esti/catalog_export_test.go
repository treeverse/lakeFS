package esti

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	"github.com/treeverse/lakefs/pkg/testutil"
	"gopkg.in/yaml.v3"
)

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
	// Name             string
	// TablePrefix      string
	SymlinkScriptPath   string
	TableDescriptorPath string
	GlueScriptPath      string
	Branch              string
	GlueDB              string
	AccessKeyId         string
	SecretAccessKey     string
	OverrideCommitID    string
	TableSpec           *hiveTableSpec
}

const glueExportScript = `
local aws = require("aws")
local exporter = require("lakefs/catalogexport/glue_exporter")
action.commit_id = "{{ .OverrideCommitID }}" -- override commit id to use specific symlink file previously created
local glue = aws.glue_client(args.aws.aws_access_key_id, args.aws.aws_secret_access_key, args.aws.aws_region)
exporter.export_glue(glue, args.catalog.db_name, args.table_source, args.catalog.table_input, action, {debug=true})
`

const glueExporterAction = `
name: Glue Exporter
on:
  post-commit:
    branches: ["{{ .Branch }}*"]
hooks:
  - id: glue_exporter
    type: lua
    properties:
      script_path: "{{ .GlueScriptPath }}"
      args:
        aws:
          aws_access_key_id: "{{ .AccessKeyId }}"
          aws_secret_access_key: "{{ .SecretAccessKey }}"
          aws_region: us-east-1
        table_source: '{{ .TableDescriptorPath }}'
        catalog:
          db_name: "{{ .GlueDB }}"
          table_input:
            StorageDescriptor: 
              InputFormat: "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
              OutputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
              SerdeInfo:
                SerializationLibrary: "org.apache.hadoop.hive.serde2.OpenCSVSerde"
                Parameters:
                  separatorChar: ","
            Parameters: 
              classification: "csv"
              "skip.header.line.count": "1"`

const symlinkExporterScript = `
local exporter = require("lakefs/catalogexport/symlink_exporter")
local aws = require("aws")
local table_path = args.table_source
local s3 = aws.s3_client(args.aws.aws_access_key_id, args.aws.aws_secret_access_key, args.aws.aws_region)
exporter.export_s3(s3, table_path, action, {debug=true})
`

const symlinkExporterAction = `
name: Symlink S3 Exporter
on:
  post-commit:
    branches: ["{{ .Branch }}*"]
hooks:
  - id: symlink_exporter
    type: lua
    properties:
      script_path: "{{ .SymlinkScriptPath }}"
      args:
        aws:
          aws_access_key_id: "{{ .AccessKeyId }}"
          aws_secret_access_key: "{{ .SecretAccessKey }}"
          aws_region: us-east-1
        table_source: '{{ .TableDescriptorPath }}'`

func renderTplAsStr(t *testing.T, tplData any, name, content string) string {
	t.Helper()
	tpl, err := template.New(name).Parse(content)
	require.NoError(t, err, "rendering template")
	var doc bytes.Buffer
	err = tpl.Execute(&doc, tplData)
	require.NoError(t, err)
	return doc.String()
}

func setupGlueClient(ctx context.Context, accessKeyID, secretAccessKey string) (*glue.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
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
func genCSVData(cols []string, n int) string {
	csvBlob := ""
	// generate header
	for _, c := range cols {
		csvBlob += c + ","
	}
	csvBlob = strings.TrimSuffix(csvBlob, ",") + "\n"
	// generate rows
	for rowNum := 0; rowNum < n; rowNum++ {
		row := ""
		for i := range cols {
			row += fmt.Sprintf("%d,", rowNum+i)
		}
		csvBlob += strings.TrimSuffix(row, ",") + "\n"
	}
	return csvBlob
}

func testSymlinkS3Exporter(t *testing.T, ctx context.Context, repo string, tablePaths map[string]string, testData *exportHooksTestData) (string, string) {
	t.Helper()

	tableYaml, err := yaml.Marshal(&testData.TableSpec)

	if err != nil {
		require.NoError(t, err, "failed marshaling table spec to YAML")
	}

	hookFiles := map[string]string{
		testData.SymlinkScriptPath:   symlinkExporterScript,
		testData.TableDescriptorPath: renderTplAsStr(t, testData.TableSpec, "table", string(tableYaml)),
	}

	// upload table objects and action script
	uploadAndCommitObjects(t, ctx, repo, mainBranch, hookFiles, tablePaths)

	// upload action
	commit := uploadAndCommitObjects(t, ctx, repo, mainBranch, map[string]string{
		"_lakefs_actions/animals_symlink.yaml": renderTplAsStr(t, testData, "action", symlinkExporterAction),
	})

	// wait until actions finish running
	runs := waitForListRepositoryRunsLen(ctx, t, repo, commit.Id, 1)
	require.Equal(t, "completed", runs.Results[0].Status, "action result not finished")

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
			return fmt.Errorf("no objects found")
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
		if !strings.Contains(entry.Path, "_hidden") {
			lakefsPhysicalAddrs[entry.PhysicalAddress] = true
		}
	}

	require.Equal(t, lakefsPhysicalAddrs, storagePhysicalAddrs, "mismatch between lakefs exported objects in symlink files")

	return commit.Id, symlinksPrefix
}

func TestAWSCatalogExport(t *testing.T) {
	var (
		glueTable     *types.Table
		commitID      string
		symlinkPrefix string
	)
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	testData := &exportHooksTestData{
		Branch:              mainBranch,
		SymlinkScriptPath:   "scripts/symlink_exporter.lua",
		GlueScriptPath:      "scripts/glue_exporter.lua",
		TableDescriptorPath: "_lakefs_tables/animals.yaml",
		GlueDB:              "testexport1",
		AccessKeyId:         viper.GetString("aws_access_key_id"),
		SecretAccessKey:     viper.GetString("aws_secret_access_key"),
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
		tablePaths := map[string]string{
			testData.TableSpec.Path + "/type=axolotl/weight=22/a.csv":   genCSVData(columns, 3),
			testData.TableSpec.Path + "/type=axolotl/weight=22/b.csv":   genCSVData(columns, 3),
			testData.TableSpec.Path + "/type=axolotl/weight=22/c.csv":   genCSVData(columns, 3),
			testData.TableSpec.Path + "/type=axolotl/weight=12/a.csv":   genCSVData(columns, 3),
			testData.TableSpec.Path + "/type=axolotl/weight=12/_hidden": "blob",
			testData.TableSpec.Path + "/type=cat/weight=33/a.csv":       genCSVData(columns, 3),
			testData.TableSpec.Path + "/type=dog/weight=10/b.csv":       genCSVData(columns, 3),
			testData.TableSpec.Path + "/type=dog/weight=10/a.csv":       genCSVData(columns, 3),
		}
		commitID, symlinkPrefix = testSymlinkS3Exporter(t, ctx, repo, tablePaths, testData)
		t.Logf("commit id %s symlinks prefix %s", commitID, symlinkPrefix)
	})
	t.Run("glue_exporter", func(t *testing.T) {
		// override commit ID to make the export table point to the previous commit of data
		testData.OverrideCommitID = commitID
		luaScript := renderTplAsStr(t, testData, "export_script", glueExportScript)
		actionFileBlob := renderTplAsStr(t, testData, "export_action", glueExporterAction)

		headCommit := uploadAndCommitObjects(t, ctx, repo, mainBranch, map[string]string{
			testData.GlueScriptPath:            luaScript,
			"_lakefs_actions/glue_export.yaml": actionFileBlob,
		})

		// wait for action to finish
		runs := waitForListRepositoryRunsLen(ctx, t, repo, headCommit.Id, 1)
		require.Equal(t, "completed", runs.Results[0].Status, "action result not finished")

		// create glue client

		glueClient, err := setupGlueClient(ctx, testData.AccessKeyId, testData.SecretAccessKey)
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
		require.Equal(t, "matilda", *glueTable.Name, "not matila yo")
	})
}
