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
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cenkalti/backoff/v4"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const symlinkExporterScript = `
local exporter = require("lakefs/catalogexport/symlink_exporter")
local aws = require("aws")
local table_path = args.table_source
local s3 = aws.s3_client(args.s3.aws_access_key_id, args.s3.aws_secret_access_key, args.s3.aws_region)
exporter.export_s3(s3, table_path, action, {debug=true})
`
const animalsTableSpec = `
name: '{{ .Name }}'
type: hive
path: '{{ .TablePrefix }}'
partition_columns: ['type', 'weight']
schema:
  type: struct
  fields:
    - name: weight
      type: integer
      nullable: false
    - name: name
      type: string
      nullable: false
      metadata: {}
    - name: type
      type: string
      nullable: true
      metadata:
        comment: axolotl, cat, dog, fish etc`

const symlinkExporterAction = `
name: Symlink S3 Exporter
on:
  post-commit:
    branches: ["main*"]
hooks:
  - id: symlink_exporter
    type: lua
    properties:
      script_path: scripts/symlink_exporter.lua
      args:
        s3:
          aws_access_key_id: "{{ .AccessKeyId }}"
          aws_secret_access_key: "{{ .SecretAccessKey }}"
          aws_region: us-east-1
        table_source: '_lakefs_tables/{{ .Name }}.yaml'`

func renderTpl(t *testing.T, tplData any, name, content string) string {
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

func uploadAndCommitObjects(t *testing.T, ctx context.Context, repo, branch string, objects map[string]string) {
	for path, obj := range objects {
		resp, err := uploadContent(ctx, repo, branch, path, obj)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode())
	}
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "Table Data",
	})
}

func TestSymlinkS3Exporter(t *testing.T) {
	// setup
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	tablePrefix := "tables/animals"
	tablePaths := map[string]string{
		// tables/animals
		tablePrefix + "/type=axolotl/weight=22/a.csv":   "blob",
		tablePrefix + "/type=axolotl/weight=22/b.csv":   "blob",
		tablePrefix + "/type=axolotl/weight=22/c.csv":   "blob",
		tablePrefix + "/type=axolotl/weight=12/a.csv":   "blob",
		tablePrefix + "/type=axolotl/weight=12/_hidden": "blob",
		tablePrefix + "/type=cat/weight=33/a.csv":       "blob",
		tablePrefix + "/type=dog/weight=10/a.csv":       "blob",
		tablePrefix + "/type=dog/weight=10/b.csv":       "blob",
	}
	// render actions based on templates
	actionData := struct {
		Name                         string
		TablePrefix                  string
		AccessKeyId, SecretAccessKey string
	}{
		Name:            "animals",
		TablePrefix:     tablePrefix,
		AccessKeyId:     viper.GetString("aws_access_key_id"),
		SecretAccessKey: viper.GetString("aws_secret_access_key"),
	}
	hookFiles := map[string]string{
		"scripts/symlink_exporter.lua": symlinkExporterScript,
		"_lakefs_tables/animals.yaml":  renderTpl(t, actionData, "table", animalsTableSpec),
	}

	uploadObjects(t, ctx, repo, mainBranch, hookFiles)
	// table data
	uploadObjects(t, ctx, repo, mainBranch, tablePaths)

	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "Table Data",
	})
	require.NoError(t, err, "failed to commit table data content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())
	// upload action

	resp, err = uploadContent(ctx, repo, mainBranch, "_lakefs_actions/animals_symlink.yaml", renderTpl(t, actionData, "action", symlinkExporterAction))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())
	commitResp, err = client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "Action commit",
	})
	require.NoError(t, err, "failed to commit action content")
	require.Equal(t, http.StatusCreated, commitResp.StatusCode())

	// wait until actions finish running
	runs := waitForListRepositoryRunsLen(ctx, t, repo, commitResp.JSON201.Id, 1)
	require.Equal(t, "completed", runs.Results[0].Status, "action result not finished")

	// list symlink.txt files from blockstore

	repoResponse, err := client.GetRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "could not get repository information")
	require.Equal(t, repoResponse.StatusCode(), http.StatusOK, "could not get repository information")
	namespace := repoResponse.JSON200.StorageNamespace

	//testesti/rand_qtvU6OdSpF/ckei2hn6i1efi86dngh0/testsymlinkexporter/_lakefs/exported/main/9a196e/animals/type=axolotl/weight=22/symlink.tx
	symlinksPrefix := fmt.Sprintf("%s/_lakefs/exported/%s/%s/animals/", namespace, mainBranch, commitResp.JSON201.Id[:6])
	storageURL, err := url.Parse(symlinksPrefix)
	require.NoError(t, err, "failed extracting bucket name")

	s3Client, err := testutil.SetupTestS3Client("https://s3.amazonaws.com", actionData.AccessKeyId, actionData.SecretAccessKey)

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

	lakeFSObjs, err := client.ListObjectsWithResponse(ctx, repo, commitResp.JSON201.Id, &apigen.ListObjectsParams{
		Prefix: apiutil.Ptr(apigen.PaginationPrefix(tablePrefix)),
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

	// glue test
	glueClient, err := setupGlueClient(ctx, actionData.AccessKeyId, actionData.SecretAccessKey)
	require.NoError(t, err, "creating glue client")
	glueClient.CreateTable()
}
