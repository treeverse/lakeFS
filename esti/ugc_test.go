package esti

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
<<<<<<< HEAD
	"github.com/treeverse/lakefs/pkg/testutil"
=======
	"github.com/treeverse/lakefs/pkg/config"
>>>>>>> 625af94e (use blockstore type key as constant)
	"golang.org/x/exp/slices"
)

const (
	uncommittedGCRepoName = "ugc"
	ugcFindingsFilename   = "ugc-findings.json"
)

type UncommittedFindings struct {
	All     []string
	Deleted []string
}

func TestUncommittedGC(t *testing.T) {
	SkipTestIfAskedTo(t)
	blockstoreType := viper.GetString(config.BlockstoreTypeKey)
	if blockstoreType != block.BlockstoreTypeS3 {
		t.Skip("Running on S3 only")
	}
	prepareForUncommittedGC(t)
	submitConfig := &sparkSubmitConfig{
		sparkVersion:    sparkImageTag,
		localJar:        metaclientJarPath,
		entryPoint:      "io.treeverse.gc.UncommittedGarbageCollector",
		extraSubmitArgs: []string{"--conf", "spark.hadoop.lakefs.debug.gc.uncommitted_min_age_seconds=1"},
		programArgs:     []string{uncommittedGCRepoName, "us-east-1"},
		logSource:       "ugc",
	}
	testutil.MustDo(t, "run uncommitted GC", runSparkSubmit(submitConfig))
	validateUncommittedGC(t)

}

func prepareForUncommittedGC(t *testing.T) {
	ctx := context.Background()
	repo := createRepositoryByName(ctx, t, uncommittedGCRepoName)
	var gone []string

	// upload some data and commit
	for i := 0; i < 3; i++ {
		_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, "committed/data-"+strconv.Itoa(i), false)
		_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, "committed/data-direct-"+strconv.Itoa(i), true)
	}
	_, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "Commit initial data"})
	if err != nil {
		t.Fatal("Commit some data", err)
	}

	// upload same file twice and commit, keep delete physical location
	for _, direct := range []bool{false, true} {
		objPath := fmt.Sprintf("committed/double-or-nothing-%t", direct)
		_, err = uploadFileAndReport(ctx, repo, mainBranch, objPath, objPath+"1", direct)
		if err != nil {
			t.Fatalf("Failed to upload double-or-nothing I: %s", err)
		}
		addr := objectPhysicalAddress(t, ctx, repo, objPath)
		gone = append(gone, addr)

		_, err = uploadFileAndReport(ctx, repo, mainBranch, objPath, objPath+"2", direct)
		if err != nil {
			t.Fatalf("Failed to upload double-or-nothing II: %s", err)
		}
		_, err = client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "Commit initial data"})
		if err != nil {
			t.Fatal("Commit single file uploaded twice", err)
		}
	}

	// upload uncommitted data

	for _, direct := range []bool{false, true} {
		// just leave uncommitted data
		_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, fmt.Sprintf("uncommitted/data1-%t", direct), direct)

		// delete uncommitted
		objPath := fmt.Sprintf("uncommitted/data2-%t", direct)
		_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, objPath, direct)

		gone = append(gone, objectPhysicalAddress(t, ctx, repo, objPath))

		delResp, err := client.DeleteObjectWithResponse(ctx, repo, mainBranch, &api.DeleteObjectParams{Path: objPath})
		if err != nil {
			t.Fatalf("Delete object '%s' failed: %s", objPath, err)
		}
		if delResp.StatusCode() != http.StatusNoContent {
			t.Fatalf("Delete object '%s' failed with status code %d", objPath, delResp.StatusCode())
		}
	}

	// write findings into a file
	findings, err := json.MarshalIndent(
		UncommittedFindings{
			All:     listRepositoryUnderlyingStorage(t, ctx, repo),
			Deleted: gone,
		}, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal findings: %s", err)
	}
	t.Logf("Findings: %s", findings)
	findingsPath := findingFilePath()
	err = os.WriteFile(findingsPath, findings, 0o666)
	if err != nil {
		t.Fatalf("Failed to write findings file '%s': %s", findingsPath, err)
	}
}

func findingFilePath() string {
	return filepath.Join(os.TempDir(), ugcFindingsFilename)
}

func objectPhysicalAddress(t *testing.T, ctx context.Context, repo, objPath string) string {
	resp, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{Path: objPath})
	if err != nil {
		t.Fatalf("Failed to stat object '%s': %s", objPath, err)
	}
	if resp.JSON200 == nil {
		t.Fatalf("Failed to stat object '%s': status code %d", objPath, resp.StatusCode())
	}
	return resp.JSON200.PhysicalAddress
}

func validateUncommittedGC(t *testing.T) {
	ctx := context.Background()
	const repo = uncommittedGCRepoName

	findingPath := findingFilePath()
	bytes, err := os.ReadFile(findingPath)
	if err != nil {
		t.Fatalf("Failed to read '%s': %s", findingPath, err)
	}
	var findings UncommittedFindings
	err = json.Unmarshal(bytes, &findings)
	if err != nil {
		t.Fatalf("Failed to unmarshal findings '%s': %s", findingPath, err)
	}

	// list underlying storage
	objects := listRepositoryUnderlyingStorage(t, ctx, repo)

	// verify that all previous objects are found or deleted, if needed
	for _, obj := range findings.All {
		foundIt := slices.Contains(objects, obj)
		expectDeleted := slices.Contains(findings.Deleted, obj)
		if foundIt && expectDeleted {
			t.Errorf("Object '%s' FOUND - should have been deleted", obj)
		} else if !foundIt && !expectDeleted {
			t.Errorf("Object '%s' NOT FOUND - should NOT been deleted", obj)
		}
	}

	// verify that we do not have new objects
	for _, obj := range objects {
		// skip uncommitted retention reports
		if strings.Contains(obj, "/_lakefs/retention/gc/uncommitted/") {
			continue
		}
		// verify we know this object
		if !slices.Contains(findings.All, obj) {
			t.Errorf("Object '%s' FOUND KNOWN - was not found pre ugc", obj)
		}
	}
}

func listRepositoryUnderlyingStorage(t *testing.T, ctx context.Context, repo string) []string {
	// list all objects in the physical layer
	repoResponse, err := client.GetRepositoryWithResponse(ctx, repo)
	if err != nil {
		t.Fatalf("Failed to get repository '%s' information: %s", repo, err)
	}
	if repoResponse.JSON200 == nil {
		t.Fatalf("Failed to get repository '%s' information: status code %d", repo, repoResponse.StatusCode())
	}

	// list committed files and keep the physical paths
	storageNamespace := repoResponse.JSON200.StorageNamespace
	qp, err := block.ResolveNamespacePrefix(storageNamespace, "")
	if err != nil {
		t.Fatalf("Failed to resolve namespace '%s': %s", storageNamespace, err)
	}

	s3Client := newDefaultS3Client()
	objects := listUnderlyingStorage(t, ctx, s3Client, qp.StorageNamespace, qp.Prefix)
	return objects
}

func listUnderlyingStorage(t *testing.T, ctx context.Context, s3Client *s3.S3, bucket, prefix string) []string {
	listInput := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	listOutput, err := s3Client.ListObjectsWithContext(ctx, listInput)
	if err != nil {
		t.Fatalf("Failed ot list objects (bucket: %s, prefix: %s): %s", bucket, prefix, err)
	}

	// sorted list of objects found on repository - before ugc
	var objects []string
	for _, obj := range listOutput.Contents {
		objects = append(objects, fmt.Sprintf("s3://%s/%s", bucket, aws.StringValue(obj.Key)))
	}
	return objects
}

func newDefaultS3Client() *s3.S3 {
	// setup new s3 client for direct access to the underlying storage
	mySession := session.Must(session.NewSession())
	s3Client := s3.New(mySession, aws.NewConfig().WithRegion("us-east-1"))
	return s3Client
}
