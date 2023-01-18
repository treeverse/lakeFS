package esti

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/rs/xid"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/testutil"
	"golang.org/x/exp/slices"
)

const (
	uncommittedGCRepoName     = "ugc"
	uncommittedSafeGCRepoName = "ugc-safe"
	ugcFindingsFilename       = "ugc-findings.json"
)

type UncommittedFindings struct {
	All     []Object
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

	objects, _ := listRepositoryUnderlyingStorage(t, ctx, repo)
	// write findings into a file
	findings, err := json.MarshalIndent(
		UncommittedFindings{
			All:     objects,
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
	b, err := os.ReadFile(findingPath)
	if err != nil {
		t.Fatalf("Failed to read '%s': %s", findingPath, err)
	}
	var findings UncommittedFindings
	err = json.Unmarshal(b, &findings)
	if err != nil {
		t.Fatalf("Failed to unmarshal findings '%s': %s", findingPath, err)
	}

	// list underlying storage
	objects, _ := listRepositoryUnderlyingStorage(t, ctx, repo)

	// verify that all previous objects are found or deleted, if needed
	for _, obj := range findings.All {
		foundIt := slices.Contains(objects, obj)
		expectDeleted := slices.Contains(findings.Deleted, obj.Key)
		if foundIt && expectDeleted {
			t.Errorf("Object '%s' FOUND - should have been deleted", obj)
		} else if !foundIt && !expectDeleted {
			t.Errorf("Object '%s' NOT FOUND - should NOT been deleted", obj)
		}
	}

	// verify that we do not have new objects
	for _, obj := range objects {
		// skip uncommitted retention reports
		if strings.Contains(obj.Key, "/_lakefs/retention/gc/uncommitted/") {
			continue
		}
		// verify we know this object
		if !slices.Contains(findings.All, obj) {
			t.Errorf("Object '%s' FOUND KNOWN - was not found pre ugc", obj)
		}
	}
}

func listRepositoryUnderlyingStorage(t *testing.T, ctx context.Context, repo string) ([]Object, block.QualifiedPrefix) {
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
	return objects, qp
}

func listUnderlyingStorage(t *testing.T, ctx context.Context, s3Client *s3.S3, bucket, prefix string) []Object {
	listInput := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	listOutput, err := s3Client.ListObjectsWithContext(ctx, listInput)
	if err != nil {
		t.Fatalf("Failed ot list objects (bucket: %s, prefix: %s): %s", bucket, prefix, err)
	}

	// sorted list of objects found on repository - before ugc
	var objects []Object
	for _, obj := range listOutput.Contents {
		objects = append(objects, Object{
			Key:          fmt.Sprintf("s3://%s/%s", bucket, aws.StringValue(obj.Key)),
			LastModified: *obj.LastModified})
	}
	return objects
}

type Object struct {
	Key          string
	LastModified time.Time
}

func newDefaultS3Client() *s3.S3 {
	// setup new s3 client for direct access to the underlying storage
	mySession := session.Must(session.NewSession())
	s3Client := s3.New(mySession, aws.NewConfig().WithRegion("us-east-1"))
	return s3Client
}

func getLastUGCRunID(t *testing.T, ctx context.Context, s3Client *s3.S3, bucket, prefix string) string {
	runIDPrefix := prefix + "_lakefs/retention/gc/uncommitted/"

	listInput := &s3.ListObjectsInput{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(runIDPrefix),
		Delimiter: aws.String("/"),
	}
	listOutput, err := s3Client.ListObjectsWithContext(ctx, listInput)
	if err != nil {
		t.Fatalf("Failed ot list objects (bucket: %s, prefix: %s): %s", bucket, runIDPrefix, err)
	}

	if len(listOutput.CommonPrefixes) == 0 {
		t.Fatalf("Failed to list last run id (bucket: %s, prefix: %s): %s", bucket, runIDPrefix, err)
	}
	key := strings.ReplaceAll(*listOutput.CommonPrefixes[0].Prefix, runIDPrefix, "")
	runID := strings.ReplaceAll(key, "/", "")
	return runID
}

func TestSafeUncommittedGC(t *testing.T) {
	SkipTestIfAskedTo(t)
	blockstoreType := viper.GetString(config.BlockstoreTypeKey)
	if blockstoreType != block.BlockstoreTypeS3 {
		t.Skip("Running on S3 only")
	}
	ctx := context.Background()
	repo := createRepositoryByName(ctx, t, uncommittedSafeGCRepoName)

	// pre run
	getPhysicalResp, err := client.GetPhysicalAddressWithResponse(ctx, repo, mainBranch, &api.GetPhysicalAddressParams{Path: "foo/bar"})
	if err != nil {
		t.Fatalf("Failed to get physical address %s", err)
	}
	if getPhysicalResp.JSON200 == nil {
		t.Fatalf("Failed to get physical address information: status code %d", getPhysicalResp.StatusCode())
	}

	// upload files while ugc is running
	ticker := time.NewTicker(time.Second * 1)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				ticker.Stop()
				return
			case <-ticker.C:
				uploadAndDeleteSafeTestData(t, ctx, repo)
			}
		}
	}()

	// run ugc
	submitConfig := &sparkSubmitConfig{
		sparkVersion:    sparkImageTag,
		localJar:        metaclientJarPath,
		entryPoint:      "io.treeverse.gc.UncommittedGarbageCollector",
		extraSubmitArgs: []string{"--conf", "spark.hadoop.lakefs.debug.gc.uncommitted_min_age_seconds=1"},
		programArgs:     []string{uncommittedSafeGCRepoName, "us-east-1"},
		logSource:       "ugc",
	}
	testutil.MustDo(t, "run uncommitted GC", runSparkSubmit(submitConfig))
	done <- true

	// post run
	const expectedSizeBytes = 38
	_, err = client.LinkPhysicalAddressWithResponse(ctx, "repo1", "main", &api.LinkPhysicalAddressParams{
		Path: "foo/bar",
	}, api.LinkPhysicalAddressJSONRequestBody{
		Checksum:  "afb0689fe58b82c5f762991453edbbec",
		SizeBytes: expectedSizeBytes,
		Staging: api.StagingLocation{
			PhysicalAddress: getPhysicalResp.JSON200.PhysicalAddress,
			Token:           getPhysicalResp.JSON200.Token,
		},
	})
	if err != nil {
		t.Fatalf("Failed to link physical address %s", err)
	}

	s3Client := newDefaultS3Client()
	validateSafeUncommittedGC(t, ctx, s3Client, repo)
}

func uploadAndDeleteSafeTestData(t *testing.T, ctx context.Context, repository string) {
	name := xid.New().String()
	_, _ = uploadFileRandomData(ctx, t, repository, mainBranch, name, false)

	_, err := client.DeleteObjectWithResponse(ctx, repository, mainBranch, &api.DeleteObjectParams{Path: name})
	if err != nil {
		t.Fatalf("Failed to delete object %s", err)
	}
}

func validateSafeUncommittedGC(t *testing.T, ctx context.Context, s3Client *s3.S3, repository string) {
	objects, qp := listRepositoryUnderlyingStorage(t, ctx, repository)
	runID := getLastUGCRunID(t, ctx, s3Client, qp.StorageNamespace, qp.Prefix)

	reportPath := fmt.Sprintf("%s_lakefs/retention/gc/uncommitted/%s/summary.json", qp.Prefix, runID)
	startListTime, err := getReportCutoffTime(s3Client, qp.StorageNamespace, reportPath)
	if err != nil {
		t.Fatalf("Failed to get start list time from ugc report %s", err)
	}

	// verify that all objects are deleted, if needed
	for _, obj := range objects {
		if strings.Contains(obj.Key, "/_lakefs/retention/gc/uncommitted/") || strings.Contains(obj.Key, "dummy") {
			continue
		}

		if obj.LastModified.Before(startListTime) {
			t.Errorf("object time '%s' start list time %s", obj.LastModified.String(), startListTime.String())
			t.Errorf("Object '%s' FOUND - should have been deleted", obj)
		}
	}

}

func getReportCutoffTime(s3Client *s3.S3, bucket, reportPath string) (time.Time, error) {
	res, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(reportPath),
	})
	if err != nil {
		return time.Time{}, err
	}
	defer res.Body.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(res.Body)
	if err != nil {
		return time.Time{}, err
	}
	myFileContentAsString := buf.String()

	type Report struct {
		RunId             string    `json:"run_id"`
		Success           bool      `json:"success"`
		FirstSlice        string    `json:"first_slice"`
		StartTime         time.Time `json:"start_time,time"`
		StartListTime     time.Time `json:"start_list_time,time"`
		NumDeletedObjects int       `json:"num_deleted_objects"`
	}

	var report Report
	err = json.Unmarshal([]byte(myFileContentAsString), &report)
	if err != nil {
		return time.Time{}, err
	}
	return report.StartListTime, nil
}
