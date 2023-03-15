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
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/rs/xid"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
	s3Adapter "github.com/treeverse/lakefs/pkg/block/s3"
	"github.com/treeverse/lakefs/pkg/testutil"
	"golang.org/x/exp/slices"
)

const (
	uncommittedGCRepoName = "ugc"
	ugcFindingsFilename   = "ugc-findings.json"
)

type UncommittedFindings struct {
	All            []Object
	Deleted        []string
	preLinkAddress api.StagingLocation
}

func TestUncommittedGC(t *testing.T) {
	SkipTestIfAskedTo(t)
	requireBlockstoreType(t, block.BlockstoreTypeS3)
	ctx := context.Background()
	prepareForUncommittedGC(t, ctx)

	// upload files while ugc is running
	ticker := time.NewTicker(time.Second)
	var durObjects []string
	done := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				ticker.Stop()
				return
			case <-ticker.C:
				obj := uploadAndDeleteSafeTestData(t, ctx, uncommittedGCRepoName)
				durObjects = append(durObjects, obj)
			}
		}
	}()

	submitConfig := &sparkSubmitConfig{
		sparkVersion:    sparkImageTag,
		localJar:        metaclientJarPath,
		entryPoint:      "io.treeverse.gc.UncommittedGarbageCollector",
		extraSubmitArgs: []string{"--conf", "spark.hadoop.lakefs.debug.gc.uncommitted_min_age_seconds=1"},
		programArgs:     []string{uncommittedGCRepoName, "us-east-1"},
		logSource:       "ugc",
	}
	testutil.MustDo(t, "run uncommitted GC", runSparkSubmit(submitConfig))
	done <- true
	wg.Wait()

	validateUncommittedGC(t, durObjects)
}

func prepareForUncommittedGC(t *testing.T, ctx context.Context) {
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

	// getting physical address before the ugc run and linking it after
	getPhysicalResp, err := client.GetPhysicalAddressWithResponse(ctx, repo, mainBranch, &api.GetPhysicalAddressParams{Path: "foo/bar"})
	if err != nil {
		t.Fatalf("Failed to get physical address %s", err)
	}
	if getPhysicalResp.JSON200 == nil {
		t.Fatalf("Failed to get physical address information: status code %d", getPhysicalResp.StatusCode())
	}

	objects, _ := listRepositoryUnderlyingStorage(t, ctx, repo)
	// write findings into a file
	findings, err := json.MarshalIndent(
		UncommittedFindings{
			All:     objects,
			Deleted: gone,
			preLinkAddress: api.StagingLocation{
				PhysicalAddress: getPhysicalResp.JSON200.PhysicalAddress,
				Token:           getPhysicalResp.JSON200.Token,
			},
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

func validateUncommittedGC(t *testing.T, durObjects []string) {
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

	// link the pre physical address
	const expectedSizeBytes = 38
	_, err = client.LinkPhysicalAddressWithResponse(ctx, "repo1", "main", &api.LinkPhysicalAddressParams{
		Path: "foo/bar",
	}, api.LinkPhysicalAddressJSONRequestBody{
		Checksum:  "afb0689fe58b82c5f762991453edbbec",
		SizeBytes: expectedSizeBytes,
		Staging: api.StagingLocation{
			PhysicalAddress: findings.preLinkAddress.PhysicalAddress,
			Token:           findings.preLinkAddress.Token,
		},
	})
	if err != nil {
		t.Fatalf("Failed to link physical address %s", err)
	}

	// list underlying storage
	objects, qk := listRepositoryUnderlyingStorage(t, ctx, repo)

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
		if !slices.Contains(findings.All, obj) && !slices.Contains(durObjects, obj.Key) {
			t.Errorf("Object '%s' FOUND KNOWN - was not found pre ugc", obj)
		}
	}

	bucket, key := s3Adapter.ExtractParamsFromQK(qk)
	s3Client := newDefaultS3Client()
	runID := getLastUGCRunID(t, ctx, s3Client, bucket, key)
	reportPath := fmt.Sprintf("%s_lakefs/retention/gc/uncommitted/%s/summary.json", key, runID)
	cutoffTime, err := getReportCutoffTime(s3Client, bucket, reportPath)
	if err != nil {
		t.Fatalf("Failed to get start list time from ugc report %s", err)
	}

	objectsMap := make(map[string]Object)
	for _, obj := range objects {
		objectsMap[obj.Key] = obj
	}

	// Validation that deleted objects that were created after the cutoff are not deleted
	// and that deleted objects that were created before the cutoff are deleted
	for _, obj := range durObjects {
		x, foundIt := objectsMap[obj]
		expectDeleted := true
		if foundIt {
			expectDeleted = x.LastModified.Before(cutoffTime)
		}
		if foundIt && expectDeleted {
			t.Errorf("Object '%s' FOUND - should have been deleted", obj)
		} else if !foundIt && !expectDeleted {
			t.Errorf("Object '%s' NOT FOUND - should NOT been deleted", obj)
		}
	}
}

// listRepositoryUnderlyingStorage list on the repository storage namespace returns its objects, and the storage namespace
func listRepositoryUnderlyingStorage(t *testing.T, ctx context.Context, repo string) ([]Object, block.CommonQualifiedKey) {
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
	qk, err := block.DefaultResolveNamespace(storageNamespace, "", block.IdentifierTypeRelative)
	if err != nil {
		t.Fatalf("Failed to resolve namespace '%s': %s", storageNamespace, err)
	}

	bucket, key := s3Adapter.ExtractParamsFromQK(qk)
	s3Client := newDefaultS3Client()
	objects := listUnderlyingStorage(t, ctx, s3Client, bucket, key)
	return objects, qk
}

func listUnderlyingStorage(t *testing.T, ctx context.Context, s3Client *s3.S3, bucket, prefix string) []Object {
	listInput := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	listOutput, err := s3Client.ListObjectsWithContext(ctx, listInput)
	if err != nil {
		t.Fatalf("Failed to list objects (bucket: %s, prefix: %s): %s", bucket, prefix, err)
	}

	// sorted list of objects found on repository - before ugc
	var objects []Object
	for _, obj := range listOutput.Contents {
		objects = append(objects, Object{
			Key:          fmt.Sprintf("s3://%s/%s", bucket, aws.StringValue(obj.Key)),
			LastModified: aws.TimeValue(obj.LastModified),
		})
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
	listSize := int64(1)
	listInput := &s3.ListObjectsInput{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(runIDPrefix),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(listSize),
	}
	listOutput, err := s3Client.ListObjectsWithContext(ctx, listInput)
	if err != nil {
		t.Fatalf("Failed to list objects (bucket: %s, prefix: %s): %s", bucket, runIDPrefix, err)
	}

	if len(listOutput.CommonPrefixes) == 0 {
		t.Fatalf("Failed to list last run id (bucket: %s, prefix: %s): %s", bucket, runIDPrefix, err)
	}
	key := strings.ReplaceAll(*listOutput.CommonPrefixes[0].Prefix, runIDPrefix, "")
	runID := strings.ReplaceAll(key, "/", "")
	return runID
}

func uploadAndDeleteSafeTestData(t *testing.T, ctx context.Context, repository string) string {
	name := xid.New().String()
	_, _ = uploadFileRandomData(ctx, t, repository, mainBranch, name, false)

	addr := objectPhysicalAddress(t, ctx, repository, name)

	_, err := client.DeleteObjectWithResponse(ctx, repository, mainBranch, &api.DeleteObjectParams{Path: name})
	if err != nil {
		t.Fatalf("Failed to delete object %s", err)
	}
	return addr
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
		StartTime         time.Time `json:"start_time"`
		CutoffTime        time.Time `json:"cutoff_time"`
		NumDeletedObjects int       `json:"num_deleted_objects"`
	}

	var report Report
	err = json.Unmarshal([]byte(myFileContentAsString), &report)
	if err != nil {
		return time.Time{}, err
	}
	return report.CutoffTime, nil
}
