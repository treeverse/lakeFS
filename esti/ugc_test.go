package esti

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/go-test/deep"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
)

const uncommittedGCRepoName = "ugc"

func TestUncommittedGC(t *testing.T) {
	SkipTestIfAskedTo(t)
	blockstoreType := viper.GetViper().GetString("blockstore_type")
	if blockstoreType != block.BlockstoreTypeS3 {
		t.Skip("Running on S3 only")
	}

	step := viper.GetViper().GetString("ugc_step")
	switch step {
	case "pre":
		t.Run("pre", testPreUncommittedGC)
	case "post":
		t.Run("post", testPostUncommittedGC)
	default:
		t.Skip("No known value (pre/port) for ugc_step")
	}
}

func testPreUncommittedGC(t *testing.T) {
	ctx := context.Background()
	repo := createRepositoryByName(ctx, t, uncommittedGCRepoName)

	// defer tearDownTest(repo)
	t.Log("Test uncommitted GC on repo", repo)

	const sampleObjects = 3

	// upload and commit some data
	for i := 0; i < sampleObjects; i++ {
		_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, "committed/data-"+strconv.Itoa(i), false)
	}
	_, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "Commit initial data"})
	if err != nil {
		t.Fatal("Commit some data", err)
	}

	// upload same file twice and commit (should leave one unused)
	for i := 0; i < 2; i++ {
		_, err = uploadFileAndReport(ctx, repo, mainBranch, "double-or-nothing", "double-or-nothing", false)
		if err != nil {
			t.Fatalf("Failed to upload double-or-nothing (round %d): %s", i+1, err)
		}
	}
	_, err = client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "Commit initial data"})
	if err != nil {
		t.Fatal("Commit single file uploaded twice", err)
	}

	// upload uncommitted data
	for i := 0; i < sampleObjects; i++ {
		_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, "uncommitted/data-"+strconv.Itoa(i), false)
	}

	// write current underlying storage to a file
	objects := listRepositoryUnderlyingStorage(t, ctx, repo)
	preFilename := filepath.Join(os.TempDir(), "per_ugc.txt")
	err = os.WriteFile(preFilename, []byte(strings.Join(objects, "\n")), 0666)
	if err != nil {
		t.Fatalf("Failed to write objects list to %s: %s", preFilename, err)
	}
}

func testPostUncommittedGC(t *testing.T) {
	ctx := context.Background()
	const repo = uncommittedGCRepoName

	preFilename := filepath.Join(os.TempDir(), "per_ugc.txt")
	bytes, err := os.ReadFile(preFilename)
	if err != nil {
		t.Fatalf("Failed to read '%s': %s", preFilename, err)
	}
	preObjects := strings.Split(string(bytes), "\n")

	objects := listRepositoryUnderlyingStorage(t, ctx, repo)
	if diff := deep.Equal(preObjects, objects); diff != nil {
		t.Errorf("Diff found: %s", diff)
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
	return listUnderlyingStorage(t, ctx, s3Client, qp.StorageNamespace, qp.Prefix)
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
		objects = append(objects, aws.StringValue(obj.Key))
	}
	sort.Strings(objects)
	return objects
}

func newDefaultS3Client() *s3.S3 {
	// setup new s3 client for direct access to the underlying storage
	mySession := session.Must(session.NewSession())
	s3Client := s3.New(mySession, aws.NewConfig().WithRegion("us-east-1"))
	return s3Client
}
