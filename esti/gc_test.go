package esti

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
)

const (
	fullGCMode          = 0
	markOnlyMode        = 1
	sweepOnlyMode       = 2
	committedGCRepoName = "gc"
	gcFindingsFilename  = "gc-findings.json"
)

var (
	currentEpochInSeconds = time.Now().Unix()
	dayInSeconds          = int64(100000) // rounded up from 86400
)

type testCase struct {
	id           string
	policy       api.GarbageCollectionRules
	branches     []branchProperty
	fileDeleted  bool
	description  string
	directUpload bool
	testMode     string
	markId       string
}

type branchProperty struct {
	name                string
	deleteCommitDaysAgo int64
}

var testCases = []testCase{
	{id: "1",
		policy:       api.GarbageCollectionRules{Branches: []api.GarbageCollectionRule{}, DefaultRetentionDays: 1},
		branches:     []branchProperty{{name: "a1", deleteCommitDaysAgo: 2}, {name: "b1", deleteCommitDaysAgo: 2}},
		fileDeleted:  true,
		description:  "The file is deleted according to the default retention policy",
		directUpload: false,
	},
}

func TestCommittedGC(t *testing.T) {
	SkipTestIfAskedTo(t)
	blockstoreType := viper.GetViper().GetString("blockstore_type")
	//TODO lynn: change this for test also on Azure
	if blockstoreType != block.BlockstoreTypeS3 {
		t.Skip("Running on S3 only")
	}

	step := viper.GetViper().GetString("gc_step")
	switch step {
	case "pre":
		t.Run("pre", testPreCommittedGC)
	case "post":
		t.Run("post", testPostCommittedGC)
	default:
		t.Skip("No known value (pre/port) for gc_step")
	}
}

func testPreCommittedGC(t *testing.T) {
	ctx := context.Background()
	blockstoreType := viper.GetViper().GetString("blockstore_type")
	fileRefPerTestCase := make(map[string]string)

	for _, testCase := range testCases {
		t.Logf("Test case %s", testCase.id)
		fileExistingRef := prepareForGC(t, ctx, testCase, blockstoreType)
		t.Logf("fileExistingRef %s", fileExistingRef)
		fileRefPerTestCase[testCase.id] = fileExistingRef
	}

	// write findings into a file
	findings, err := json.MarshalIndent(fileRefPerTestCase, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal findings: %s", err)
	}
	t.Logf("File Ref Per Test Case: %s", findings)
	findingsPath := filepath.Join(os.TempDir(), gcFindingsFilename)
	err = os.WriteFile(findingsPath, findings, 0o666)
	if err != nil {
		t.Fatalf("Failed to write findings file '%s': %s", findingsPath, err)
	}
}

func prepareForGC(t *testing.T, ctx context.Context, testCase testCase, blockstoreType string) string {
	repo := createRepositoryByName(ctx, t, committedGCRepoName+testCase.id)

	// upload 3 files not to be deleted and commit
	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, "not_deleted_file1", false)
	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, "not_deleted_file2", false)
	direct := blockstoreType == block.BlockstoreTypeS3
	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, "not_deleted_file3", direct)

	commitTime := int64(0)
	_, err := client.CommitWithResponse(ctx, repo, mainBranch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "add three files not to be deleted", Date: &commitTime})
	if err != nil {
		t.Fatal("Commit some data", err)
	}

	newBranch := "a" + testCase.id
	_, err = client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{Name: newBranch, Source: mainBranch})
	if err != nil {
		t.Fatal("Create new branch", err)
	}

	direct = testCase.directUpload && blockstoreType == block.BlockstoreTypeS3
	_, _ = uploadFileRandomData(ctx, t, repo, newBranch, "file"+testCase.id, direct)
	commitTime = int64(10)

	//get commit id after commit for validation step in the tests
	commitRes, err := client.CommitWithResponse(ctx, repo, newBranch, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "Uploaded file" + testCase.id, Date: &commitTime})
	if err != nil || commitRes.StatusCode() != 201 {
		t.Fatal("Commit some data", err)
	}
	commit := commitRes.JSON201
	commitId := commit.Id

	_, err = client.CreateBranchWithResponse(ctx, repo, api.CreateBranchJSONRequestBody{Name: "b" + testCase.id, Source: newBranch})
	if err != nil {
		t.Fatal("Create new branch", err)
	}

	_, err = client.SetGarbageCollectionRulesWithResponse(ctx, repo, api.SetGarbageCollectionRulesJSONRequestBody{Branches: testCase.policy.Branches, DefaultRetentionDays: testCase.policy.DefaultRetentionDays})
	if err != nil {
		t.Fatal("Set GC rules", err)
	}

	for _, branch := range testCase.branches {
		if branch.deleteCommitDaysAgo > -1 {
			_, err = client.DeleteObjectWithResponse(ctx, repo, branch.name, &api.DeleteObjectParams{Path: "file" + testCase.id})
			epochCommitDateInSeconds := currentEpochInSeconds - (dayInSeconds * branch.deleteCommitDaysAgo)
			_, err = client.CommitWithResponse(ctx, repo, branch.name, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "Deleted file" + testCase.id, Date: &epochCommitDateInSeconds})
			if err != nil {
				t.Fatal("Commit some data", err)
			}
			_, _ = uploadFileRandomData(ctx, t, repo, branch.name, "file"+testCase.id+"not_deleted", false)
			// This is for the previous commit to be the HEAD of the branch outside the retention time (according to GC https://github.com/treeverse/lakeFS/issues/1932)
			_, err = client.CommitWithResponse(ctx, repo, branch.name, &api.CommitParams{}, api.CommitJSONRequestBody{Message: "not deleted file commit: " + testCase.id, Date: &epochCommitDateInSeconds})
			if err != nil {
				t.Fatal("Commit some data", err)
			}
		} else {
			_, err = client.DeleteBranchWithResponse(ctx, repo, branch.name)
			if err != nil {
				t.Fatal("Delete brach", err)
			}
		}
	}
	return commitId
}

func getGCMode(testCase testCase) int {
	if testCase.testMode == "mark" {
		return markOnlyMode
	} else if testCase.testMode == "sweep" {
		return sweepOnlyMode
	} else {
		return fullGCMode
	}
}

func validateGCJob(t *testing.T, ctx context.Context, testCase testCase, existingRef string) {
	repo := committedGCRepoName + testCase.id

	res, _ := client.GetObjectWithResponse(ctx, repo, existingRef, &api.GetObjectParams{Path: "file" + testCase.id})
	fileExists := res.StatusCode() == 200

	if fileExists && testCase.fileDeleted {
		t.Fatalf("Expected the file to be removed by the garbage collector but it has remained in the repository. Test case '%s'. Test description '%s'", testCase.id, testCase.description)
	} else if !fileExists && !testCase.fileDeleted {
		t.Fatalf("Expected the file to remain in the repository but it was removed by the garbage collector. Test case '%s'. Test description '%s'", testCase.id, testCase.description)
	}
	locations := []string{"not_deleted_file1", "not_deleted_file2", "not_deleted_file3"}
	for _, location := range locations {
		res, _ = client.GetObjectWithResponse(ctx, repo, "main", &api.GetObjectParams{Path: location})
		if res.StatusCode() != 200 {
			t.Fatalf("expected '%s' to exist. Test case '%s', Test description '%s'", location, testCase.id, testCase.description)
		}
	}
}

func testPostCommittedGC(t *testing.T) {
	ctx := context.Background()

	findingPath := filepath.Join(os.TempDir(), gcFindingsFilename)
	bytes, err := os.ReadFile(findingPath)
	if err != nil {
		t.Fatalf("Failed to read '%s': %s", findingPath, err)
	}
	fileRefPerTestCase := make(map[string]string)

	err = json.Unmarshal(bytes, &fileRefPerTestCase)
	if err != nil {
		t.Fatalf("Failed to unmarshal findings '%s': %s", findingPath, err)
	}

	for _, testCase := range testCases {
		existingRef := fileRefPerTestCase[testCase.id]
		validateGCJob(t, ctx, testCase, existingRef)
	}

	t.Logf("Tests completed successfully")
}
