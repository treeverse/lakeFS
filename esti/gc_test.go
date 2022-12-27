package esti

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
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

	awsAccessKey := viper.GetViper().GetString("aws_access_key_id")
	awsSecretKey := viper.GetViper().GetString("aws_secret_access_key")

	ctx := context.Background()

	for _, testCase := range testCases {
		t.Logf("Test case %s", testCase.id)

		fileExistingRef := prepareForGC(t, ctx, testCase, blockstoreType)
		t.Logf("fileExistingRef %s", fileExistingRef)

		repo := committedGCRepoName + testCase.id
		gcMode := getGCMode(testCase)
		runGC(repo, gcMode, testCase.markId, awsAccessKey, awsSecretKey)

		validateGCJob(t, ctx, testCase, repo, fileExistingRef)
	}

	t.Logf("Tests completed successfully")
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

func runGC(repo string, gcMode int, markId string, awsAccessKey string, awsSecretKey string) {
	sweepConfig := ""
	markIDConfig := ""
	if gcMode == markOnlyMode || gcMode == sweepOnlyMode {
		// If we are in sweep-only mode, we'll first mark and afterwards we'll sweep
		sweepConfig = "-c spark.hadoop.lakefs.gc.do_sweep=false "
	}
	if markId != "" {
		markIDConfig = "-c spark.hadoop.lakefs.gc.mark_id=" + markId + " "
	}

	out, err := exec.Command("/bin/sh", "-c",
		"cd ../clients/spark/target; echo $(pwd); CLIENT_JAR=$(ls core-301/scala-2.12/lakefs-spark-client-301-assembly-*.jar); cd ..; echo $CLIENT_JAR;").Output()
	if err != nil {
		log.Fatal("cmd0: ", err)
	}

	fmt.Printf("The jar is %s\n", out)

	cmdLine := `cd ../clients/spark/target; CLIENT_JAR=$(ls core-301/scala-2.12/lakefs-spark-client-301-assembly-*.jar); cd ..; echo $CLIENT_JAR;
			docker run --network host --add-host lakefs:127.0.0.1 -v $(pwd)/target:/local -v $(pwd)/ivy:/opt/bitnami/spark/.ivy2 --rm docker.io/bitnami/spark:3.2 spark-submit \
			--master spark://localhost:7077 \
			--conf spark.hadoop.lakefs.api.url=http://lakefs:8000/api/v1 \
			%MORE_CONFIG% \
			--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
			--conf spark.hadoop.fs.s3a.access.key=%ACCESS_KEY_ID% \
			--conf spark.hadoop.fs.s3a.secret.key=%SECRET_ACCESS_KEY% \
			--conf spark.hadoop.lakefs.api.access_key=AKIAIOSFDNN7EXAMPLEQ \
			--conf spark.hadoop.lakefs.api.secret_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
			--class io.treeverse.clients.GarbageCollector \
			--packages org.apache.hadoop:hadoop-aws:2.7.7 \
			/local/${CLIENT_JAR}`
	vars := strings.NewReplacer("%MORE_CONFIG%", markIDConfig+sweepConfig, "%ACCESS_KEY_ID%", awsAccessKey, "%SECRET_ACCESS_KEY%", awsSecretKey)
	cmdLine = vars.Replace(cmdLine)
	cmd := exec.Command("/bin/sh", "-c", cmdLine+" "+repo+" us-east-1")

	err = cmd.Run()
	if err != nil {
		log.Fatal("Running GC: ", err)
	}

	//need to uncomment it, and create the cmd as above (see existing gc test for refference)
	//if gcMode == sweepOnlyMode {
	//	cmd = exec.Command("bash", "-c",
	//		"cd target; CLIENT_JAR=$(ls core-301/scala-2.12/lakefs-spark-client-301-assembly-*.jar); cd ..; echo $CLIENT_JAR;
	//			"docker run --network host --add-host lakefs:127.0.0.1 -v $(pwd)/target:/local -v $(pwd)/ivy:/opt/bitnami/spark/.ivy2 --rm docker.io/bitnami/spark:3.2 spark-submit
	//			"--master spark://localhost:7077
	//			"--conf spark.hadoop.lakefs.api.url=http://lakefs:8000/api/v1
	//			"-c spark.hadoop.lakefs.gc.do_mark=false "+markIDConfig+ //after mark we'll run sweep
	//			"--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
	//			"--conf spark.hadoop.fs.s3a.access.key="+awsAccessKey+"
	//			"--conf spark.hadoop.fs.s3a.secret.key="+awsSecretKey+"
	//			"--conf spark.hadoop.lakefs.api.access_key=AKIAIOSFDNN7EXAMPLEQ
	//			"--conf spark.hadoop.lakefs.api.secret_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
	//			"--class io.treeverse.clients.GarbageCollector
	//			"--packages org.apache.hadoop:hadoop-aws:2.7.7
	//			"/local/${CLIENT_JAR}
	//			repo+" us-east-1")
	//
	//	err := cmd.Run()
	//	if err != nil {
	//		log.Fatal("Running GC in sweep mode ", err)
	//	}
	//}
}

func validateGCJob(t *testing.T, ctx context.Context, testCase testCase, repo string, existingRef string) {
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
