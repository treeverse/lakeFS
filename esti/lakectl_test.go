package esti

import (
	"context"
	"fmt"
	"testing"
	"time"
)

var emptyVars = make(map[string]string)

func TestLakectlHelp(t *testing.T) {
	SkipTestIfAskedTo(t)
	RunCmdAndVerifySuccessWithFile(t, Lakectl(), false, "lakectl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" --help", false, "lakectl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl(), true, "lakectl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" --help", true, "lakectl_help", emptyVars)
}

func TestLakectlBasicRepoActions(t *testing.T) {
	SkipTestIfAskedTo(t)
	// RunCmdAndVerifySuccess(t, Lakectl()+" repo list", false, "\n", emptyVars)

	// Fails due to the usage of repos for isolation - esti creates repos in parallel and
	// the output of 'repo list' command cannot be well-defined
	// lakectl repo list with no repo created. Verifying terminal and piped formats
	// RunCmdAndVerifySuccess(t, Lakectl()+" repo list --no-color", true, "\n", emptyVars)
	// RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list", true, "lakectl_repo_list_empty.term", emptyVars)

	// Create repo using lakectl repo create and verifying the output
	// A variable mapping is used to pass random generated names for verification
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	// lakectl repo list is expected to show the created repo

	// Fails due to the usage of repos for isolation - esti creates repos in parallel and
	// the output of 'repo list' command cannot be well-defined
	// RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list", false, "lakectl_repo_list_1", vars)
	// RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list --no-color", true, "lakectl_repo_list_1", vars)
	// RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list", true, "lakectl_repo_list_1.term", vars)

	// Create a second repo. Vars for the first repo are being saved in a new map, in order to be used
	// for a follow-up verification with 'repo list'
	// listVars := map[string]string{
	// 	"REPO1":    repoName,
	// 	"STORAGE1": storage,
	// 	"BRANCH1":  mainBranch,
	// }

	// Trying to create the same repo again fails and does not change the list
	newStorage := storage + "/new-storage/"
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+newStorage, false, "lakectl_repo_create_not_unique", vars)

	// Fails due to the usage of repos for isolation - esti creates repos in parallel and
	// the output of 'repo list' command cannot be well-defined
	// RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list", false, "lakectl_repo_list_1", vars)

	// Create another repo with non-default branch
	repoName2 := generateUniqueRepositoryName()
	storage2 := generateUniqueStorageNamespace(repoName2)
	notDefaultBranchName := "branch-123"
	vars["REPO"] = repoName2
	vars["STORAGE"] = storage2
	vars["BRANCH"] = notDefaultBranchName
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName2+" "+storage2+" -d "+notDefaultBranchName, true, "lakectl_repo_create", vars)

	// The generated names are also added to the verification vars map

	// Fails due to the usage of repos for isolation - esti creates repos in parallel and
	// the output of 'repo list' command cannot be well-defined
	// listVars["REPO2"] = repoName2
	// listVars["STORAGE2"] = storage2
	// listVars["BRANCH2"] = notDefaultBranchName
	// RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list", false, "lakectl_repo_list_2", listVars)
	// RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list --no-color", true, "lakectl_repo_list_2", listVars)
	// RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list", true, "lakectl_repo_list_2.term", listVars)

	// RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list --after "+repoName, false, "lakectl_repo_list_1", vars)

	// Trying to delete a repo using malformed_uri
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" repo delete "+repoName2+" -y", false, "lakectl_repo_delete_malformed_uri", vars)

	// Trying to delete a repo using malformed_uri, using terminal
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" repo delete "+repoName2+" -y", true, "lakectl_repo_delete_malformed_uri.term", vars)

	// Deleting a repo
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo delete lakefs://"+repoName2+" -y", false, "lakectl_repo_delete", vars)

	// Trying to delete again
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" repo delete lakefs://"+repoName2+" -y", false, "lakectl_repo_delete_not_found", vars)
}

func TestLakectlCommit(t *testing.T) {
	SkipTestIfAskedTo(t)
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_404", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_initial", vars)

	filePath := "ro_1k.1"
	vars["FILE_PATH"] = filePath
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+filePath, false, "lakectl_fs_upload", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_initial", vars)
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch, false, "lakectl_commit_no_msg", vars)
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \" \"", false, "lakectl_commit_no_msg", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" --allow-empty-message -m \" \"", false, "lakectl_commit_with_empty_msg_flag", vars)
	filePath = "ro_1k.2"
	vars["FILE_PATH"] = filePath
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+filePath, false, "lakectl_fs_upload", vars)
	commitMessage := "esti_lakectl:TestCommit"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_with_commit", vars)
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \"esti_lakectl:should fail\"", false, "lakectl_commit_no_change", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_with_commit", vars)

	filePath = "ro_1k.3"
	vars["FILE_PATH"] = filePath
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+filePath, false, "lakectl_fs_upload", vars)
	commitMessage = "commit with a very old date"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+` -m "`+commitMessage+`" --epoch-time-seconds 0`, false, "lakectl_commit", vars)
	vars["DATE"] = time.Unix(0, 0).String()
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch+" --amount 1", false, "lakectl_log_with_commit_custom_date", vars)

	// verify the latest commit using 'show commit'
	ctx := context.Background()
	getBranchResp, err := client.GetBranchWithResponse(ctx, repoName, mainBranch)
	if err != nil {
		t.Fatal("Failed to get branch information", err)
	}
	if getBranchResp.JSON200 == nil {
		t.Fatalf("Get branch status code=%d, expected 200", getBranchResp.StatusCode())
	}
	lastCommitID := getBranchResp.JSON200.CommitId

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" show commit lakefs://"+repoName+"/"+lastCommitID, false, "lakectl_show_commit", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" show commit lakefs://"+repoName+"/"+lastCommitID+" --show-meta-range-id", false, "lakectl_show_commit_metarange", vars)
}

func TestLakectlBranchAndTagValidation(t *testing.T) {
	SkipTestIfAskedTo(t)
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	validTagName := "my.valid.tag"

	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"TAG":     validTagName,
	}
	invalidBranchName := "my.invalid.branch"
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)
	vars["BRANCH"] = mainBranch
	vars["FILE_PATH"] = "a/b/c"
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/a/b/c", false, "lakectl_fs_upload", vars)
	commitMessage := "another file update on main branch"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)

	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" branch create lakefs://"+repoName+"/"+invalidBranchName+" --source lakefs://"+repoName+"/"+mainBranch, false, "lakectl_branch_create_invalid", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" tag create lakefs://"+repoName+"/"+validTagName+" lakefs://"+repoName+"/"+mainBranch, false, "lakectl_tag_create", vars)
	vars["TAG"] = "tag2"
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" tag create lakefs://"+repoName+"/"+vars["TAG"]+" lakefs://"+repoName+"/"+mainBranch+"~1", false, "lakectl_tag_create", vars)
	vars["TAG"] = "tag3"
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" tag create lakefs://"+repoName+"/"+vars["TAG"]+" lakefs://"+repoName+"/"+mainBranch+"^1", false, "lakectl_tag_create", vars)
	vars["TAG"] = "tag4"
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" tag create lakefs://"+repoName+"/"+vars["TAG"]+" lakefs://"+repoName+"/"+mainBranch+"~", false, "lakectl_tag_create", vars)

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" tag show lakefs://"+repoName+"/"+vars["TAG"], false, "lakectl_tag_show", vars)
}

func TestLakectlMergeAndStrategies(t *testing.T) {
	SkipTestIfAskedTo(t)
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	featureBranch := "feature"
	branchVars := map[string]string{
		"REPO":          repoName,
		"STORAGE":       storage,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   featureBranch,
	}

	filePath1 := "file1"
	filePath2 := "file2"
	lsVars := map[string]string{
		"REPO":        repoName,
		"STORAGE":     storage,
		"FILE_PATH_1": filePath1,
		"FILE_PATH_2": filePath2,
	}

	// create repo with 'main' branch
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	// upload 'file1' and commit
	vars["FILE_PATH"] = filePath1
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+filePath1, false, "lakectl_fs_upload", vars)
	commitMessage := "first commit to main"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)

	// create new branch 'feature'
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch create lakefs://"+repoName+"/"+featureBranch+" --source lakefs://"+repoName+"/"+mainBranch, false, "lakectl_branch_create", branchVars)

	// update 'file1' on 'main' and commit
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k_other lakefs://"+repoName+"/"+mainBranch+"/"+filePath1, false, "lakectl_fs_upload", vars)
	commitMessage = "file update on main branch"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)

	// upload 'file2' on 'feature', delete 'file1' and commit
	vars["BRANCH"] = featureBranch
	vars["FILE_PATH"] = filePath2
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+featureBranch+"/"+filePath2, false, "lakectl_fs_upload", vars)
	RunCmdAndVerifySuccess(t, Lakectl()+" fs rm lakefs://"+repoName+"/"+featureBranch+"/"+filePath1, false, "", vars)
	commitMessage = "delete file on feature branch"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)

	// try to merge - conflict
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" merge lakefs://"+repoName+"/"+mainBranch+" lakefs://"+repoName+"/"+featureBranch, false, "lakectl_merge_conflict", branchVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+featureBranch+"/", false, "lakectl_fs_ls_1_file", vars)

	// merge with strategy 'source-wins' - updated 'file1' from main is added to 'feature'
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" merge lakefs://"+repoName+"/"+mainBranch+" lakefs://"+repoName+"/"+featureBranch+" --strategy source-wins", false, "lakectl_merge_source_wins", branchVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+featureBranch+"/", false, "lakectl_fs_ls_2_file", lsVars)

	// update 'file1' again on 'main' and commit
	vars["BRANCH"] = mainBranch
	vars["FILE_PATH"] = filePath1
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+filePath1, false, "lakectl_fs_upload", vars)
	commitMessage = "another file update on main branch"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)

	// delete 'file1' on 'feature' again, and commit
	vars["BRANCH"] = featureBranch
	RunCmdAndVerifySuccess(t, Lakectl()+" fs rm lakefs://"+repoName+"/"+featureBranch+"/"+filePath1, false, "", vars)
	commitMessage = "delete file on feature branch again"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)

	// try to merge - conflict
	vars["FILE_PATH"] = filePath2
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" merge lakefs://"+repoName+"/"+mainBranch+" lakefs://"+repoName+"/"+featureBranch, false, "lakectl_merge_conflict", branchVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+featureBranch+"/", false, "lakectl_fs_ls_1_file", vars)

	// merge with strategy 'dest-wins' - 'file1' is not added to 'feature'
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" merge lakefs://"+repoName+"/"+mainBranch+" lakefs://"+repoName+"/"+featureBranch+" --strategy dest-wins", false, "lakectl_merge_source_wins", branchVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+featureBranch+"/", false, "lakectl_fs_ls_1_file", vars)
}

func TestLakectlAnnotate(t *testing.T) {
	SkipTestIfAskedTo(t)
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	// create fresh repo with 'main' branch
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	path := "aaa/bbb/ccc"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+path, false, "lakectl_fs_upload", vars)
	path = "aaa/bbb/ddd"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+path, false, "lakectl_fs_upload", vars)
	commitMessage := "commit #1"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)
	path = "aaa/bbb/eee"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+path, false, "lakectl_fs_upload", vars)
	commitMessage = "commit #2"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)
	path = "aaa/fff/ggg"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+path, false, "lakectl_fs_upload", vars)
	path = "aaa/fff/ggh"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+path, false, "lakectl_fs_upload", vars)
	path = "aaa/hhh"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+path, false, "lakectl_fs_upload", vars)
	path = "iii/jjj"
	vars["FILE_PATH"] = path
	commitMessage = "commit #3"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+path, false, "lakectl_fs_upload", vars)
	path = "iii/kkk/lll"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+path, false, "lakectl_fs_upload", vars)
	path = "mmm"
	vars["FILE_PATH"] = path
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+path, false, "lakectl_fs_upload", vars)
	commitMessage = "commit #4"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)

	delete(vars, "FILE_PATH")
	delete(vars, "MESSAGE")

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" annotate lakefs://"+repoName+"/"+mainBranch+"/", false, "lakectl_annotate_top", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" annotate lakefs://"+repoName+"/"+mainBranch+"/ --recursive", false, "lakectl_annotate_top_recursive", vars)

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" annotate lakefs://"+repoName+"/"+mainBranch+"/a", false, "lakectl_annotate_a", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" annotate lakefs://"+repoName+"/"+mainBranch+"/a --recursive", false, "lakectl_annotate_a_recursive", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" annotate lakefs://"+repoName+"/"+mainBranch+"/aa", false, "lakectl_annotate_a", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" annotate lakefs://"+repoName+"/"+mainBranch+"/aaa", false, "lakectl_annotate_a", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" annotate lakefs://"+repoName+"/"+mainBranch+"/aaa/ --recursive", false, "lakectl_annotate_a_recursive", vars)

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" annotate lakefs://"+repoName+"/"+mainBranch+"/iii/kkk/l", false, "lakectl_annotate_iiikkklll", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" annotate lakefs://"+repoName+"/"+mainBranch+"/iii/kkk/l --recursive", false, "lakectl_annotate_iiikkklll", vars)
}

func TestLakectlAuthUsers(t *testing.T) {
	SkipTestIfAskedTo(t)
	userName := "test_user"
	vars := map[string]string{
		"ID": userName,
	}

	// Not Found
	RunCmdAndVerifyFailure(t, Lakectl()+" auth users delete --id "+userName, false, "user not found\n404 Not Found\n", vars)

	// Check unique
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" auth users create --id "+userName, false, "lakectl_auth_users_create_success", vars)
	RunCmdAndVerifyFailure(t, Lakectl()+" auth users create --id "+userName, false, "Already exists\n409 Conflict\n", vars)

	// Cleanup
	RunCmdAndVerifySuccess(t, Lakectl()+" auth users delete --id "+userName, false, "User deleted successfully\n", vars)
}

func TestLakectlIngestS3(t *testing.T) {
	SkipTestIfAskedTo(t)
	// Specific S3 test - due to the limitation on ingest source type that has to match lakefs underlying block store,
	// this test can only run on AWS setup, and therefore is skipped for other store types
	skipOnSchemaMismatch(t, IngestTestBucketPath)

	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	const (
		lakectlIngestBucket  = "lakectl-ingest-test-data"
		expectedIngestOutput = "Staged 10 external objects (total of 10.2 kB)"
	)

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)
	RunCmdAndVerifyContainsText(t, Lakectl()+" ingest --from s3://"+lakectlIngestBucket+" --to lakefs://"+repoName+"/"+mainBranch+"/", false, expectedIngestOutput, vars)
	RunCmdAndVerifyContainsText(t, Lakectl()+" ingest --from s3://"+lakectlIngestBucket+" --to lakefs://"+repoName+"/"+mainBranch+"/to-pref/", false, expectedIngestOutput, vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+mainBranch+"/", false, "lakectl_fs_ls_after_ingest", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+mainBranch+"/ --recursive", false, "lakectl_fs_ls_after_ingest_recursive", vars)

	// rerunning the same ingest command should succeed and have no effect
	RunCmdAndVerifyContainsText(t, Lakectl()+" ingest --from s3://"+lakectlIngestBucket+" --to lakefs://"+repoName+"/"+mainBranch+"/", false, expectedIngestOutput, vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+mainBranch+"/", false, "lakectl_fs_ls_after_ingest", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+mainBranch+"/ --recursive", false, "lakectl_fs_ls_after_ingest_recursive", vars)

	// 'from' can also be specified with terminating "/"
	RunCmdAndVerifyContainsText(t, Lakectl()+" ingest --from s3://"+lakectlIngestBucket+"/ --to lakefs://"+repoName+"/"+mainBranch+"/", false, expectedIngestOutput, vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+mainBranch+"/", false, "lakectl_fs_ls_after_ingest", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+mainBranch+"/ --recursive", false, "lakectl_fs_ls_after_ingest_recursive", vars)
}

func TestLakectlFsDownload(t *testing.T) {
	SkipTestIfAskedTo(t)
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	// upload some data
	const totalObjects = 5
	for i := 0; i < totalObjects; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/ro/ro_1k.%d", i)
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, "lakectl_fs_upload", vars)
	}

	t.Run("single", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs download lakefs://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.0", false, "lakectl_fs_download", map[string]string{
			"REPO":    repoName,
			"STORAGE": storage,
			"BRANCH":  mainBranch,
			"PATH":    "data/ro",
			"FILE":    "ro_1k.0",
		})
	})

	t.Run("single_with_dest", func(t *testing.T) {
		dest := t.TempDir()
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs download lakefs://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.1 "+dest, false, "lakectl_fs_download_custom", map[string]string{
			"REPO":    repoName,
			"STORAGE": storage,
			"BRANCH":  mainBranch,
			"DEST":    dest,
			"PATH":    "data/ro",
			"FILE":    "ro_1k.1",
		})
	})

	t.Run("recursive", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs download --recursive --parallel 1 lakefs://"+repoName+"/"+mainBranch+"/data", false, "lakectl_fs_download_recursive", map[string]string{
			"REPO":        repoName,
			"STORAGE":     storage,
			"BRANCH":      mainBranch,
			"PATH":        "data",
			"FILE_PREFIX": "ro/ro_1k",
		})
	})

	t.Run("recursive_with_dest", func(t *testing.T) {
		dest := t.TempDir()
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs download --recursive --parallel 1 lakefs://"+repoName+"/"+mainBranch+"/data "+dest, false, "lakectl_fs_download_recursive_custom", map[string]string{
			"REPO":        repoName,
			"STORAGE":     storage,
			"BRANCH":      mainBranch,
			"DEST":        dest,
			"PATH":        "data",
			"FILE_PREFIX": "ro/ro_1k",
		})
	})
}

func TestLakectlImport(t *testing.T) {
	SkipTestIfAskedTo(t)

	// TODO(barak): generalize test to work all supported object stores
	skipOnSchemaMismatch(t, IngestTestBucketPath)

	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":            repoName,
		"STORAGE":         storage,
		"BRANCH":          mainBranch,
		"IMPORTED_BRANCH": "_" + mainBranch + "_imported",
		"OBJECTS":         "10",
	}

	const from = "s3://lakectl-ingest-test-data"
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" import --no-progress --from "+from+" --to lakefs://"+repoName+"/"+mainBranch+"/to/", false, "lakectl_import", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" import --no-progress --from "+from+" --to lakefs://"+repoName+"/"+mainBranch+"/too/ --message \"import too\"", false, "lakectl_import_with_message", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" import --no-progress --from "+from+" --to lakefs://"+repoName+"/"+mainBranch+"/another/import/ --merge", false, "lakectl_import_and_merge", vars)
}
