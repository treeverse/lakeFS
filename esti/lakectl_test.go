package esti

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/graveler"
)

var emptyVars = make(map[string]string)

const branchProtectTimeout = graveler.BranchUpdateMaxInterval + time.Second

func TestLakectlHelp(t *testing.T) {
	RunCmdAndVerifySuccessWithFile(t, Lakectl(), false, "lakectl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" --help", false, "lakectl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl(), true, "lakectl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" --help", true, "lakectl_help", emptyVars)
}

func TestLakectlBasicRepoActions(t *testing.T) {
	// RunCmdAndVerifySuccess(t, Lakectl()+" repo list", false, "\n", emptyVars)

	// Fails due to the usage of repos for isolation - esti creates repos in parallel and
	// the output of 'repo list' command cannot be well-defined
	// lakectl repo list with no repo created. Verifying terminal and piped formats
	// RunCmdAndVerifySuccess(t, Lakectl()+" repo list --no-color", true, "\n", emptyVars)
	// RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list", true, "lakectl_repo_list_empty.term", emptyVars)

	// Create repo using lakectl repo create and verifying the output
	// A variable mapping is used to pass random generated names for verification
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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
	repoName2 := GenerateUniqueRepositoryName()
	storage2 := GenerateUniqueStorageNamespace(repoName2)
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

	// Create repository with sample data
	repoName3 := GenerateUniqueRepositoryName()
	storage3 := GenerateUniqueStorageNamespace(repoName3)
	vars = map[string]string{
		"REPO":    repoName3,
		"STORAGE": storage3,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName3+" "+storage3+" --sample-data", false, "lakectl_repo_create_sample", vars)
}

func TestLakectlRepoCreateWithStorageID(t *testing.T) {
	// Validate the --storage-id flag (currently only allowed to be empty)
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage+" --storage-id storage1", false, "lakectl_repo_create_with_storage_id", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage+" --storage-id \"\"", false, "lakectl_repo_create", vars)
}

func TestLakectlPreSignUpload(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_initial", vars)

	filePath := "ro_1k.1"
	t.Run("upload from file", func(t *testing.T) {
		vars["FILE_PATH"] = filePath
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+filePath+" --pre-sign", false, "lakectl_fs_upload", vars)
	})
	t.Run("upload from stdin", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, "cat files/ro_1k | "+Lakectl()+" fs upload -s - lakefs://"+repoName+"/"+mainBranch+"/"+filePath+" --pre-sign", false, "lakectl_fs_upload", vars)
	})
}

func TestLakectlCommit(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	author, _ := GetAuthor(t)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"AUTHOR":  author,
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
	vars["AUTHOR"] = GetCommitter(t)
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
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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

func TestLakectlMerge(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	author, _ := GetAuthor(t)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"AUTHOR":  author,
	}

	// create repo with 'main' branch
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)
	// upload file and commit
	vars["FILE_PATH"] = filePath1
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+filePath1, false, "lakectl_fs_upload", vars)
	commitMessage := "first commit to main"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)

	// create new feature branch
	featureBranch := "feature"
	featureBranchVars := map[string]string{
		"REPO":          repoName,
		"STORAGE":       storage,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   featureBranch,
	}

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch create lakefs://"+repoName+"/"+featureBranch+" --source lakefs://"+repoName+"/"+mainBranch, false, "lakectl_branch_create", featureBranchVars)

	// update 'file1' on feature branch and commit
	vars["FILE_PATH"] = filePath1
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k_other lakefs://"+repoName+"/"+featureBranch+"/"+filePath1, false, "lakectl_fs_upload", vars)
	commitMessage = "file update on feature branch"
	vars["BRANCH"] = featureBranch
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)

	// update 'file2' on 'main' and commit
	vars["FILE_PATH"] = filePath2
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k_other lakefs://"+repoName+"/"+featureBranch+"/"+filePath2, false, "lakectl_fs_upload", vars)
	commitMessage = "another file update on main branch"
	vars["MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", vars)

	cases := []struct {
		Name   string
		Squash bool
	}{
		{Name: "regular", Squash: false},
		{Name: "squash", Squash: true},
	}
	for _, tc := range cases {
		t.Run("merge with commit message and meta "+tc.Name, func(t *testing.T) {
			destBranch := "dest-" + tc.Name
			destBranchVars := map[string]string{
				"REPO":          repoName,
				"STORAGE":       storage,
				"SOURCE_BRANCH": mainBranch,
				"DEST_BRANCH":   destBranch,
			}
			// create new destBranch from main, before the additions to main.
			RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch create lakefs://"+repoName+"/"+destBranch+" --source lakefs://"+repoName+"/"+mainBranch, false, "lakectl_branch_create", destBranchVars)

			commitMessage = "merge commit"
			vars["MESSAGE"] = commitMessage
			meta := "key1=value1,key2=value2"
			squash := ""
			if tc.Squash {
				squash = "--squash"
			}
			destBranchVars["SOURCE_BRANCH"] = featureBranch
			RunCmdAndVerifySuccessWithFile(t, Lakectl()+" merge lakefs://"+repoName+"/"+featureBranch+" lakefs://"+repoName+"/"+destBranch+" -m '"+commitMessage+"' --meta "+meta+" "+squash, false, "lakectl_merge_success", destBranchVars)

			golden := "lakectl_merge_with_commit"
			if tc.Squash {
				golden = "lakectl_merge_with_squashed_commit"
			}
			vars["AUTHOR"] = GetCommitter(t)
			RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log --amount 1 lakefs://"+repoName+"/"+destBranch, false, golden, vars)
		})
	}
}

func TestLakectlMergeAndStrategies(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" merge lakefs://"+repoName+"/"+mainBranch+" lakefs://"+repoName+"/"+featureBranch+" --strategy source-wins", false, "lakectl_merge_success", branchVars)
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
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" merge lakefs://"+repoName+"/"+mainBranch+" lakefs://"+repoName+"/"+featureBranch+" --strategy dest-wins", false, "lakectl_merge_success", branchVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs ls lakefs://"+repoName+"/"+featureBranch+"/", false, "lakectl_fs_ls_1_file", vars)
}

func TestLakectlLogNoMergesWithCommitsAndMerges(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	author, _ := GetAuthor(t)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"AUTHOR":  author,
	}

	featureBranch := "feature"
	branchVars := map[string]string{
		"REPO":          repoName,
		"STORAGE":       storage,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   featureBranch,
		"BRANCH":        featureBranch,
	}

	filePath1 := "file1"
	filePath2 := "file2"

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

	// upload 'file2' to feature branch and commit
	branchVars["FILE_PATH"] = filePath2
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+featureBranch+"/"+filePath2, false, "lakectl_fs_upload", branchVars)
	commitMessage = "second commit to feature branch"
	branchVars["MESSAGE"] = commitMessage
	vars["SECOND_MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", branchVars)

	// merge feature into main
	branchVars["SOURCE_BRANCH"] = featureBranch
	branchVars["DEST_BRANCH"] = mainBranch
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" merge lakefs://"+repoName+"/"+featureBranch+" lakefs://"+repoName+"/"+mainBranch, false, "lakectl_merge_success", branchVars)

	// log the commits without merges
	vars["AUTHOR"] = GetCommitter(t)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch+" --no-merges", false, "lakectl_log_no_merges", vars)
}

func TestLakectlLogNoMergesAndAmount(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	author, _ := GetAuthor(t)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"AUTHOR":  author,
	}

	featureBranch := "feature"
	branchVars := map[string]string{
		"REPO":          repoName,
		"STORAGE":       storage,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   featureBranch,
		"BRANCH":        featureBranch,
	}

	filePath1 := "file1"
	filePath2 := "file2"

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

	// upload 'file2' to feature branch and commit
	branchVars["FILE_PATH"] = filePath2
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+featureBranch+"/"+filePath2, false, "lakectl_fs_upload", branchVars)
	commitMessage = "second commit to feature branch"
	branchVars["MESSAGE"] = commitMessage
	vars["SECOND_MESSAGE"] = commitMessage
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+featureBranch+" -m \""+commitMessage+"\"", false, "lakectl_commit", branchVars)

	// merge feature into main
	branchVars["SOURCE_BRANCH"] = featureBranch
	branchVars["DEST_BRANCH"] = mainBranch
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" merge lakefs://"+repoName+"/"+featureBranch+" lakefs://"+repoName+"/"+mainBranch, false, "lakectl_merge_success", branchVars)

	// log the commits without merges
	vars["AUTHOR"] = GetCommitter(t)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch+" --no-merges --amount=2", false, "lakectl_log_no_merges_amount", vars)
}

func TestLakectlAnnotate(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"AUTHOR":  fmt.Sprintf("%-20s", GetCommitter(t)), // WA to the formatting of the annotate command output - support variable length author
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
	ctx := context.Background()
	userName := "test_user"
	vars := map[string]string{
		"ID": userName,
	}
	isSupported := !isBasicAuth(t, ctx)

	// Not Found
	RunCmdAndVerifyFailure(t, Lakectl()+" auth users delete --id "+userName, false, "user not found\n404 Not Found\n", vars)

	// Check unique
	if isSupported {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" auth users create --id "+userName, false, "lakectl_auth_users_create_success", vars)
	}
	RunCmdAndVerifyFailure(t, Lakectl()+" auth users create --id "+userName, false, "Already exists\n409 Conflict\n", vars)

	// Cleanup
	expected := "user not found\n404 Not Found\n"
	if isSupported {
		expected = "User deleted successfully\n"
	}
	runCmdAndVerifyResult(t, Lakectl()+" auth users delete --id "+userName, !isSupported, false, expected, vars)
}

// testing without user email for now, since it is a pain to config esti with a mail
func TestLakectlIdentity(t *testing.T) {
	author, email := GetAuthor(t)
	vars := map[string]string{
		"AUTHOR": author,
		"EMAIL":  email,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" identity", false, "lakectl_identity", vars)
}

func TestLakectlFsDownload(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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
		src := "lakefs://" + repoName + "/" + mainBranch + "/data/ro/ro_1k.0"
		sanitizedResult := runCmd(t, Lakectl()+" fs download "+src, false, false, map[string]string{})
		require.Contains(t, sanitizedResult, "download: "+src)
	})

	t.Run("single_with_dest", func(t *testing.T) {
		src := "lakefs://" + repoName + "/" + mainBranch + "/data/ro/ro_1k.0"
		dest := t.TempDir()
		sanitizedResult := runCmd(t, Lakectl()+" fs download "+src+" "+dest, false, false, map[string]string{})
		require.Contains(t, sanitizedResult, "download: "+src)
		require.Contains(t, sanitizedResult, dest+"/"+"ro_1k.0")
	})

	t.Run("single_with_rel_dest", func(t *testing.T) {
		dest := t.TempDir()

		// Change directory
		currDir, err := os.Getwd()
		require.NoError(t, err)
		require.NoError(t, os.Chdir(dest))
		defer func() {
			require.NoError(t, os.Chdir(currDir))
		}()

		src := "lakefs://" + repoName + "/" + mainBranch + "/data/ro/ro_1k.0"
		sanitizedResult := runCmd(t, Lakectl()+" fs download "+src+" ./", false, false, map[string]string{})
		require.Contains(t, sanitizedResult, "download: "+src)
		require.Contains(t, sanitizedResult, dest+"/ro_1k.0")
	})

	t.Run("single_with_recursive_flag", func(t *testing.T) {
		dest := t.TempDir()
		RunCmdAndVerifyFailure(t, Lakectl()+" fs download lakefs://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.0 "+dest+" --recursive", false, "No objects in path: lakefs://${REPO}/${BRANCH}/data/ro/ro_1k.0/\nError executing command.\n", vars)
	})

	t.Run("directory", func(t *testing.T) {
		sanitizedResult := runCmd(t, Lakectl()+" fs download --parallelism 1 lakefs://"+repoName+"/"+mainBranch+"/data --recursive", false, false, map[string]string{})
		require.Contains(t, sanitizedResult, "download ro/ro_1k.0")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.1")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.2")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.3")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.4")
		require.Contains(t, sanitizedResult, "Download Summary:")
		require.Contains(t, sanitizedResult, "Downloaded: 5")
		require.Contains(t, sanitizedResult, "Uploaded: 0")
		require.Contains(t, sanitizedResult, "Removed: 0")
	})

	t.Run("directory_with_dest", func(t *testing.T) {
		dest := t.TempDir()
		sanitizedResult := runCmd(t, Lakectl()+" fs download --parallelism 1 lakefs://"+repoName+"/"+mainBranch+"/data "+dest+" --recursive", false, false, map[string]string{})
		require.Contains(t, sanitizedResult, "download ro/ro_1k.0")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.1")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.2")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.3")
		require.Contains(t, sanitizedResult, "download ro/ro_1k.4")
		require.Contains(t, sanitizedResult, "Download Summary:")
		require.Contains(t, sanitizedResult, "Downloaded: 5")
		require.Contains(t, sanitizedResult, "Uploaded: 0")
		require.Contains(t, sanitizedResult, "Removed: 0")
	})

	t.Run("directory_without_recursive", func(t *testing.T) {
		RunCmdAndVerifyFailure(t, Lakectl()+" fs download --parallelism 1 lakefs://"+repoName+"/"+mainBranch+"/data", false, "download failed: request failed: 404 Not Found\nError executing command.\n", map[string]string{})
	})
}

func TestLakectlFsUpload(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	t.Run("single_file", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/ro_1k.0"
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, "lakectl_fs_upload", vars)
	})
	t.Run("single_file_with_separator", func(t *testing.T) {
		// First upload the file without separator
		vars["FILE_PATH"] = "data/ro/ro_1k.0_sep"
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, "lakectl_fs_upload", vars)

		// Then upload the prefix with separator
		vars["FILE_PATH"] = "data/ro/ro_1k.0_sep/"
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, "lakectl_fs_upload", vars)
	})
	t.Run("single_file_with_recursive", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/ro_1k.0"
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload --recursive -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, "lakectl_fs_upload", vars)
	})
	t.Run("dir", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/"
		sanitizedResult := runCmd(t, Lakectl()+" fs upload --recursive -s files/ lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, false, vars)

		require.Contains(t, sanitizedResult, "diff 'local://files/' <--> 'lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+"'...")
		require.Contains(t, sanitizedResult, "upload ro_1k")
		require.Contains(t, sanitizedResult, "upload ro_1k_other")
		require.Contains(t, sanitizedResult, "upload upload_file.txt")
		require.Contains(t, sanitizedResult, "Upload Summary:")
		require.Contains(t, sanitizedResult, "Downloaded: 0")
		require.Contains(t, sanitizedResult, "Uploaded: 3")
		require.Contains(t, sanitizedResult, "Removed: 0")
	})
	t.Run("exist_dir", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/"
		sanitizedResult := runCmd(t, Lakectl()+" fs upload --recursive -s files/ lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, false, vars)
		require.Contains(t, sanitizedResult, "diff 'local://files/' <--> 'lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"]+"'...")
		require.Contains(t, sanitizedResult, "Upload Summary:")
		require.Contains(t, sanitizedResult, "No changes")
	})
	t.Run("dir_without_recursive", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/"
		RunCmdAndVerifyFailureContainsText(t, Lakectl()+" fs upload -s files/ lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, "read files/: is a directory", vars)
	})
	t.Run("dir_without_recursive_to_file", func(t *testing.T) {
		vars["FILE_PATH"] = "data/ro/1.txt"
		RunCmdAndVerifyFailureContainsText(t, Lakectl()+" fs upload -s files/ lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, "read files/: is a directory", vars)
	})
	t.Run("directory_marker_with_trailing_slash", func(t *testing.T) {
		vars["FILE_PATH"] = "dir-with-marker/"
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s /dev/null lakefs://"+repoName+"/"+mainBranch+"/dir-with-marker/", false, "lakectl_fs_upload_dir_marker", vars)
	})
}

func getStorageConfig(t *testing.T) *apigen.StorageConfig {
	storageResp, err := client.GetStorageConfigWithResponse(context.Background())
	if err != nil {
		t.Fatalf("GetStorageConfig failed: %s", err)
	}
	if storageResp.JSON200 == nil {
		t.Fatalf("GetStorageConfig failed with stats: %s", storageResp.Status())
	}
	return storageResp.JSON200
}

func TestLakectlFsUpload_protectedBranch(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+vars["REPO"]+" "+vars["STORAGE"], false, "lakectl_repo_create", vars)
	runCmd(t, Lakectl()+" branch-protect add lakefs://"+vars["REPO"]+"/  '*'", false, false, vars)
	RunCmdAndVerifyContainsText(t, Lakectl()+" branch-protect list lakefs://"+vars["REPO"]+"/ ", false, "*", vars)
	// BranchUpdateMaxInterval - sleep in order to overcome branch update caching
	time.Sleep(branchProtectTimeout)
	vars["FILE_PATH"] = "ro_1k.0"
	RunCmdAndVerifyFailure(t, Lakectl()+" fs upload lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, "cannot write to protected branch\n403 Forbidden\n", vars)
}

func TestLakectlFsRm_protectedBranch(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+vars["REPO"]+" "+vars["STORAGE"], false, "lakectl_repo_create", vars)
	vars["FILE_PATH"] = "ro_1k.0"
	runCmd(t, Lakectl()+" fs upload lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+"/"+vars["FILE_PATH"]+" -s files/ro_1k", false, false, vars)
	runCmd(t, Lakectl()+" commit lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+" --allow-empty-message -m \" \"", false, false, vars)
	runCmd(t, Lakectl()+" branch-protect add lakefs://"+vars["REPO"]+"/  '*'", false, false, vars)
	// BranchUpdateMaxInterval - sleep in order to overcome branch update caching
	time.Sleep(branchProtectTimeout)
	RunCmdAndVerifyContainsText(t, Lakectl()+" branch-protect list lakefs://"+vars["REPO"]+"/ ", false, "*", vars)
	RunCmdAndVerifyFailure(t, Lakectl()+" fs rm lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+"/"+vars["FILE_PATH"], false, "cannot write to protected branch\n403 Forbidden\n", vars)
}

func TestLakectlFsPresign(t *testing.T) {
	config := getStorageConfig(t)
	if !config.PreSignSupport {
		t.Skip()
	}
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	// upload some data
	const totalObjects = 2
	for i := 0; i < totalObjects; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/ro/ro_1k.%d", i)
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, "lakectl_fs_upload", vars)
	}

	goldenFile := "lakectl_fs_presign"
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs presign lakefs://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.0", false, goldenFile, map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"PATH":    "data/ro",
		"FILE":    "ro_1k.0",
	})
}

func TestLakectlFsStat(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	// upload some data
	const totalObjects = 2
	for i := 0; i < totalObjects; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/ro/ro_1k.%d", i)
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, "lakectl_fs_upload", vars)
	}

	t.Run("default", func(t *testing.T) {
		config := getStorageConfig(t)
		goldenFile := "lakectl_stat_default"
		if config.PreSignSupport {
			goldenFile = "lakectl_stat_pre_sign"
			if config.BlockstoreType == "s3" {
				goldenFile = "lakectl_stat_pre_sign_with_expiry"
			}
		}
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs stat lakefs://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.0", false, goldenFile, map[string]string{
			"REPO":    repoName,
			"STORAGE": storage,
			"BRANCH":  mainBranch,
			"PATH":    "data/ro",
			"FILE":    "ro_1k.0",
		})
	})

	t.Run("no_presign", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs stat --pre-sign=false lakefs://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.0", false, "lakectl_stat_default", map[string]string{
			"REPO":    repoName,
			"STORAGE": storage,
			"BRANCH":  mainBranch,
			"PATH":    "data/ro",
			"FILE":    "ro_1k.0",
		})
	})

	t.Run("pre-sign", func(t *testing.T) {
		config := getStorageConfig(t)
		if !config.PreSignSupport {
			t.Skip("No pre-sign support for this storage")
		}
		goldenFile := "lakectl_stat_pre_sign"
		if config.BlockstoreType == "s3" {
			goldenFile = "lakectl_stat_pre_sign_with_expiry"
		}
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs stat --pre-sign lakefs://"+repoName+"/"+mainBranch+"/data/ro/ro_1k.1", false, goldenFile, map[string]string{
			"REPO":    repoName,
			"STORAGE": storage,
			"BRANCH":  mainBranch,
			"PATH":    "data/ro",
			"FILE":    "ro_1k.1",
		})
	})
}

func TestLakectlImport(t *testing.T) {
	// TODO(barak): generalize test to work all supported object stores
	const IngestTestBucketPath = "s3://esti-system-testing-data/ingest-test-data/"
	skipOnSchemaMismatch(t, IngestTestBucketPath)

	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"OBJECTS": "10",
	}

	const from = "s3://lakectl-ingest-test-data"
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" import --no-progress --from "+from+" --to lakefs://"+repoName+"/"+mainBranch+"/to/", false, "lakectl_import", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" import --no-progress --from "+from+" --to lakefs://"+repoName+"/"+mainBranch+"/too/ --message \"import too\"", false, "lakectl_import_with_message", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" import --no-progress --from "+from+" --to lakefs://"+repoName+"/"+mainBranch+"/another/import/ --merge", false, "lakectl_import_and_merge", vars)
}

func TestLakectlCherryPick(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	branch1 := "branch1"
	branch2 := "branch2"
	branchVars := map[string]string{
		"REPO":          repoName,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   "branch1",
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch create lakefs://"+repoName+"/"+branch1+" --source lakefs://"+repoName+"/"+mainBranch, false, "lakectl_branch_create", branchVars)
	branchVars["DEST_BRANCH"] = "branch2"
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch create lakefs://"+repoName+"/"+branch2+" --source lakefs://"+repoName+"/"+mainBranch, false, "lakectl_branch_create", branchVars)

	// upload some data
	vars["BRANCH"] = branch1
	for i := 1; i <= 3; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/%d", i)
		commitMessage := fmt.Sprintf("commit %d", i)
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+branch1+"/"+vars["FILE_PATH"], false, "lakectl_fs_upload", vars)

		commitVars := map[string]string{
			"REPO":    repoName,
			"BRANCH":  branch1,
			"MESSAGE": commitMessage,
		}
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+branch1+` -m "`+commitMessage+`" --epoch-time-seconds 0`, false, "lakectl_commit", commitVars)
	}

	vars["BRANCH"] = branch2
	for i := 3; i <= 5; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/%d", i)
		commitMessage := fmt.Sprintf("commit %d", i)
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k_other lakefs://"+repoName+"/"+branch2+"/"+vars["FILE_PATH"], false, "lakectl_fs_upload", vars)

		commitVars := map[string]string{
			"REPO":    repoName,
			"BRANCH":  branch2,
			"MESSAGE": commitMessage,
		}
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+branch2+` -m "`+commitMessage+`" --epoch-time-seconds 0`, false, "lakectl_commit", commitVars)
	}

	t.Run("success", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" cherry-pick lakefs://"+repoName+"/"+branch1+" lakefs://"+repoName+"/"+mainBranch, false, "lakectl_cherry_pick", map[string]string{
			"REPO":    repoName,
			"BRANCH":  mainBranch,
			"MESSAGE": "commit 3",
		})

		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" cherry-pick lakefs://"+repoName+"/"+branch2+"~1"+" lakefs://"+repoName+"/"+mainBranch, false, "lakectl_cherry_pick", map[string]string{
			"REPO":    repoName,
			"BRANCH":  mainBranch,
			"MESSAGE": "commit 4",
		})
	})

	t.Run("conflict", func(t *testing.T) {
		RunCmdAndVerifyFailure(t, Lakectl()+" cherry-pick lakefs://"+repoName+"/"+branch1+" lakefs://"+repoName+"/"+branch2, false,
			fmt.Sprintf("Branch: lakefs://%s/%s\nupdate branch: conflict found\n409 Conflict\n", repoName, branch2), nil)
	})
}

func TestLakectlBisect(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"AUTHOR":  GetCommitter(t),
	}

	r := strings.NewReplacer("{lakectl}", Lakectl(), "{repo}", repoName, "{storage}", storage, "{branch}", "main")
	runCmd(t, r.Replace("{lakectl} repo create lakefs://{repo} {storage}"), false, false, nil)

	// Reset bisect state in case of re-run
	runCmd(t, Lakectl()+" bisect reset", false, false, nil)

	// generate to test data
	for i := 0; i < 5; i++ {
		obj := fmt.Sprintf("file%d", i)
		runCmd(t, r.Replace("{lakectl} fs upload -s files/ro_1k lakefs://{repo}/{branch}/")+obj, false, false, nil)
		commit := fmt.Sprintf("commit%d", i)
		runCmd(t, r.Replace("{lakectl} commit lakefs://{repo}/{branch} -m ")+commit, false, false, nil)
	}
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{lakectl} bisect reset"), false,
		"lakectl_bisect_reset_not_started", vars)
	RunCmdAndVerifyFailureWithFile(t, r.Replace("{lakectl} bisect good"), false,
		"lakectl_bisect_good_invalid", vars)
	RunCmdAndVerifyFailureWithFile(t, r.Replace("{lakectl} bisect bad"), false,
		"lakectl_bisect_bad_invalid", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{lakectl} bisect start lakefs://{repo}/{branch} lakefs://{repo}/{branch}~5"), false,
		"lakectl_bisect_start", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{lakectl} bisect view"), false,
		"lakectl_bisect_view1", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{lakectl} bisect good"), false,
		"lakectl_bisect_good1", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{lakectl} bisect view"), false,
		"lakectl_bisect_view2", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{lakectl} bisect log"), false,
		"lakectl_bisect_log1", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{lakectl} bisect bad"), false,
		"lakectl_bisect_bad1", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{lakectl} bisect log"), false,
		"lakectl_bisect_log2", vars)
	RunCmdAndVerifySuccessWithFile(t, r.Replace("{lakectl} bisect reset"), false,
		"lakectl_bisect_reset", vars)
}

func TestLakectlUsage(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	r := strings.NewReplacer("{lakectl}", Lakectl(), "{repo}", repoName, "{storage}", storage, "{branch}", "main")
	runCmd(t, r.Replace("{lakectl} repo create lakefs://{repo} {storage}"), false, false, nil)
	runCmd(t, r.Replace("{lakectl} repo list"), false, false, nil)
	RunCmdAndVerifyFailureWithFile(t, r.Replace("{lakectl} usage summary"), false, "lakectl_usage_summary", vars)
}

func TestLakectlBranchProtection(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch-protect add lakefs://"+repoName+" "+mainBranch, false, "lakectl_empty", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch-protect list lakefs://"+repoName, false, "lakectl_branch_protection_list.term", vars)
}

// TestLakectlAbuse runs a series of abuse commands to test the functionality of lakectl abuse (not in order to test how lakeFS handles abuse)
func TestLakectlAbuse(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	fromFile := ""
	const totalObjects = 5
	for i := 0; i < totalObjects; i++ {
		vars["FILE_PATH"] = fmt.Sprintf("data/ro/ro_1k.%d", i)
		fromFile = fromFile + vars["FILE_PATH"] + "\n"
		runCmd(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+vars["FILE_PATH"], false, false, vars)
	}
	f, err := os.CreateTemp("", "abuse-read")
	require.NoError(t, err)
	_, err = f.WriteString(fromFile)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	const (
		abuseAmount      = 50
		abuseParallelism = 3
	)
	tests := []struct {
		Cmd            string
		Amount         int
		AdditionalArgs string
	}{
		{
			Cmd:    "commit",
			Amount: 10,
		},
		{
			Cmd:            "create-branches",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d", abuseParallelism),
		},
		{
			Cmd:            "link-same-object",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d", abuseParallelism),
		},
		{
			Cmd:            "list",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d", abuseParallelism),
		},
		{
			Cmd:            "random-read",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d --from-file %s", abuseParallelism, f.Name()),
		},
		{
			Cmd:            "random-delete",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d --from-file %s", abuseParallelism, f.Name()),
		},
		{
			Cmd:            "random-write",
			Amount:         abuseAmount,
			AdditionalArgs: fmt.Sprintf("--parallelism %d", abuseParallelism),
		},
	}
	for _, tt := range tests {
		t.Run(tt.Cmd, func(t *testing.T) {
			lakefsURI := "lakefs://" + repoName + "/" + mainBranch
			RunCmdAndVerifyContainsText(t, fmt.Sprintf("%s abuse %s %s --amount %d %s", Lakectl(), tt.Cmd, lakefsURI, tt.Amount, tt.AdditionalArgs), false, "errors: 0", map[string]string{})
		})
	}
}

func TestLakectlBranchList(t *testing.T) {
	tempBranch := "temp"
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	}

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	branchVars := map[string]string{
		"REPO":          repoName,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   tempBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch create lakefs://"+repoName+"/"+tempBranch+" --source lakefs://"+repoName+"/"+mainBranch, false, "lakectl_branch_create", branchVars)

	t.Run("default", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch list lakefs://"+repoName, false, "lakectl_branch_list", branchVars)
	})

	t.Run("with prefix", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch list lakefs://"+repoName+" --prefix="+tempBranch, false, "lakectl_branch_list_prefix", branchVars)
	})

	t.Run("with prefix and amount", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch list lakefs://"+repoName+" --prefix="+tempBranch+" --amount=1", false, "lakectl_branch_list_prefix", branchVars)
	})
}

func TestLakectlRepoList(t *testing.T) {
	repoName := "a" + GenerateUniqueRepositoryName()
	repoName2 := "b" + GenerateUniqueRepositoryName()
	storage1 := GenerateUniqueStorageNamespace(repoName)
	storage2 := GenerateUniqueStorageNamespace(repoName2)

	repo1Vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage1,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage1, false, "lakectl_repo_create", repo1Vars)

	repo2Vars := map[string]string{
		"REPO":    repoName2,
		"STORAGE": storage2,
		"BRANCH":  mainBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName2+" "+storage2, false, "lakectl_repo_create", repo2Vars)

	t.Run("with prefix", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list --prefix=b", false, "lakectl_repo_list_prefix", repo2Vars)
	})

	t.Run("with prefix and amount", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo list --prefix=b --amount=1", false, "lakectl_repo_list_prefix", repo2Vars)
	})
}

func TestLakectlTagList(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"TAG":     "tag1",
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" tag create lakefs://"+repoName+"/"+vars["TAG"]+" lakefs://"+repoName+"/"+mainBranch, false, "lakectl_tag_create", vars)

	vars["TAG"] = "tag2"
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" tag create lakefs://"+repoName+"/"+vars["TAG"]+" lakefs://"+repoName+"/"+mainBranch, false, "lakectl_tag_create", vars)

	vars_test := map[string]string{
		"TAG1": "tag1",
		"TAG2": "tag2",
	}
	t.Run("default", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" tag list lakefs://"+repoName, false, "lakectl_tag_list", vars_test)
	})

	t.Run("with prefix", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" tag list lakefs://"+repoName+" --prefix="+vars_test["TAG1"], false, "lakectl_tag_list_prefix", vars_test)
	})

	t.Run("with prefix and amount", func(t *testing.T) {
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" tag list lakefs://"+repoName+" --prefix="+vars_test["TAG1"]+" --amount=1", false, "lakectl_tag_list_prefix", vars_test)
	})
}
