package nessie

import (
	"testing"
)

var emptyVars = make(map[string]string)

func TestLakectlHelp(t *testing.T) {
	RunCmdAndVerifySuccessWithFile(t, Lakectl(), false, "lakectl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" --help", false, "lakectl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl(), true, "lakectl_help", emptyVars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" --help", true, "lakectl_help", emptyVars)

}

func TestBasicRepoActions(t *testing.T) {

	// RunCmdAndVerifySuccess(t, Lakectl()+" repo list", false, "\n", emptyVars)

	// Fails due to the usage of repos for isolation - nessie creates repos in parallel and
	// the output of 'repo list' command cannot be well defined
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

	// Fails due to the usage of repos for isolation - nessie creates repos in parallel and
	// the output of 'repo list' command cannot be well defined
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
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create_not_unique", vars)

	// Fails due to the usage of repos for isolation - nessie creates repos in parallel and
	// the output of 'repo list' command cannot be well defined
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

	// Fails due to the usage of repos for isolation - nessie creates repos in parallel and
	// the output of 'repo list' command cannot be well defined
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

func TestCommit(t *testing.T) {
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
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/ro_1k.1", false, "lakectl_fs_upload", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_initial", vars)
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch, false, "lakectl_commit_no_msg", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \"nessie_lakectl:TestCommit\"", false, "lakectl_commit", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_with_commit", vars)
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" -m \"nessie_lakectl:should fail\"", false, "lakectl_commit_no_change", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_with_commit", vars)
}
