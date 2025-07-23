package esti

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	testFile1 = "path/to/test1.txt"
	testFile2 = "path/to/test2.txt"
	moveBranch = "move-test"
)

var filesForMoveTest = map[string]string{
	testFile1: "ro_1k",
	testFile2: "ro_1k_other",
}

func TestLakectlBranchMoveToCommit(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)

	// Create repository
	createRepo(t, repoName, storage)

	// Upload files to main branch and create initial commit
	uploadFiles(t, repoName, mainBranch, filesForMoveTest)
	commit(t, repoName, mainBranch, "initial commit with test files")

	// Get the initial commit ID using API
	ctx := context.Background()
	
	logAmount := apigen.PaginationAmount(1)
	logResp, err := client.LogCommitsWithResponse(ctx, repoName, mainBranch, &apigen.LogCommitsParams{
		Amount: &logAmount,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, logResp.StatusCode())
	require.NotEmpty(t, logResp.JSON200.Results)
	
	initialCommitID := logResp.JSON200.Results[0].Id

	// Create additional files and commit on main branch
	additionalFiles := map[string]string{
		"additional/file.txt": "ro_1k",
	}
	uploadFiles(t, repoName, mainBranch, additionalFiles)
	commit(t, repoName, mainBranch, "second commit with additional files")

	// Create a test branch from current main
	createBranch(t, repoName, storage, moveBranch)

	// Add some files to the test branch to create uncommitted changes
	uncommittedFiles := map[string]string{
		"uncommitted/file.txt": "ro_1k",
	}
	uploadFiles(t, repoName, moveBranch, uncommittedFiles)

	// Test 1: Try to move branch without --force flag (should fail due to uncommitted changes)
	moveCmd := NewLakeCtl().
		Arg("branch move-to-commit").
		URLArg("lakefs://", repoName, moveBranch).
		Arg(initialCommitID).
		Flag("--yes") // Auto-confirm
	RunCmdAndVerifyFailureContainsText(t, moveCmd.Get(), false, "has uncommitted changes", nil)

	// Test 2: Move branch with --force flag (should succeed)
	moveCmdForce := NewLakeCtl().
		Arg("branch move-to-commit").
		URLArg("lakefs://", repoName, moveBranch).
		Arg(initialCommitID).
		Flag("--force").
		Flag("--yes") // Auto-confirm
	RunCmdAndVerifyContainsText(t, moveCmdForce.Get(), false, "Successfully moved branch", nil)

	// Test 3: Verify the branch now points to the initial commit
	// Check that the additional files from the second commit are no longer present
	listCmd := NewLakeCtl().
		Arg("fs ls").
		URLArg("lakefs://", repoName, moveBranch, "additional/")
	RunCmdAndVerifyFailureContainsText(t, listCmd.Get(), false, "path not found", nil)

	// Test 4: Verify the original files from initial commit are still there
	statCmd := NewLakeCtl().
		Arg("fs stat").
		URLArg("lakefs://", repoName, moveBranch, testFile1)
	RunCmdAndVerifySuccess(t, statCmd.Get(), false, "", nil)

	// Test 5: Try to move to a non-existent commit (should fail)
	invalidMoveCmd := NewLakeCtl().
		Arg("branch move-to-commit").
		URLArg("lakefs://", repoName, moveBranch).
		Arg("nonexistentcommit1234567890abcdef").
		Flag("--yes")
	RunCmdAndVerifyFailureContainsText(t, invalidMoveCmd.Get(), false, "not found", nil)
}

func TestLakectlBranchMoveToCommitCleanBranch(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)

	// Create repository
	createRepo(t, repoName, storage)

	// Upload files and create initial commit
	uploadFiles(t, repoName, mainBranch, filesForMoveTest)
	commit(t, repoName, mainBranch, "initial commit")

	// Get the initial commit ID using API
	ctx := context.Background()
	
	logAmount := apigen.PaginationAmount(1)
	logResp, err := client.LogCommitsWithResponse(ctx, repoName, mainBranch, &apigen.LogCommitsParams{
		Amount: &logAmount,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, logResp.StatusCode())
	require.NotEmpty(t, logResp.JSON200.Results)
	
	initialCommitID := logResp.JSON200.Results[0].Id

	// Add more files and create second commit
	additionalFiles := map[string]string{
		"second/commit/file.txt": "ro_1k",
	}
	uploadFiles(t, repoName, mainBranch, additionalFiles)
	commit(t, repoName, mainBranch, "second commit")

	// Create branch from current main (which has both commits)
	createBranch(t, repoName, storage, moveBranch)

	// Move the branch to the initial commit (no uncommitted changes)
	moveCmd := NewLakeCtl().
		Arg("branch move-to-commit").
		URLArg("lakefs://", repoName, moveBranch).
		Arg(initialCommitID).
		Flag("--yes")
	RunCmdAndVerifyContainsText(t, moveCmd.Get(), false, "Successfully moved branch", nil)

	// Verify the second commit files are no longer accessible
	listCmd := NewLakeCtl().
		Arg("fs ls").
		URLArg("lakefs://", repoName, moveBranch, "second/")
	RunCmdAndVerifyFailureContainsText(t, listCmd.Get(), false, "path not found", nil)

	// Verify original files are still there
	statCmd := NewLakeCtl().
		Arg("fs stat").
		URLArg("lakefs://", repoName, moveBranch, testFile1)
	RunCmdAndVerifySuccess(t, statCmd.Get(), false, "", nil)
}

func TestLakectlBranchMoveToCommitPermissions(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)

	// Create repository
	createRepo(t, repoName, storage)

	// Upload files and commit
	uploadFiles(t, repoName, mainBranch, filesForMoveTest)
	commit(t, repoName, mainBranch, "test commit")

	// Get commit ID using API
	ctx := context.Background()
	
	logAmount := apigen.PaginationAmount(1)
	logResp, err := client.LogCommitsWithResponse(ctx, repoName, mainBranch, &apigen.LogCommitsParams{
		Amount: &logAmount,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, logResp.StatusCode())
	require.NotEmpty(t, logResp.JSON200.Results)
	
	commitID := logResp.JSON200.Results[0].Id

	// Create branch
	createBranch(t, repoName, storage, moveBranch)

	// Test basic permission validation by trying to move to same commit (should succeed)
	moveCmd := NewLakeCtl().
		Arg("branch move-to-commit").
		URLArg("lakefs://", repoName, moveBranch).
		Arg(commitID).
		Flag("--yes")
	RunCmdAndVerifyContainsText(t, moveCmd.Get(), false, "Successfully moved branch", nil)
}

func TestLakectlBranchMoveToCommitValidation(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)

	// Create repository
	createRepo(t, repoName, storage)

	// Test 1: Invalid branch URI format
	invalidBranchCmd := NewLakeCtl().
		Arg("branch move-to-commit").
		Arg("invalid-uri").
		Arg("somecommit").
		Flag("--yes")
	RunCmdAndVerifyFailureContainsText(t, invalidBranchCmd.Get(), false, "Invalid input", nil)

	// Test 2: Empty commit reference
	emptyCommitCmd := NewLakeCtl().
		Arg("branch move-to-commit").
		URLArg("lakefs://", repoName, mainBranch).
		Arg("").
		Flag("--yes")
	RunCmdAndVerifyFailureContainsText(t, emptyCommitCmd.Get(), false, "Invalid input", nil)

	// Test 3: Non-existent repository
	nonExistentRepoCmd := NewLakeCtl().
		Arg("branch move-to-commit").
		URLArg("lakefs://", "nonexistent-repo", mainBranch).
		Arg("somecommit").
		Flag("--yes")
	RunCmdAndVerifyFailureContainsText(t, nonExistentRepoCmd.Get(), false, "not found", nil)

	// Test 4: Non-existent branch
	nonExistentBranchCmd := NewLakeCtl().
		Arg("branch move-to-commit").
		URLArg("lakefs://", repoName, "nonexistent-branch").
		Arg("somecommit").
		Flag("--yes")
	RunCmdAndVerifyFailureContainsText(t, nonExistentBranchCmd.Get(), false, "not found", nil)
}

