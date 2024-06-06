package esti

import (
	"sort"
	"strings"
	"testing"
)

const (
	filePath1  = "path/to/file1.txt"
	filePath2  = "path/to/other/file2.txt"
	testBranch = "test"
)

var filesForDiffTest = map[string]string{
	filePath1: "ro_1k",
	filePath2: "ro_1k_other",
}

func TestLakectlDiffAddedFiles(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)

	createRepo(t, repoName, storage)
	createBranch(t, repoName, storage, testBranch)

	uploadFiles(t, repoName, testBranch, filesForDiffTest)
	commit(t, repoName, testBranch, "adding test files to "+testBranch)

	expectedDiff := &ExpectedDiff{
		added:   []string{filePath1, filePath2},
		deleted: []string{},
	}
	runDiffAndExpect(t, repoName, testBranch, "", expectedDiff)
}

func TestLakectlDiffDeletedFiles(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)

	createRepo(t, repoName, storage)
	uploadFiles(t, repoName, mainBranch, filesForDiffTest)
	commit(t, repoName, mainBranch, "adding test files to "+mainBranch)

	createBranch(t, repoName, storage, testBranch)
	deleteFiles(t, repoName, testBranch, filePath1, filePath2)
	commit(t, repoName, testBranch, "deleting test files from "+testBranch)

	expectedDiff := &ExpectedDiff{
		added:   []string{},
		deleted: []string{filePath1, filePath2},
	}
	runDiffAndExpect(t, repoName, testBranch, "", expectedDiff)
}

func TestLakectlDiffPrefix(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)

	createRepo(t, repoName, storage)
	createBranch(t, repoName, storage, testBranch)

	uploadFiles(t, repoName, testBranch, filesForDiffTest)
	commit(t, repoName, testBranch, "adding test files to "+testBranch)

	runDiffAndExpect(t, repoName, testBranch, "path/to/", &ExpectedDiff{
		added:   []string{filePath1, filePath2},
		deleted: []string{},
	})
	runDiffAndExpect(t, repoName, testBranch, "path/to/o", &ExpectedDiff{
		added:   []string{filePath2},
		deleted: []string{},
	})
	runDiffAndExpect(t, repoName, testBranch, "path/to/f", &ExpectedDiff{
		added:   []string{filePath1},
		deleted: []string{},
	})
	runDiffAndExpect(t, repoName, testBranch, "path/to/x", &ExpectedDiff{
		added:   []string{},
		deleted: []string{},
	})
}

type ExpectedDiff struct {
	added   []string
	deleted []string
}

func (a ExpectedDiff) buildAssertionString() string {
	type PrefixedFile struct {
		prefix string
		path   string
	}
	var added []PrefixedFile
	var deleted []PrefixedFile
	for _, file := range a.added {
		added = append(added, PrefixedFile{"+ added ", file})
	}
	for _, file := range a.deleted {
		deleted = append(deleted, PrefixedFile{"- removed ", file})
	}
	var all = append(added, deleted...)
	sort.Slice(all, func(i, j int) bool {
		return all[i].path < all[j].path
	})

	var sb strings.Builder
	for _, file := range all {
		sb.WriteString(file.prefix)
		sb.WriteString(file.path)
		sb.WriteString("\n")
	}
	return sb.String()
}

func runDiffAndExpect(t *testing.T, repoName string, testBranch string, prefix string, diffArgs *ExpectedDiff) {
	diffVars := map[string]string{
		"REPO":         repoName,
		"LEFT_BRANCH":  mainBranch,
		"RIGHT_BRANCH": testBranch,
		"DIFF_LIST":    diffArgs.buildAssertionString(),
	}

	cmd := NewLakeCtl().
		Arg("diff").
		URLArg("lakefs://", repoName, mainBranch).
		URLArg("lakefs://", repoName, testBranch)
	if prefix != "" {
		cmd.Flag("--prefix").Arg(prefix)
	}

	RunCmdAndVerifySuccessWithFile(t, cmd.Get(), false, "lakectl_diff", diffVars)
}

func uploadFiles(t *testing.T, repoName string, branch string, files map[string]string) {
	for filePath, contentPath := range files {
		cmd := NewLakeCtl().
			Arg("fs upload").
			Flag("-s").
			PathArg("files", contentPath).
			URLArg("lakefs://", repoName, branch, filePath)
		RunCmdAndVerifyContainsText(t, cmd.Get(), false, filePath, nil)
	}
}

func commit(t *testing.T, repoName string, branch string, commitMessage string) {
	cmd := NewLakeCtl().
		Arg("commit").
		URLArg("lakefs://", repoName, branch).
		Flag("-m").
		Arg("\"" + commitMessage + "\"")
	RunCmdAndVerifyContainsText(t, cmd.Get(), false, commitMessage, nil)
}

func deleteFiles(t *testing.T, repoName string, branch string, files ...string) {
	for _, filePath := range files {
		cmd := NewLakeCtl().
			Arg("fs rm").
			URLArg("lakefs://", repoName, branch, filePath)
		RunCmdAndVerifySuccess(t, cmd.Get(), false, "", nil)
	}
}

func createRepo(t *testing.T, repoName string, storage string) {
	cmd := NewLakeCtl().
		Arg("repo create").
		URLArg("lakefs://", repoName).
		Arg(storage)
	RunCmdAndVerifySuccessWithFile(t, cmd.Get(), false, "lakectl_repo_create", map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
	})
}

func createBranch(t *testing.T, repoName string, storage string, branch string) {
	branchVars := map[string]string{
		"REPO":          repoName,
		"STORAGE":       storage,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   branch,
	}
	cmd := NewLakeCtl().
		Arg("branch create").
		URLArg("lakefs://", repoName, branch).
		Flag("--source").
		URLArg("lakefs://", repoName, mainBranch)
	RunCmdAndVerifySuccessWithFile(t, cmd.Get(), false, "lakectl_branch_create", branchVars)
}
