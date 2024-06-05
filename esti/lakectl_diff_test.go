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
	runDiffAndExpect(t, repoName, testBranch, expectedDiff, "")
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
	runDiffAndExpect(t, repoName, testBranch, expectedDiff, "")
}

func TestLakectlDiffPrefix(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)

	createRepo(t, repoName, storage)
	createBranch(t, repoName, storage, testBranch)

	uploadFiles(t, repoName, testBranch, filesForDiffTest)
	commit(t, repoName, testBranch, "adding test files to "+testBranch)

	runDiffAndExpect(t, repoName, testBranch, &ExpectedDiff{
		added:   []string{filePath1, filePath2},
		deleted: []string{},
	}, "path/to/")
	runDiffAndExpect(t, repoName, testBranch, &ExpectedDiff{
		added:   []string{filePath2},
		deleted: []string{},
	}, "path/to/o")
	runDiffAndExpect(t, repoName, testBranch, &ExpectedDiff{
		added:   []string{filePath1},
		deleted: []string{},
	}, "path/to/f")
	runDiffAndExpect(t, repoName, testBranch, &ExpectedDiff{
		added:   []string{},
		deleted: []string{},
	}, "path/to/x")
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

func runDiffAndExpect(t *testing.T, repoName string, testBranch string, diffArgs *ExpectedDiff, prefix string) {
	diffVars := map[string]string{
		"REPO":         repoName,
		"LEFT_BRANCH":  mainBranch,
		"RIGHT_BRANCH": testBranch,
		"DIFF_LIST":    diffArgs.buildAssertionString(),
	}

	cmdArgs := " diff lakefs://" + repoName + "/" + mainBranch + " lakefs://" + repoName + "/" + testBranch
	if prefix != "" {
		cmdArgs += " --prefix " + prefix
	}

	RunCmdAndVerifySuccessWithFile(t, Lakectl()+cmdArgs, false, "lakectl_diff", diffVars)
}

func uploadFiles(t *testing.T, repoName string, branch string, files map[string]string) {
	for filePath, contentPath := range files {
		RunCmdAndVerifyContainsText(t, Lakectl()+" fs upload -s files/"+contentPath+" lakefs://"+repoName+"/"+branch+"/"+filePath, false, filePath, nil)
	}
}

func commit(t *testing.T, repoName string, branch string, commitMessage string) {
	RunCmdAndVerifyContainsText(t, Lakectl()+" commit lakefs://"+repoName+"/"+branch+" -m \""+commitMessage+"\"", false, commitMessage, nil)
}

func deleteFiles(t *testing.T, repoName string, branch string, files ...string) {
	for _, filePath := range files {
		RunCmdAndVerifySuccess(t, Lakectl()+" fs rm lakefs://"+repoName+"/"+branch+"/"+filePath, false, "", nil)
	}
}

func createRepo(t *testing.T, repoName string, storage string) {
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", map[string]string{
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
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch create lakefs://"+repoName+"/"+branch+" --source lakefs://"+repoName+"/"+mainBranch, false, "lakectl_branch_create", branchVars)
}
