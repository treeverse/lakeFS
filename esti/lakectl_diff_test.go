package esti

import (
	"sort"
	"strings"
	"testing"
)

func TestLakectlDiffAddedFiles(t *testing.T) {
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	testBranch := "test"

	createRepo(t, repoName, storage)
	createBranch(t, repoName, storage, testBranch)

	files := map[string]string{
		"path/to/file1.txt":       "ro_1k",
		"path/to/other/file2.txt": "ro_1k_other",
	}
	commitFilesForDiff(t, repoName, testBranch, files)
	expectedDiff := &ExpectedDiff{
		added:   []string{"path/to/file1.txt", "path/to/other/file2.txt"},
		deleted: []string{},
	}
	runDiffAndExpect(t, repoName, testBranch, expectedDiff)
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
		deleted = append(deleted, PrefixedFile{"- deleted ", file})
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

func runDiffAndExpect(t *testing.T, repoName string, testBranch string, diffArgs *ExpectedDiff) {
	diffVars := map[string]string{
		"REPO":         repoName,
		"LEFT_BRANCH":  mainBranch,
		"RIGHT_BRANCH": testBranch,
		"DIFF_LIST":    diffArgs.buildAssertionString(),
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" diff lakefs://"+repoName+"/"+mainBranch+" lakefs://"+repoName+"/test", false, "lakectl_diff", diffVars)
}

func commitFilesForDiff(t *testing.T, repoName string, branch string, files map[string]string) {
	for filePath, contentPath := range files {
		RunCmdAndVerifyContainsText(t, Lakectl()+" fs upload -s files/"+contentPath+" lakefs://"+repoName+"/"+branch+"/"+filePath, false, filePath, nil)
	}

	commitMessage := "committing test files to " + branch
	RunCmdAndVerifyContainsText(t, Lakectl()+" commit lakefs://"+repoName+"/"+branch+" -m \""+commitMessage+"\"", false, commitMessage, nil)
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
