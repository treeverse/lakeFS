package esti

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/local"
	"golang.org/x/exp/slices"
)

func localCreateTestData(t *testing.T, vars map[string]string, objects []string) {
	for _, o := range objects {
		vars["FILE_PATH"] = o
		runCmd(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+"/"+vars["FILE_PATH"], false, false, vars)
	}
	runCmd(t, Lakectl()+" commit lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+" --allow-empty-message -m \" \"", false, false, vars)
}

func localListDir(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			rel, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			files = append(files, rel)
		}
		return nil
	})
	require.NoError(t, err)
	return files
}

func localVerifyDirContents(t *testing.T, dir string, expected []string) {
	t.Helper()
	all := append(expected, local.IndexFileName)
	files := localListDir(t, dir)
	require.ElementsMatch(t, all, files)
}

func localExtractRelativePathsByPrefix(t *testing.T, prefix string, objs []string) (expected []string) {
	t.Helper()
	for _, s := range objs {
		if strings.HasPrefix(s, prefix) {
			rel, err := filepath.Rel(prefix, s)
			require.NoError(t, err)
			expected = append(expected, rel)
		}
	}
	return
}

func localGetSummary(tasks local.Tasks) string {
	result := "Summary:\n\n"
	if tasks.Downloaded+tasks.Uploaded+tasks.Removed == 0 {
		result += "No changes\n"
	} else {
		result += fmt.Sprintf("Downloaded: %d\n", tasks.Downloaded)
		result += fmt.Sprintf("Uploaded: %d\n", tasks.Uploaded)
		result += fmt.Sprintf("Removed: %d\n", tasks.Removed)
	}

	return result
}

func TestLakectlLocal_init(t *testing.T) {
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	dataDir, err := os.MkdirTemp(tmpDir, "")
	require.NoError(t, err)
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"REF":     mainBranch,
		"PREFIX":  "",
	}

	// No repo
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local init lakefs://"+repoName+"/"+mainBranch+"/ "+tmpDir, false, "lakectl_local_repo_not_found", vars)

	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

	// Bad ref
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local init lakefs://"+repoName+"/bad_ref/"+" "+tmpDir, false, "lakectl_local_commit_not_found", vars)

	filePath := "ro_1k.1"
	vars["FILE_PATH"] = filePath
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+mainBranch+"/"+filePath, false, "lakectl_fs_upload", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_initial", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+repoName+"/"+mainBranch+" --allow-empty-message -m \" \"", false, "lakectl_commit_with_empty_msg_flag", vars)

	vars["LOCAL_DIR"] = dataDir
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local init lakefs://"+repoName+"/"+mainBranch+"/ "+dataDir, false, "lakectl_local_init", vars)

	relPath, err := filepath.Rel(tmpDir, dataDir)
	require.NoError(t, err)
	vars["LIST_DIR"] = relPath
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list "+tmpDir, false, "lakectl_local_list", vars)
	vars["LIST_DIR"] = "."
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list "+dataDir, false, "lakectl_local_list", vars)

	// Expect empty since no linked directories in CWD
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list", false, "lakectl_empty", vars)

	// Verify directory is empty
	localVerifyDirContents(t, dataDir, []string{})

	// init in a directory with files
	vars["LOCAL_DIR"] = tmpDir
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local init lakefs://"+repoName+"/"+mainBranch+"/ "+tmpDir, false, "lakectl_local_init", vars)
	vars["LIST_DIR"] = "."
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list "+tmpDir, false, "lakectl_local_list", vars)

	// Try to init twice
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local init lakefs://"+repoName+"/"+mainBranch+"/ "+tmpDir, false, "lakectl_local_init_twice", vars)

	featureBranch := "feature"
	branchVars := map[string]string{
		"REPO":          repoName,
		"STORAGE":       storage,
		"SOURCE_BRANCH": mainBranch,
		"DEST_BRANCH":   featureBranch,
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" branch create lakefs://"+repoName+"/"+featureBranch+" --source lakefs://"+repoName+"/"+mainBranch, false, "lakectl_branch_create", branchVars)

	// Try to init twice with force
	vars["REF"] = featureBranch
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local init lakefs://"+repoName+"/"+featureBranch+"/ "+tmpDir+" --force", false, "lakectl_local_init", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list "+tmpDir, false, "lakectl_local_list", vars)
}

func TestLakectlLocal_clone(t *testing.T) {
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	dataDir, err := os.MkdirTemp(tmpDir, "")
	require.NoError(t, err)
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"REF":     mainBranch,
	}

	// No repo
	vars["LOCAL_DIR"] = tmpDir
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local clone lakefs://"+repoName+"/"+mainBranch+"/ "+tmpDir, false, "lakectl_local_clone_non_empty", vars)

	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

	// Bad ref
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local init lakefs://"+repoName+"/bad_ref/ "+tmpDir, false, "lakectl_local_commit_not_found", vars)

	prefix := "images"
	objects := []string{
		"ro_1k.1",
		"ro_1k.2",
		"ro_1k.3",
		prefix + "/1.png",
		prefix + "/2.png",
		prefix + "/3.png",
		prefix + "/subdir/1.png",
		prefix + "/subdir/2.png",
		prefix + "/subdir/3.png",
	}

	localCreateTestData(t, vars, objects)

	vars["LOCAL_DIR"] = dataDir
	vars["PREFIX"] = "images"
	RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+repoName+"/"+mainBranch+"/"+prefix+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

	relPath, err := filepath.Rel(tmpDir, dataDir)
	require.NoError(t, err)
	vars["LIST_DIR"] = relPath
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list "+tmpDir, false, "lakectl_local_list", vars)
	vars["LIST_DIR"] = "."
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list "+dataDir, false, "lakectl_local_list", vars)

	// Expect empty since no linked directories in CWD
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list", false, "lakectl_empty", vars)

	expected := localExtractRelativePathsByPrefix(t, prefix, objects)
	localVerifyDirContents(t, dataDir, expected)

	// Try to clone twice
	vars["LOCAL_DIR"] = tmpDir
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local clone lakefs://"+repoName+"/"+mainBranch+"/ "+tmpDir, false, "lakectl_local_clone_non_empty", vars)
}

func TestLakectlLocal_pull(t *testing.T) {
	const successStr = "Successfully synced changes!\n\nPull "

	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"LOCAL_DIR": tmpDir,
		"REPO":      repoName,
		"STORAGE":   storage,
		"BRANCH":    mainBranch,
	}

	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

	// Not linked
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local pull "+tmpDir, false, "lakectl_local_no_index", vars)

	tests := []struct {
		name   string
		prefix string
	}{
		{
			name:   "root",
			prefix: "",
		},
		{
			name:   "prefix",
			prefix: "images",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir, err := os.MkdirTemp(tmpDir, "")
			require.NoError(t, err)
			vars["PREFIX"] = "/" + tt.prefix
			vars["LOCAL_DIR"] = dataDir
			vars["BRANCH"] = tt.name
			vars["REF"] = tt.name
			runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+vars["BRANCH"]+" --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)
			RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}${PREFIX} to ${LOCAL_DIR}.", vars)

			// Pull nothing
			expectedStr := successStr + localGetSummary(local.Tasks{})
			RunCmdAndVerifyContainsText(t, Lakectl()+" local pull "+dataDir, false, expectedStr, vars)

			// Upload and commit an object
			base := []string{
				"ro_1k.1",
				"ro_1k.2",
				"ro_1k.3",
				"images/1.png",
				"images/2.png",
				"images/3.png",
			}

			modified := []string{"ro_1k.1"}
			deleted := "deleted"
			create := append(base, deleted)
			localCreateTestData(t, vars, create)
			expected := localExtractRelativePathsByPrefix(t, tt.prefix, create)
			// Pull changes and verify data
			tasks := local.Tasks{
				Downloaded: uint64(len(expected)),
			}
			expectedStr = successStr + localGetSummary(tasks)
			RunCmdAndVerifyContainsText(t, Lakectl()+" local pull "+dataDir, false, expectedStr, vars)
			localVerifyDirContents(t, dataDir, expected)

			// Modify data
			vars["FILE_PATH"] = modified[0]
			runCmd(t, Lakectl()+" fs upload -s files/ro_1k_other lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+"/"+vars["FILE_PATH"], false, false, vars)
			runCmd(t, Lakectl()+" fs rm lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+deleted, false, false, vars)
			runCmd(t, Lakectl()+" commit lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+" --allow-empty-message -m \" \"", false, false, vars)
			expected = slices.DeleteFunc(expected, func(s string) bool {
				return s == deleted
			})

			// Pull changes and verify data
			tasks = local.Tasks{
				Downloaded: uint64(len(localExtractRelativePathsByPrefix(t, tt.prefix, modified))),
				Removed:    uint64(len(localExtractRelativePathsByPrefix(t, tt.prefix, []string{deleted}))),
			}
			expectedStr = successStr + localGetSummary(tasks)
			RunCmdAndVerifyContainsText(t, Lakectl()+" local pull "+dataDir, false, expectedStr, vars)
			localVerifyDirContents(t, dataDir, expected)
		})
	}
}
