package esti

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/local"
)

func createTestData(t *testing.T, vars map[string]string, objects []string) {
	for _, o := range objects {
		vars["FILE_PATH"] = o
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+vars["REPO"]+"/"+vars["REF"]+"/"+vars["FILE_PATH"], false, "lakectl_fs_upload", vars)
	}
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" commit lakefs://"+vars["REPO"]+"/"+vars["REF"]+" --allow-empty-message -m \" \"", false, "lakectl_commit_with_empty_msg_flag", vars)
}

func TestLakectlLocal_clone(t *testing.T) {
	tmpDir := t.TempDir()
	_, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
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
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local clone lakefs://"+repoName+"/"+mainBranch+" "+tmpDir, false, "lakectl_local_clone_non_empty", vars)

	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_404", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, "lakectl_repo_create", vars)
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, "lakectl_log_initial", vars)

	// Bad ref
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local init lakefs://"+repoName+"/bad_ref"+" "+tmpDir, false, "lakectl_local_commit_not_found", vars)

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
	expectedObjStartIdx := 3

	createTestData(t, vars, objects)

	vars["LOCAL_DIR"] = dataDir
	vars["PREFIX"] = "/images"
	RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+repoName+"/"+mainBranch+"/"+prefix+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}${PREFIX} to ${LOCAL_DIR}.", vars)

	relPath, err := filepath.Rel(tmpDir, dataDir)
	require.NoError(t, err)
	vars["LIST_DIR"] = relPath
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list "+tmpDir, false, "lakectl_local_list", vars)
	vars["LIST_DIR"] = "."
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list "+dataDir, false, "lakectl_local_list", vars)

	// Expect empty since no linked directories in CWD
	RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list", false, "lakectl_empty", vars)

	// Verify directory is empty
	indexFound := false
	count := 0
	err = filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		switch {
		case d.Name() == local.IndexFileName:
			indexFound = true

		case !d.IsDir():
			rel, err := filepath.Rel(dataDir, path)
			require.NoError(t, err)
			require.Contains(t, objects[expectedObjStartIdx:], filepath.Join(prefix, rel), "unexpected file in data dir %s", path)
			count += 1
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, indexFound, "No index file found in data dir", dataDir)

	require.Equal(t, len(objects)-expectedObjStartIdx, count)

	// Try to clone twice
	vars["LOCAL_DIR"] = tmpDir
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local clone lakefs://"+repoName+"/"+mainBranch+" "+tmpDir, false, "lakectl_local_clone_non_empty", vars)
}
