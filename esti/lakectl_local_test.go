package esti

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/fileutil"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
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
	runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+featureBranch+" --source lakefs://"+repoName+"/"+mainBranch, false, false, branchVars)

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

	t.Run("clone object path", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		vars["PATH"] = "lakefs://" + repoName + "/" + mainBranch + "/" + objects[0]
		RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local clone "+vars["PATH"]+" "+dataDir, false, "lakectl_local_init_is_object", vars)
	})

	t.Run("clone existing directory marker", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		vars["PREFIX"] = "dir_marker/"
		vars["LOCAL_DIR"] = dataDir
		_, err = uploadContent(context.Background(), vars["REPO"], vars["BRANCH"], vars["PREFIX"], "")
		require.NoError(t, err)
		runCmd(t, Lakectl()+" commit lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+" --allow-empty-message -m \" \"", false, false, vars)
		RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+repoName+"/"+mainBranch+"/"+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

		relPath, err := filepath.Rel(tmpDir, dataDir)
		require.NoError(t, err)
		vars["LIST_DIR"] = relPath
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list "+tmpDir, false, "lakectl_local_list", vars)
	})

	t.Run("clone existing path", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = prefix + uri.PathSeparator
		RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+repoName+"/"+mainBranch+"/"+prefix+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

		vars["LIST_DIR"] = "."
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list "+dataDir, false, "lakectl_local_list", vars)

		// Expect empty since no linked directories in CWD
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" local list", false, "lakectl_empty", vars)

		expected := localExtractRelativePathsByPrefix(t, prefix, objects)
		localVerifyDirContents(t, dataDir, expected)

		// Try to clone twice
		vars["LOCAL_DIR"] = tmpDir
		RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local clone lakefs://"+repoName+"/"+mainBranch+"/ "+tmpDir, false, "lakectl_local_clone_non_empty", vars)
	})

	t.Run("clone new path", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = "new_prefix"
		RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+repoName+"/"+mainBranch+"/"+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX}/ to ${LOCAL_DIR}.", vars)
		localVerifyDirContents(t, dataDir, []string{})

		// Add new files to path
		localCreateTestData(t, vars, []string{vars["PREFIX"] + "nodiff.txt"})
		require.NoError(t, os.Mkdir(filepath.Join(dataDir, vars["PREFIX"]), fileutil.DefaultDirectoryMask))
		fd, err = os.Create(filepath.Join(dataDir, "test1.txt"))
		require.NoError(t, err)
		require.NoError(t, fd.Close())
		fd, err = os.Create(filepath.Join(dataDir, vars["PREFIX"]+"test2.txt"))
		require.NoError(t, err)
		require.NoError(t, fd.Close())
		sanitizedResult := runCmd(t, Lakectl()+" local status "+dataDir, false, false, vars)
		require.Contains(t, sanitizedResult, "test1.txt")
		require.Contains(t, sanitizedResult, vars["PREFIX"]+"test2.txt")
		require.NotContains(t, sanitizedResult, vars["PREFIX"]+"nodiff.txt")
	})

	t.Run("diff with posix permissions", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = "posix"

		lakectl := LakectlWithPosixPerms()
		RunCmdAndVerifyContainsText(t, lakectl+" local clone lakefs://"+repoName+"/"+mainBranch+"/"+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX}/ to ${LOCAL_DIR}.", vars)
		localVerifyDirContents(t, dataDir, []string{})

		// Add new files to path
		localCreateTestData(t, vars, []string{
			vars["PREFIX"] + uri.PathSeparator + "with-diff.txt",
			vars["PREFIX"] + uri.PathSeparator + "no-diff.txt",
		})

		res := runCmd(t, lakectl+" local pull "+dataDir, false, false, vars)
		require.Contains(t, res, "download with-diff.txt")
		require.Contains(t, res, "download no-diff.txt")

		commitMessage := "'initialize' posix permissions for the remote repo"
		runCmd(t, lakectl+" local commit "+dataDir+" -m \""+commitMessage+"\"", false, false, vars)

		sanitizedResult := runCmd(t, lakectl+" local status "+dataDir, false, false, vars)
		require.Contains(t, sanitizedResult, "No diff found")

		err = os.Chmod(filepath.Join(dataDir, "with-diff.txt"), 0755)
		require.NoError(t, err)

		sanitizedResult = runCmd(t, lakectl+" local status "+dataDir, false, false, vars)

		require.Contains(t, sanitizedResult, "with-diff.txt")
		require.NotContains(t, sanitizedResult, "no-diff.txt")
	})
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

			vars["PREFIX"] = tt.prefix
			if len(tt.prefix) > 0 {
				vars["PREFIX"] = path.Join(uri.PathSeparator, tt.prefix)
			}
			vars["LOCAL_DIR"] = dataDir
			vars["BRANCH"] = tt.name
			vars["REF"] = tt.name
			runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+vars["BRANCH"]+" --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)
			RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+tt.prefix+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}${PREFIX}/ to ${LOCAL_DIR}.", vars)

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

func TestLakectlLocal_commit(t *testing.T) {
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
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
	vars["LOCAL_DIR"] = tmpDir
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local commit -m test "+tmpDir, false, "lakectl_local_no_index", vars)

	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

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

	tests := []struct {
		name    string
		prefix  string
		presign bool
	}{
		{
			name:    "root",
			prefix:  "",
			presign: false,
		},
		{
			name:    "root-presign",
			prefix:  "",
			presign: true,
		},
		{
			name:    prefix,
			prefix:  prefix,
			presign: false,
		},
		{
			name:    prefix + "-presign",
			prefix:  prefix,
			presign: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presign {
				// Skip due to bug on Azure https://github.com/treeverse/lakeFS/issues/6426
				requireBlockstoreType(t, block.BlockstoreTypeS3, block.BlockstoreTypeGS)
			}
			dataDir, err := os.MkdirTemp(tmpDir, "")
			require.NoError(t, err)
			deleted := prefix + "/subdir/deleted.png"

			localCreateTestData(t, vars, append(objects, deleted))

			runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+tt.name+" --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

			vars["LOCAL_DIR"] = dataDir
			vars["PREFIX"] = ""
			vars["BRANCH"] = tt.name
			vars["REF"] = tt.name
			presign := fmt.Sprintf(" --pre-sign=%v ", tt.presign)
			RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+presign+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

			RunCmdAndVerifyContainsText(t, Lakectl()+" local status "+dataDir, false, "No diff found", vars)

			// Modify local folder - add and remove files
			os.MkdirAll(filepath.Join(dataDir, "subdir"), os.ModePerm)
			os.MkdirAll(filepath.Join(dataDir, "subdir-a"), os.ModePerm)
			fd, err = os.Create(filepath.Join(dataDir, "subdir", "test.txt"))
			require.NoError(t, err)
			fd, err = os.Create(filepath.Join(dataDir, "subdir-a", "test.txt"))
			require.NoError(t, err)
			fd, err = os.Create(filepath.Join(dataDir, "test.txt"))
			require.NoError(t, err)
			require.NoError(t, fd.Close())
			require.NoError(t, os.Remove(filepath.Join(dataDir, deleted)))

			RunCmdAndVerifyContainsText(t, Lakectl()+" local status "+dataDir, false, "local  ║ added   ║ test.txt", vars)
			RunCmdAndVerifyContainsText(t, Lakectl()+" local status "+dataDir, false, "local  ║ added   ║ subdir/test.txt", vars)
			RunCmdAndVerifyContainsText(t, Lakectl()+" local status "+dataDir, false, "local  ║ added   ║ subdir-a/test.txt", vars)

			// Commit changes to branch
			RunCmdAndVerifyContainsText(t, Lakectl()+" local commit -m test"+presign+dataDir, false, "Commit for branch \"${BRANCH}\" completed", vars)

			// Check no diff after commit
			RunCmdAndVerifyContainsText(t, Lakectl()+" local status "+dataDir, false, "No diff found", vars)
		})
	}
}

func TestLakectlLocal_commit_remote_uncommitted(t *testing.T) {
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"REF":     mainBranch,
		"PREFIX":  "test-data",
	}

	runCmd(t, fmt.Sprintf("%s repo create lakefs://%s %s", Lakectl(), repoName, storage), false, false, vars)

	testCases := []struct {
		name              string
		uncommittedRemote []string
		uncommittedLocal  []string
		expectFailure     bool
		expectedMessage   string
		withForceFlag     bool
	}{
		{
			name:              "uncommitted_changes_-_none",
			uncommittedRemote: []string{},
			uncommittedLocal: []string{
				"test.data",
			},
			expectFailure:   false,
			expectedMessage: "Commit for branch \"${BRANCH}\" completed",
			withForceFlag:   false,
		},
		{
			name: "uncommitted_changes_-_outside",
			uncommittedRemote: []string{
				"otherPrefix/a",
			},
			uncommittedLocal: []string{
				"test.data",
			},
			expectFailure:   true,
			expectedMessage: "Branch ${BRANCH} contains uncommitted changes outside of local path '${LOCAL_DIR}'.\nTo proceed, use the --force flag.",
			withForceFlag:   false,
		},
		{
			name: "uncommitted_changes_-_inside",
			uncommittedRemote: []string{
				fmt.Sprintf("%s/a", vars["PREFIX"]),
			},
			uncommittedLocal: []string{
				"test.data",
			},
			expectFailure:   false,
			expectedMessage: "Commit for branch \"${BRANCH}\" completed",
			withForceFlag:   false,
		},
		{
			name: "uncommitted_changes_-_inside_before_outside",
			uncommittedRemote: []string{
				"zzz/a",
			},
			uncommittedLocal: []string{
				"test.data",
			},
			expectFailure:   true,
			expectedMessage: "Branch ${BRANCH} contains uncommitted changes outside of local path '${LOCAL_DIR}'.\nTo proceed, use the --force flag.",
			withForceFlag:   false,
		},
		{
			name: "uncommitted_changes_-_on_boundry",
			uncommittedRemote: []string{
				fmt.Sprintf("%s0", vars["PREFIX"]),
			},
			uncommittedLocal: []string{
				"test.data",
			},
			expectFailure:   true,
			expectedMessage: "Branch ${BRANCH} contains uncommitted changes outside of local path '${LOCAL_DIR}'.\nTo proceed, use the --force flag.",
			withForceFlag:   false,
		},
		{
			name: "uncommitted_changes_-_outside_force",
			uncommittedRemote: []string{
				"otherPrefix/a",
			},
			uncommittedLocal: []string{
				"test.data",
			},
			expectFailure:   false,
			expectedMessage: "Commit for branch \"${BRANCH}\" completed",
			withForceFlag:   true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir, err := os.MkdirTemp(tmpDir, "")
			require.NoError(t, err)

			runCmd(t, fmt.Sprintf("%s branch create lakefs://%s/%s --source lakefs://%s/%s", Lakectl(), repoName, tc.name, repoName, mainBranch), false, false, vars)
			vars["LOCAL_DIR"] = dataDir
			vars["BRANCH"] = tc.name
			vars["REF"] = tc.name
			RunCmdAndVerifyContainsText(t, fmt.Sprintf("%s local clone lakefs://%s/%s/%s %s", Lakectl(), repoName, vars["BRANCH"], vars["PREFIX"], dataDir), false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX}/ to ${LOCAL_DIR}.", vars)

			// add remote files
			if len(tc.uncommittedRemote) > 0 {
				for _, f := range tc.uncommittedRemote {
					vars["FILE_PATH"] = f
					runCmd(t, fmt.Sprintf("%s fs upload -s files/ro_1k lakefs://%s/%s/%s", Lakectl(), vars["REPO"], vars["BRANCH"], vars["FILE_PATH"]), false, false, vars)

				}
			}

			// add local files
			for _, f := range tc.uncommittedLocal {
				fd, err = os.Create(filepath.Join(dataDir, f))
				require.NoError(t, err)
				require.NoError(t, fd.Close())
			}

			force := ""
			if tc.withForceFlag {
				force = "--force"
			}
			cmd := fmt.Sprintf("%s local commit %s -m test %s", Lakectl(), force, dataDir)
			if tc.expectFailure {
				RunCmdAndVerifyFailureContainsText(t, cmd, false, tc.expectedMessage, vars)
			} else {
				RunCmdAndVerifyContainsText(t, cmd, false, tc.expectedMessage, vars)
			}
		})
	}
}

func TestLakectlLocal_interrupted(t *testing.T) {
	tmpDir := t.TempDir()
	repoName := generateUniqueRepositoryName()
	storage := generateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"REF":     mainBranch,
		"PREFIX":  "",
	}

	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

	tests := []struct {
		action          string
		expectedmessage string
	}{
		{
			action: "clone",
			expectedmessage: `Latest clone operation was interrupted, local data may be incomplete.
Use "lakectl local checkout..." to sync with the remote or run "lakectl local clone..." with a different directory to sync with the remote.`,
		},
		{
			action: "checkout",
			expectedmessage: `Latest checkout operation was interrupted, local data may be incomplete.
Use "lakectl local checkout..." to sync with the remote.`,
		},
		{
			action: "commit",
			expectedmessage: `Latest commit operation was interrupted, data may be incomplete.
Use "lakectl local commit..." to commit your latest changes or "lakectl local pull... --force" to sync with the remote.`,
		},
		{
			action: "pull",
			expectedmessage: `Latest pull operation was interrupted, local data may be incomplete.
Use "lakectl local pull... --force" to sync with the remote.`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			dataDir, err := os.MkdirTemp(tmpDir, "")
			require.NoError(t, err)

			runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+tt.action+" --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

			vars["LOCAL_DIR"] = dataDir
			vars["PREFIX"] = ""
			vars["BRANCH"] = tt.action
			vars["REF"] = tt.action
			RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" --pre-sign=false "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

			idx, err := local.ReadIndex(dataDir)
			require.NoError(t, err)
			u, err := idx.GetCurrentURI()
			require.NoError(t, err)
			_, err = local.WriteIndex(idx.LocalPath(), u, idx.AtHead, tt.action)
			require.NoError(t, err)

			// Pull without force flag
			sanitizedResult := runCmd(t, Lakectl()+" local pull "+dataDir, true, false, vars)
			require.Contains(t, sanitizedResult, tt.expectedmessage)
		})
	}
}
