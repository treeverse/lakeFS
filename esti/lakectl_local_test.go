package esti

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/fileutil"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
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
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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
	RunCmdAndVerifyFailureWithFile(t, Lakectl()+" local init lakefs://"+repoName+"/bad_ref/ "+tmpDir, false, "lakectl_local_commit_not_found", vars)

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
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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
		_, err = UploadContent(context.Background(), vars["REPO"], vars["BRANCH"], vars["PREFIX"], "", nil)
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
}

func TestLakectlLocal_posix_permissions(t *testing.T) {
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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

	t.Run("diff with posix permissions", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = "posix-diff"

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

		err = os.Chmod(filepath.Join(dataDir, "with-diff.txt"), 0o755)
		require.NoError(t, err)

		sanitizedResult = runCmd(t, lakectl+" local status "+dataDir, false, false, vars)

		require.Contains(t, sanitizedResult, "with-diff.txt")
		require.NotContains(t, sanitizedResult, "no-diff.txt")
	})

	t.Run("sync folders deletion", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = "posix-folder-deletion"

		lakectl := LakectlWithPosixPerms()
		RunCmdAndVerifyContainsText(t, lakectl+" local clone lakefs://"+repoName+"/"+mainBranch+"/"+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX}/ to ${LOCAL_DIR}.", vars)
		localVerifyDirContents(t, dataDir, []string{})

		// upload a new empty folder
		emptyDirName := "empty_local_folder"
		localDirPath := filepath.Join(dataDir, "empty_local_folder")
		err = os.Mkdir(localDirPath, fileutil.DefaultDirectoryMask)
		require.NoError(t, err)
		commitMessage := "add empty folder"
		res := runCmd(t, lakectl+" local commit "+dataDir+" -m \""+commitMessage+"\"", false, false, vars)
		require.Contains(t, res, fmt.Sprintf("upload %s", emptyDirName))

		// remove the empty folder locally, and validate it's removed from the remote repo
		err = os.Remove(localDirPath)
		require.NoError(t, err)
		commitMessage = "remove empty folder"
		res = runCmd(t, lakectl+" local commit "+dataDir+" -m \""+commitMessage+"\"", false, false, vars)
		require.Contains(t, res, fmt.Sprintf("delete remote path: %s/", emptyDirName))

		res = runCmd(t, lakectl+" local status "+dataDir, false, false, vars)
		require.Contains(t, res, "No diff found")
		require.NotContains(t, res, emptyDirName)
	})

	t.Run("existing posix path", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = "existing_path"
		vars["FILE_PATH"] = vars["PREFIX"]
		lakectl := LakectlWithPosixPerms()

		RunCmdAndVerifyContainsText(t, lakectl+" local clone lakefs://"+repoName+"/"+mainBranch+"/"+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX}/ to ${LOCAL_DIR}.", vars)
		localVerifyDirContents(t, dataDir, []string{})

		// Add new files to path
		contents := []string{
			vars["PREFIX"] + uri.PathSeparator + "with-diff.txt",
			vars["PREFIX"] + uri.PathSeparator + "subdir1" + uri.PathSeparator + "no-diff.txt",
		}
		localCreateTestData(t, vars, contents)

		// upload a new empty folder
		emptyDirName := "empty_dir"
		emptyDirPath := filepath.Join(dataDir, emptyDirName)
		err = os.Mkdir(emptyDirPath, fileutil.DefaultDirectoryMask)
		require.NoError(t, err)

		commitMessage := "sync local"
		res := runCmd(t, lakectl+" local commit "+dataDir+" -m \""+commitMessage+"\"", false, false, vars)
		require.Contains(t, res, fmt.Sprintf("upload %s", emptyDirName))

		// clone path to a new local dir
		dataDir2, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		vars["LOCAL_DIR"] = dataDir2
		RunCmdAndVerifyContainsText(t, lakectl+" local clone lakefs://"+repoName+"/"+mainBranch+"/"+vars["PREFIX"]+" "+dataDir2, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX}/ to ${LOCAL_DIR}.", vars)
		for _, f := range append(contents, "empty_dir") {
			p := filepath.Join(dataDir2, strings.TrimPrefix(f, vars["PREFIX"]))
			_, err = os.Stat(p)
			require.NoError(t, err)
		}
	})
}

func TestLakectlLocal_pull(t *testing.T) {
	const successStr = "Successfully synced changes!\n\nPull "

	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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

func TestLakectlLocal_commitProtectedBranch(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	dataDir, err := os.MkdirTemp(tmpDir, "")
	require.NoError(t, err)
	file := "test.txt"

	vars := map[string]string{
		"REPO":      repoName,
		"STORAGE":   storage,
		"BRANCH":    mainBranch,
		"REF":       mainBranch,
		"LOCAL_DIR": dataDir,
		"FILE":      file,
	}
	runCmd(t, Lakectl()+" repo create lakefs://"+vars["REPO"]+" "+vars["STORAGE"], false, false, vars)
	runCmd(t, Lakectl()+" branch-protect add lakefs://"+vars["REPO"]+"/  '*'", false, false, vars)
	// BranchUpdateMaxInterval - sleep in order to overcome branch update caching
	time.Sleep(branchProtectTimeout)
	// Cloning local dir
	RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+"/ "+vars["LOCAL_DIR"], false, "Successfully cloned lakefs://${REPO}/${REF}/ to ${LOCAL_DIR}.", vars)
	RunCmdAndVerifyContainsText(t, Lakectl()+" local status "+vars["LOCAL_DIR"], false, "No diff found", vars)
	// Adding file to local dir
	fd, err = os.Create(filepath.Join(vars["LOCAL_DIR"], vars["FILE"]))
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	RunCmdAndVerifyContainsText(t, Lakectl()+" local status "+vars["LOCAL_DIR"], false, "local  ║ added  ║ test.txt", vars)
	// Try to commit local dir, expect failure
	RunCmdAndVerifyFailureContainsText(t, Lakectl()+" local commit -m test "+vars["LOCAL_DIR"], false, "cannot write to protected branch", vars)
}

func TestLakectlLocal_RmCommitProtectedBranch(t *testing.T) {
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	dataDir, err := os.MkdirTemp(tmpDir, "")
	require.NoError(t, err)
	file := "ro_1k.0"

	vars := map[string]string{
		"REPO":      repoName,
		"STORAGE":   storage,
		"BRANCH":    mainBranch,
		"REF":       mainBranch,
		"LOCAL_DIR": dataDir,
		"FILE_PATH": file,
	}
	runCmd(t, Lakectl()+" repo create lakefs://"+vars["REPO"]+" "+vars["STORAGE"], false, false, vars)

	// Cloning local dir
	RunCmdAndVerifyContainsText(t, Lakectl()+" local clone lakefs://"+vars["REPO"]+"/"+vars["BRANCH"]+"/ "+vars["LOCAL_DIR"], false, "Successfully cloned lakefs://${REPO}/${REF}/ to ${LOCAL_DIR}.", vars)
	RunCmdAndVerifyContainsText(t, Lakectl()+" local status "+vars["LOCAL_DIR"], false, "No diff found", vars)

	// locally add a file and commit
	fd, err = os.Create(filepath.Join(vars["LOCAL_DIR"], vars["FILE_PATH"]))
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	RunCmdAndVerifyContainsText(t, Lakectl()+" local commit "+vars["LOCAL_DIR"]+" -m test", false, "Commit for branch \""+vars["BRANCH"]+"\" completed.", vars)
	runCmd(t, Lakectl()+" branch-protect add lakefs://"+vars["REPO"]+"/  '*'", false, false, vars)
	// BranchUpdateMaxInterval - sleep in order to overcome branch update caching
	time.Sleep(branchProtectTimeout)
	// Try delete file from local dir and then commit
	require.NoError(t, os.Remove(filepath.Join(vars["LOCAL_DIR"], vars["FILE_PATH"])))
	RunCmdAndVerifyContainsText(t, Lakectl()+" local status "+vars["LOCAL_DIR"], false, "local  ║ removed ║ "+vars["FILE_PATH"], vars)
	RunCmdAndVerifyFailureContainsText(t, Lakectl()+" local commit -m test "+vars["LOCAL_DIR"], false, "cannot write to protected branch", vars)
}

func TestLakectlLocal_commit(t *testing.T) {
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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
				RequireBlockstoreType(t, block.BlockstoreTypeS3, block.BlockstoreTypeGS)
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
			require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "subdir"), os.ModePerm))
			require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "subdir-a"), os.ModePerm))
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

func TestLakectlLocal_commit_symlink(t *testing.T) {
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"REF":     mainBranch,
		"PREFIX":  "",
	}

	// No repo
	vars["LOCAL_DIR"] = tmpDir
	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

	tests := []struct {
		name        string
		skipSymlink bool
	}{
		{
			name:        "skip-symlink",
			skipSymlink: true,
		},
		{
			name:        "fail-on-symlink",
			skipSymlink: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir, err := os.MkdirTemp(tmpDir, "")
			require.NoError(t, err)
			file := filepath.Join(dataDir, "file1.txt")
			require.NoError(t, os.WriteFile(file, []byte("foo"), os.ModePerm))
			symlink := filepath.Join(dataDir, "link_file1.txt")
			require.NoError(t, os.Symlink(file, symlink))

			runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+tt.name+" --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

			vars["LOCAL_DIR"] = dataDir
			vars["PREFIX"] = ""
			vars["BRANCH"] = tt.name
			vars["REF"] = tt.name
			lakectlCmd := Lakectl()
			if tt.skipSymlink {
				lakectlCmd = "LAKECTL_LOCAL_SKIP_NON_REGULAR_FILES=true " + lakectlCmd
			}
			runCmd(t, lakectlCmd+" local init lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+dataDir, false, false, vars)
			if tt.skipSymlink {
				RunCmdAndVerifyContainsText(t, lakectlCmd+" local status "+dataDir, false, "local  ║ added  ║ file1.txt", vars)
			} else {
				RunCmdAndVerifyFailureContainsText(t, lakectlCmd+" local status "+dataDir, false, "link_file1.txt: not a regular file", vars)
			}

			// Commit changes to branch
			if tt.skipSymlink {
				RunCmdAndVerifyContainsText(t, lakectlCmd+" local commit -m test "+dataDir, false, "Commit for branch \"${BRANCH}\" completed", vars)
			} else {
				RunCmdAndVerifyFailureContainsText(t, lakectlCmd+" local commit -m test "+dataDir, false, "link_file1.txt: not a regular file", vars)
			}

			// Check diff after commit
			if tt.skipSymlink {
				RunCmdAndVerifyContainsText(t, lakectlCmd+" local status "+dataDir, false, "No diff found", vars)
			} else {
				RunCmdAndVerifyFailureContainsText(t, lakectlCmd+" local status "+dataDir, false, "link_file1.txt: not a regular file", vars)
			}
		})
	}
}

func TestLakectlLocal_commit_remote_uncommitted(t *testing.T) {
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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
			name: "uncommitted_changes_-_on_boundary",
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
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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

func TestLakectlLocal_symlink_modes(t *testing.T) {
	tmpDir := t.TempDir()
	fd, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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
		name        string
		symlinkMode string
		expectError bool
	}{
		{
			name:        "follow-mode",
			symlinkMode: "follow",
			expectError: false,
		},
		{
			name:        "skip-mode",
			symlinkMode: "skip",
			expectError: false,
		},
		{
			name:        "support-mode",
			symlinkMode: "support",
			expectError: false,
		},
		{
			name:        "invalid-mode",
			symlinkMode: "invalid",
			expectError: false, // defaults to follow mode
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir, err := os.MkdirTemp(tmpDir, "")
			require.NoError(t, err)

			// Create test files and symlinks
			targetFile := filepath.Join(dataDir, "target.txt")
			require.NoError(t, os.WriteFile(targetFile, []byte("target content"), os.ModePerm))

			symlinkFile := filepath.Join(dataDir, "symlink.txt")
			require.NoError(t, os.Symlink(targetFile, symlinkFile))

			// Create a symlink to a directory
			targetDir := filepath.Join(dataDir, "target_dir")
			require.NoError(t, os.Mkdir(targetDir, os.ModePerm))
			dirFile := filepath.Join(targetDir, "dir_file.txt")
			require.NoError(t, os.WriteFile(dirFile, []byte("dir content"), os.ModePerm))

			symlinkDir := filepath.Join(dataDir, "symlink_dir")
			require.NoError(t, os.Symlink(targetDir, symlinkDir))

			runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+tt.name+" --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

			vars["LOCAL_DIR"] = dataDir
			vars["PREFIX"] = ""
			vars["BRANCH"] = tt.name
			vars["REF"] = tt.name

			lakectlCmd := Lakectl()
			if tt.symlinkMode != "" {
				lakectlCmd = fmt.Sprintf("LAKECTL_LOCAL_SYMLINK_MODE=%s %s", tt.symlinkMode, lakectlCmd)
			}

			// Test local init
			runCmd(t, lakectlCmd+" local init lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+dataDir, false, false, vars)

			// Test local status
			statusResult := runCmd(t, lakectlCmd+" local status "+dataDir, false, false, vars)
			require.Contains(t, statusResult, "target.txt")

			switch tt.symlinkMode {
			case "skip":
				// In skip mode, symlinks should be ignored
				require.NotContains(t, statusResult, "symlink.txt")
				require.NotContains(t, statusResult, "symlink_dir")
			case "support":
				// In support mode, symlinks should be included
				require.Contains(t, statusResult, "symlink.txt")
				require.Contains(t, statusResult, "symlink_dir")
			case "follow", "invalid", "":
				// In follow mode (default), symlinks should be followed
				require.Contains(t, statusResult, "symlink.txt")
				require.Contains(t, statusResult, "symlink_dir")
			}

			// Test local commit
			commitResult := runCmd(t, lakectlCmd+" local commit -m \"test symlink modes\" "+dataDir, false, false, vars)
			require.Contains(t, commitResult, "Commit for branch \""+tt.name+"\" completed")

			// Test local pull to verify the behavior
			pullResult := runCmd(t, lakectlCmd+" local pull "+dataDir, false, false, vars)
			require.Contains(t, pullResult, "Successfully synced changes")

			// Verify no diff after commit
			statusResult = runCmd(t, lakectlCmd+" local status "+dataDir, false, false, vars)
			require.Contains(t, statusResult, "No diff found")
		})
	}
}

func TestLakectlLocal_symlink_support_mode_upload_download(t *testing.T) {
	tmpDir := t.TempDir()
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"REF":     mainBranch,
		"PREFIX":  "",
	}

	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

	t.Run("upload_and_download_symlinks", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)

		// Create test structure
		targetFile := filepath.Join(dataDir, "target.txt")
		require.NoError(t, os.WriteFile(targetFile, []byte("target content"), os.ModePerm))

		symlinkFile := filepath.Join(dataDir, "symlink.txt")
		require.NoError(t, os.Symlink(targetFile, symlinkFile))

		// Create a symlink to a relative path
		relativeSymlink := filepath.Join(dataDir, "relative_symlink.txt")
		require.NoError(t, os.Symlink("target.txt", relativeSymlink))

		// Create a symlink to an absolute path
		absoluteSymlink := filepath.Join(dataDir, "absolute_symlink.txt")
		require.NoError(t, os.Symlink("/tmp/absolute_target.txt", absoluteSymlink))

		runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/symlink-support --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = ""
		vars["BRANCH"] = "symlink-support"
		vars["REF"] = "symlink-support"

		lakectlCmd := "LAKECTL_LOCAL_SYMLINK_MODE=support " + Lakectl()

		// Test local clone
		RunCmdAndVerifyContainsText(t, lakectlCmd+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

		// Test local status - should show symlinks
		statusResult := runCmd(t, lakectlCmd+" local status "+dataDir, false, false, vars)
		require.Contains(t, statusResult, "target.txt")
		require.Contains(t, statusResult, "symlink.txt")
		require.Contains(t, statusResult, "relative_symlink.txt")
		require.Contains(t, statusResult, "absolute_symlink.txt")

		// Test local commit
		commitResult := runCmd(t, lakectlCmd+" local commit -m \"upload symlinks\" "+dataDir, false, false, vars)
		require.Contains(t, commitResult, "Commit for branch \"symlink-support\" completed")

		// Create a new directory to test download
		downloadDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)

		vars["LOCAL_DIR"] = downloadDir
		RunCmdAndVerifyContainsText(t, lakectlCmd+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+downloadDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

		// Verify files were downloaded
		_, err = os.Stat(filepath.Join(downloadDir, "target.txt"))
		require.NoError(t, err)

		// Verify symlinks were recreated
		symlinkInfo, err := os.Lstat(filepath.Join(downloadDir, "symlink.txt"))
		require.NoError(t, err)
		require.True(t, symlinkInfo.Mode()&os.ModeSymlink != 0)

		// Check symlink target
		target, err := os.Readlink(filepath.Join(downloadDir, "symlink.txt"))
		require.NoError(t, err)
		require.Equal(t, targetFile, target)

		// Check relative symlink
		relativeTarget, err := os.Readlink(filepath.Join(downloadDir, "relative_symlink.txt"))
		require.NoError(t, err)
		require.Equal(t, "target.txt", relativeTarget)

		// Check absolute symlink
		absoluteTarget, err := os.Readlink(filepath.Join(downloadDir, "absolute_symlink.txt"))
		require.NoError(t, err)
		require.Equal(t, "/tmp/absolute_target.txt", absoluteTarget)
	})
}

func TestLakectlLocal_symlink_follow_mode(t *testing.T) {
	tmpDir := t.TempDir()
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"REF":     mainBranch,
		"PREFIX":  "",
	}

	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

	t.Run("follow_symlink_content", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)

		// Create test structure
		targetFile := filepath.Join(dataDir, "target.txt")
		require.NoError(t, os.WriteFile(targetFile, []byte("target content"), os.ModePerm))

		symlinkFile := filepath.Join(dataDir, "symlink.txt")
		require.NoError(t, os.Symlink(targetFile, symlinkFile))

		runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/symlink-follow --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = ""
		vars["BRANCH"] = "symlink-follow"
		vars["REF"] = "symlink-follow"

		lakectlCmd := "LAKECTL_LOCAL_SYMLINK_MODE=follow " + Lakectl()

		// Test local clone
		RunCmdAndVerifyContainsText(t, lakectlCmd+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

		// Test local status - should show symlinks as regular files (followed)
		statusResult := runCmd(t, lakectlCmd+" local status "+dataDir, false, false, vars)
		require.Contains(t, statusResult, "target.txt")
		require.Contains(t, statusResult, "symlink.txt")

		// Test local commit
		commitResult := runCmd(t, lakectlCmd+" local commit -m \"upload with follow mode\" "+dataDir, false, false, vars)
		require.Contains(t, commitResult, "Commit for branch \"symlink-follow\" completed")

		// Create a new directory to test download
		downloadDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)

		vars["LOCAL_DIR"] = downloadDir
		RunCmdAndVerifyContainsText(t, lakectlCmd+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+downloadDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

		// Verify both files exist as regular files (symlink content was followed)
		_, err = os.Stat(filepath.Join(downloadDir, "target.txt"))
		require.NoError(t, err)

		// The symlink should be a regular file with the target content
		symlinkInfo, err := os.Stat(filepath.Join(downloadDir, "symlink.txt"))
		require.NoError(t, err)
		require.True(t, symlinkInfo.Mode().IsRegular())

		// Verify content is the same
		targetContent, err := os.ReadFile(filepath.Join(downloadDir, "target.txt"))
		require.NoError(t, err)
		symlinkContent, err := os.ReadFile(filepath.Join(downloadDir, "symlink.txt"))
		require.NoError(t, err)
		require.Equal(t, targetContent, symlinkContent)
	})
}

func TestLakectlLocal_symlink_skip_mode(t *testing.T) {
	tmpDir := t.TempDir()
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"REF":     mainBranch,
		"PREFIX":  "",
	}

	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

	t.Run("skip_symlinks", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)

		// Create test structure
		targetFile := filepath.Join(dataDir, "target.txt")
		require.NoError(t, os.WriteFile(targetFile, []byte("target content"), os.ModePerm))

		symlinkFile := filepath.Join(dataDir, "symlink.txt")
		require.NoError(t, os.Symlink(targetFile, symlinkFile))

		// Create a broken symlink
		brokenSymlink := filepath.Join(dataDir, "broken_symlink.txt")
		require.NoError(t, os.Symlink("/nonexistent/path", brokenSymlink))

		runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/symlink-skip --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = ""
		vars["BRANCH"] = "symlink-skip"
		vars["REF"] = "symlink-skip"

		lakectlCmd := "LAKECTL_LOCAL_SYMLINK_MODE=skip " + Lakectl()

		// Test local clone
		RunCmdAndVerifyContainsText(t, lakectlCmd+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

		// Test local status - should only show regular files, skip symlinks
		statusResult := runCmd(t, lakectlCmd+" local status "+dataDir, false, false, vars)
		require.Contains(t, statusResult, "target.txt")
		require.NotContains(t, statusResult, "symlink.txt")
		require.NotContains(t, statusResult, "broken_symlink.txt")

		// Test local commit
		commitResult := runCmd(t, lakectlCmd+" local commit -m \"upload with skip mode\" "+dataDir, false, false, vars)
		require.Contains(t, commitResult, "Commit for branch \"symlink-skip\" completed")

		// Create a new directory to test download
		downloadDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)

		vars["LOCAL_DIR"] = downloadDir
		RunCmdAndVerifyContainsText(t, lakectlCmd+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+downloadDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

		// Verify only the regular file was downloaded
		_, err = os.Stat(filepath.Join(downloadDir, "target.txt"))
		require.NoError(t, err)

		// Verify symlinks were not downloaded
		_, err = os.Stat(filepath.Join(downloadDir, "symlink.txt"))
		require.True(t, os.IsNotExist(err))

		_, err = os.Stat(filepath.Join(downloadDir, "broken_symlink.txt"))
		require.True(t, os.IsNotExist(err))
	})
}

func TestLakectlLocal_symlink_recursive_handling(t *testing.T) {
	tmpDir := t.TempDir()
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"REF":     mainBranch,
		"PREFIX":  "",
	}

	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

	t.Run("recursive_symlink_detection", func(t *testing.T) {
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)

		// Create a circular symlink reference
		symlink1 := filepath.Join(dataDir, "symlink1.txt")
		symlink2 := filepath.Join(dataDir, "symlink2.txt")

		// Create a regular file first
		targetFile := filepath.Join(dataDir, "target.txt")
		require.NoError(t, os.WriteFile(targetFile, []byte("target content"), os.ModePerm))

		// Create symlinks that reference each other
		require.NoError(t, os.Symlink(symlink2, symlink1))
		require.NoError(t, os.Symlink(symlink1, symlink2))

		runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/symlink-recursive --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = ""
		vars["BRANCH"] = "symlink-recursive"
		vars["REF"] = "symlink-recursive"

		lakectlCmd := "LAKECTL_LOCAL_SYMLINK_MODE=follow " + Lakectl()

		// Test local clone
		RunCmdAndVerifyContainsText(t, lakectlCmd+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

		// Test local status - should handle recursive symlinks gracefully
		statusResult := runCmd(t, lakectlCmd+" local status "+dataDir, false, false, vars)
		require.Contains(t, statusResult, "target.txt")

		// The recursive symlinks should be detected and handled appropriately
		// This might result in an error or the symlinks being skipped
		if strings.Contains(statusResult, "symlink1.txt") || strings.Contains(statusResult, "symlink2.txt") {
			// If they appear in status, they should be handled
			t.Log("Recursive symlinks detected in status")
		}
	})
}

func TestLakectlLocal_symlink_configuration_validation(t *testing.T) {
	tmpDir := t.TempDir()
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
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
		name         string
		symlinkMode  string
		expectedMode string
		description  string
	}{
		{
			name:         "empty-mode-defaults-to-follow",
			symlinkMode:  "",
			expectedMode: "follow",
			description:  "Empty symlink mode should default to follow",
		},
		{
			name:         "case-insensitive-follow",
			symlinkMode:  "FOLLOW",
			expectedMode: "follow",
			description:  "Case insensitive follow mode should work",
		},
		{
			name:         "case-insensitive-skip",
			symlinkMode:  "Skip",
			expectedMode: "skip",
			description:  "Case insensitive skip mode should work",
		},
		{
			name:         "case-insensitive-support",
			symlinkMode:  "SUPPORT",
			expectedMode: "support",
			description:  "Case insensitive support mode should work",
		},
		{
			name:         "invalid-mode-defaults-to-follow",
			symlinkMode:  "invalid_mode",
			expectedMode: "follow",
			description:  "Invalid mode should default to follow",
		},
		{
			name:         "whitespace-trimmed",
			symlinkMode:  " follow ",
			expectedMode: "follow",
			description:  "Whitespace should be trimmed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir, err := os.MkdirTemp(tmpDir, "")
			require.NoError(t, err)

			// Create test files and symlinks
			targetFile := filepath.Join(dataDir, "target.txt")
			require.NoError(t, os.WriteFile(targetFile, []byte("target content"), os.ModePerm))

			symlinkFile := filepath.Join(dataDir, "symlink.txt")
			require.NoError(t, os.Symlink(targetFile, symlinkFile))

			runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+tt.name+" --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

			vars["LOCAL_DIR"] = dataDir
			vars["PREFIX"] = ""
			vars["BRANCH"] = tt.name
			vars["REF"] = tt.name

			lakectlCmd := Lakectl()
			if tt.symlinkMode != "" {
				lakectlCmd = fmt.Sprintf("LAKECTL_LOCAL_SYMLINK_MODE=%s %s", tt.symlinkMode, lakectlCmd)
			}

			// Test local init
			runCmd(t, lakectlCmd+" local init lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+dataDir, false, false, vars)

			// Test local status
			statusResult := runCmd(t, lakectlCmd+" local status "+dataDir, false, false, vars)
			require.Contains(t, statusResult, "target.txt")

			// Verify behavior based on expected mode
			switch tt.expectedMode {
			case "skip":
				require.NotContains(t, statusResult, "symlink.txt")
			case "support", "follow":
				require.Contains(t, statusResult, "symlink.txt")
			}

			// Test local commit
			commitResult := runCmd(t, lakectlCmd+" local commit -m \"test configuration validation\" "+dataDir, false, false, vars)
			require.Contains(t, commitResult, "Commit for branch \""+tt.name+"\" completed")

			// Verify no diff after commit
			statusResult = runCmd(t, lakectlCmd+" local status "+dataDir, false, false, vars)
			require.Contains(t, statusResult, "No diff found")
		})
	}
}

func TestLakectlLocal_symlink_with_existing_remote_symlinks(t *testing.T) {
	tmpDir := t.TempDir()
	repoName := GenerateUniqueRepositoryName()
	storage := GenerateUniqueStorageNamespace(repoName)
	vars := map[string]string{
		"REPO":    repoName,
		"STORAGE": storage,
		"BRANCH":  mainBranch,
		"REF":     mainBranch,
		"PREFIX":  "",
	}

	runCmd(t, Lakectl()+" repo create lakefs://"+repoName+" "+storage, false, false, vars)
	runCmd(t, Lakectl()+" log lakefs://"+repoName+"/"+mainBranch, false, false, vars)

	t.Run("download_existing_remote_symlinks", func(t *testing.T) {
		// First, create a local directory with symlinks and upload them in support mode
		uploadDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)

		targetFile := filepath.Join(uploadDir, "target.txt")
		require.NoError(t, os.WriteFile(targetFile, []byte("target content"), os.ModePerm))

		symlinkFile := filepath.Join(uploadDir, "symlink.txt")
		require.NoError(t, os.Symlink(targetFile, symlinkFile))

		runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/remote-symlinks --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

		vars["LOCAL_DIR"] = uploadDir
		vars["PREFIX"] = ""
		vars["BRANCH"] = "remote-symlinks"
		vars["REF"] = "remote-symlinks"

		// Upload symlinks in support mode
		uploadCmd := "LAKECTL_LOCAL_SYMLINK_MODE=support " + Lakectl()
		RunCmdAndVerifyContainsText(t, uploadCmd+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+uploadDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)
		runCmd(t, uploadCmd+" local commit -m \"upload symlinks\" "+uploadDir, false, false, vars)

		// Now test downloading the symlinks in different modes
		downloadTests := []struct {
			name          string
			symlinkMode   string
			expectSymlink bool
		}{
			{
				name:          "download-support-mode",
				symlinkMode:   "support",
				expectSymlink: true,
			},
			{
				name:          "download-follow-mode",
				symlinkMode:   "follow",
				expectSymlink: false, // Should be regular file with content
			},
			{
				name:          "download-skip-mode",
				symlinkMode:   "skip",
				expectSymlink: false, // Should not exist
			},
		}

		for _, dt := range downloadTests {
			t.Run(dt.name, func(t *testing.T) {
				downloadDir, err := os.MkdirTemp(tmpDir, "")
				require.NoError(t, err)

				vars["LOCAL_DIR"] = downloadDir
				downloadCmd := fmt.Sprintf("LAKECTL_LOCAL_SYMLINK_MODE=%s %s", dt.symlinkMode, Lakectl())
				RunCmdAndVerifyContainsText(t, downloadCmd+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+downloadDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

				// Verify target file exists
				_, err = os.Stat(filepath.Join(downloadDir, "target.txt"))
				require.NoError(t, err)

				// Check symlink behavior
				symlinkPath := filepath.Join(downloadDir, "symlink.txt")
				if dt.expectSymlink {
					// Should be a symlink
					symlinkInfo, err := os.Lstat(symlinkPath)
					require.NoError(t, err)
					require.True(t, symlinkInfo.Mode()&os.ModeSymlink != 0)

					// Check symlink target
					target, err := os.Readlink(symlinkPath)
					require.NoError(t, err)
					require.Equal(t, targetFile, target)
				} else if dt.symlinkMode == "skip" {
					// Should not exist
					_, err = os.Stat(symlinkPath)
					require.True(t, os.IsNotExist(err))
				} else {
					// Should be a regular file with content
					symlinkInfo, err := os.Stat(symlinkPath)
					require.NoError(t, err)
					require.True(t, symlinkInfo.Mode().IsRegular())

					// Verify content is the same as target
					targetContent, err := os.ReadFile(filepath.Join(downloadDir, "target.txt"))
					require.NoError(t, err)
					symlinkContent, err := os.ReadFile(symlinkPath)
					require.NoError(t, err)
					require.Equal(t, targetContent, symlinkContent)
				}
			})
		}
	})
}
