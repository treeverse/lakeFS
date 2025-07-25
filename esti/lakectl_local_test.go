package esti

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
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

	// Test init with symlinks present locally
	t.Run("init with symlinks", func(t *testing.T) {
		symlinkDir, err := os.MkdirTemp(tmpDir, "symlink_test")
		require.NoError(t, err)

		// Create a regular file and a symlink
		regularFile := filepath.Join(symlinkDir, "regular.txt")
		require.NoError(t, os.WriteFile(regularFile, []byte("content"), os.ModePerm))
		symlinkFile := filepath.Join(symlinkDir, "symlink.txt")
		require.NoError(t, os.Symlink(regularFile, symlinkFile))

		vars["LOCAL_DIR"] = symlinkDir
		vars["REF"] = mainBranch
		vars["PREFIX"] = "symlink_test/"

		// Test with symlink support enabled
		lakectlWithSymlinks := "LAKECTL_LOCAL_SYMLINK_SUPPORT=true " + Lakectl()
		RunCmdAndVerifySuccessWithFile(t, lakectlWithSymlinks+" local init lakefs://"+repoName+"/"+mainBranch+"/symlink_test "+symlinkDir, false, "lakectl_local_init_symlink", vars)

		// Verify both files are detected
		result := runCmd(t, lakectlWithSymlinks+" local status "+symlinkDir, false, false, vars)
		require.Contains(t, result, "regular.txt")
		require.Contains(t, result, "symlink.txt")
	})
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

	t.Run("clone with symlinks", func(t *testing.T) {
		symlinkBranch := "symlink_clone_test"
		runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+symlinkBranch+" --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

		// Create files with symlinks on remote
		regularFile := "target_file.txt"
		vars["FILE_PATH"] = regularFile
		vars["BRANCH"] = symlinkBranch
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+symlinkBranch+"/"+regularFile, false, "lakectl_fs_upload_symlink", vars)

		// Create symlink using metadata
		symlinkFile := "symlink_to_target.txt"
		content := ""
		_, err := UploadContentWithMetadata(context.Background(), client, repoName, symlinkBranch, symlinkFile, map[string]string{
			apiutil.SymlinkMetadataKey: "target_file.txt",
		}, "text/plain", strings.NewReader(content))
		require.NoError(t, err)

		runCmd(t, Lakectl()+" commit lakefs://"+repoName+"/"+symlinkBranch+" -m 'add files with symlinks'", false, false, vars)

		// Clone with symlink support enabled
		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		vars["LOCAL_DIR"] = dataDir
		vars["BRANCH"] = symlinkBranch
		vars["REF"] = symlinkBranch
		vars["PREFIX"] = ""

		lakectlWithSymlinks := "LAKECTL_LOCAL_SYMLINK_SUPPORT=true " + Lakectl()
		RunCmdAndVerifyContainsText(t, lakectlWithSymlinks+" local clone lakefs://"+repoName+"/"+symlinkBranch+"/ "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/ to ${LOCAL_DIR}.", vars)

		// Verify files exist
		regularFilePath := filepath.Join(dataDir, regularFile)
		require.FileExists(t, regularFilePath)

		symlinkFilePath := filepath.Join(dataDir, symlinkFile)
		require.FileExists(t, symlinkFilePath)

		// Check if it's a symlink
		info, err := os.Lstat(symlinkFilePath)
		require.NoError(t, err)
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(symlinkFilePath)
			require.NoError(t, err)
			require.Equal(t, "target_file.txt", target)
		}

		// Verify no changes after clone
		result := runCmd(t, lakectlWithSymlinks+" local status "+dataDir, false, false, vars)
		require.Contains(t, result, "No diff found")
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

	// Test pull with symlinks
	t.Run("pull with symlinks", func(t *testing.T) {
		symlinkBranch := "symlink_pull_test"
		runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+symlinkBranch+" --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)

		vars["LOCAL_DIR"] = dataDir
		vars["BRANCH"] = symlinkBranch
		vars["REF"] = symlinkBranch

		lakectlWithSymlinks := "LAKECTL_LOCAL_SYMLINK_SUPPORT=true " + Lakectl()
		RunCmdAndVerifyContainsText(t, lakectlWithSymlinks+" local clone lakefs://"+repoName+"/"+symlinkBranch+"/ "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/ to ${LOCAL_DIR}.", vars)

		// Add files with symlinks to remote
		const regularFile = "target_file.txt"
		vars["FILE_PATH"] = regularFile
		RunCmdAndVerifySuccessWithFile(t, Lakectl()+" fs upload -s files/ro_1k lakefs://"+repoName+"/"+symlinkBranch+"/"+regularFile, false, "lakectl_fs_upload_symlink", vars)

		// Create symlink using metadata
		const symlinkFile = "symlink_to_target.txt"
		metadata := map[string]string{
			apiutil.SymlinkMetadataKey: "target_file.txt",
		}
		ctx := context.Background()
		uploadResponse, err := UploadContentWithMetadata(ctx, client, repoName, symlinkBranch, symlinkFile, metadata, "text/plain", bytes.NewReader([]byte{}))
		require.NoError(t, err)
		require.Equal(t, uploadResponse.StatusCode(), http.StatusCreated)

		// Commit changes with symlinks
		runCmd(t, Lakectl()+" commit lakefs://"+repoName+"/"+symlinkBranch+" -m 'add files with symlinks'", false, false, vars)

		// Pull changes with symlinks
		result := runCmd(t, lakectlWithSymlinks+" local pull "+dataDir, false, false, vars)
		require.Contains(t, result, "download target_file.txt")
		require.Contains(t, result, "download symlink_to_target.txt")

		// Verify files exist
		regularFilePath := filepath.Join(dataDir, regularFile)
		require.FileExists(t, regularFilePath)

		symlinkFilePath := filepath.Join(dataDir, symlinkFile)
		require.FileExists(t, symlinkFilePath)

		// Check if symlink was created properly
		info, err := os.Lstat(symlinkFilePath)
		require.NoError(t, err)
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(symlinkFilePath)
			require.NoError(t, err)
			require.Equal(t, "target_file.txt", target)
		}

		// Verify no diff after pull
		result = runCmd(t, lakectlWithSymlinks+" local status "+dataDir, false, false, vars)
		require.Contains(t, result, "No diff found")
	})
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

	// Test commit with symlink support
	t.Run("commit with symlink support", func(t *testing.T) {
		symlinkBranch := "symlink_commit_test"
		runCmd(t, Lakectl()+" branch create lakefs://"+repoName+"/"+symlinkBranch+" --source lakefs://"+repoName+"/"+mainBranch, false, false, vars)

		dataDir, err := os.MkdirTemp(tmpDir, "")
		require.NoError(t, err)
		deleted := prefix + "/subdir/deleted.png"

		localCreateTestData(t, vars, append(objects, deleted))

		vars["LOCAL_DIR"] = dataDir
		vars["PREFIX"] = ""
		vars["BRANCH"] = symlinkBranch
		vars["REF"] = symlinkBranch
		lakectlWithSymlinks := "LAKECTL_LOCAL_SYMLINK_SUPPORT=true " + Lakectl()
		RunCmdAndVerifyContainsText(t, lakectlWithSymlinks+" local clone lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+dataDir, false, "Successfully cloned lakefs://${REPO}/${REF}/${PREFIX} to ${LOCAL_DIR}.", vars)

		RunCmdAndVerifyContainsText(t, lakectlWithSymlinks+" local status "+dataDir, false, "No diff found", vars)

		// Create local files including symlinks
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "subdir"), os.ModePerm))
		regularFile := filepath.Join(dataDir, "test.txt")
		fd, err = os.Create(regularFile)
		require.NoError(t, err)
		require.NoError(t, fd.Close())

		symlinkFile := filepath.Join(dataDir, "link_to_test.txt")
		require.NoError(t, os.Symlink(regularFile, symlinkFile))

		subRegularFile := filepath.Join(dataDir, "subdir", "test.txt")
		fd, err = os.Create(subRegularFile)
		require.NoError(t, err)
		require.NoError(t, fd.Close())

		subSymlinkFile := filepath.Join(dataDir, "subdir", "link_to_test.txt")
		require.NoError(t, os.Symlink(subRegularFile, subSymlinkFile))

		require.NoError(t, os.Remove(filepath.Join(dataDir, deleted)))

		RunCmdAndVerifyContainsText(t, lakectlWithSymlinks+" local status "+dataDir, false, "local  ║ added   ║ test.txt", vars)
		RunCmdAndVerifyContainsText(t, lakectlWithSymlinks+" local status "+dataDir, false, "local  ║ added   ║ link_to_test.txt", vars)
		RunCmdAndVerifyContainsText(t, lakectlWithSymlinks+" local status "+dataDir, false, "local  ║ added   ║ subdir/test.txt", vars)
		RunCmdAndVerifyContainsText(t, lakectlWithSymlinks+" local status "+dataDir, false, "local  ║ added   ║ subdir/link_to_test.txt", vars)

		// Commit changes to branch
		RunCmdAndVerifyContainsText(t, lakectlWithSymlinks+" local commit -m test "+dataDir, false, "Commit for branch \"${BRANCH}\" completed", vars)

		// Check no diff after commit
		RunCmdAndVerifyContainsText(t, lakectlWithSymlinks+" local status "+dataDir, false, "No diff found", vars)
	})
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
		name           string
		skipSymlink    bool
		symlinkSupport bool
	}{
		{
			name:        "skip-symlink",
			skipSymlink: true,
		},
		{
			name:        "fail-on-symlink",
			skipSymlink: false,
		},
		{
			name:           "support-symlink",
			symlinkSupport: true,
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
			} else if tt.symlinkSupport {
				lakectlCmd = "LAKECTL_LOCAL_SYMLINK_SUPPORT=true " + lakectlCmd
			}
			runCmd(t, lakectlCmd+" local init lakefs://"+repoName+"/"+vars["BRANCH"]+"/"+vars["PREFIX"]+" "+dataDir, false, false, vars)
			if tt.skipSymlink {
				RunCmdAndVerifyContainsText(t, lakectlCmd+" local status "+dataDir, false, "local  ║ added  ║ file1.txt", vars)
			} else if tt.symlinkSupport {
				RunCmdAndVerifyContainsText(t, lakectlCmd+" local status "+dataDir, false, "local  ║ added  ║ file1.txt", vars)
				RunCmdAndVerifyContainsText(t, lakectlCmd+" local status "+dataDir, false, "local  ║ added  ║ link_file1.txt", vars)
			} else {
				RunCmdAndVerifyFailureContainsText(t, lakectlCmd+" local status "+dataDir, false, "link_file1.txt: not a regular file", vars)
			}

			// Commit changes to branch
			if tt.skipSymlink || tt.symlinkSupport {
				RunCmdAndVerifyContainsText(t, lakectlCmd+" local commit -m test "+dataDir, false, "Commit for branch \"${BRANCH}\" completed", vars)
			} else {
				RunCmdAndVerifyFailureContainsText(t, lakectlCmd+" local commit -m test "+dataDir, false, "link_file1.txt: not a regular file", vars)
			}

			// Check diff after commit
			if tt.skipSymlink || tt.symlinkSupport {
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
