package git_test

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/git"
	giterror "github.com/treeverse/lakefs/pkg/git/errors"
)

func TestIsRepository(t *testing.T) {
	tmpdir := t.TempDir()
	tmpSubdir, err := os.MkdirTemp(tmpdir, "")
	require.NoError(t, err)
	defer func(name string) {
		err = os.RemoveAll(name)
		if err != nil {
			t.Error("failed to remove temp dir", err)
		}
	}(tmpSubdir)
	tmpFile, err := os.CreateTemp(tmpSubdir, "")
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(tmpFile.Name())
		_ = tmpFile.Close()
	}()

	require.False(t, git.IsRepository(tmpFile.Name()))
	require.False(t, git.IsRepository(tmpdir))

	// Init git repo on root
	require.NoError(t, exec.Command("git", "init", "-q", tmpdir).Run())
	require.False(t, git.IsRepository(tmpFile.Name()))
	require.True(t, git.IsRepository(tmpdir))
	require.True(t, git.IsRepository(tmpSubdir))
}

func TestGetRepositoryPath(t *testing.T) {
	var err error
	tmpdir := t.TempDir()
	tmpdir, err = filepath.EvalSymlinks(tmpdir) // on macOS tmpdir is a symlink
	require.NoError(t, err)
	tmpSubdir, err := os.MkdirTemp(tmpdir, "")
	require.NoError(t, err)
	defer func(name string) {
		_ = os.Remove(name)
	}(tmpSubdir)
	tmpFile, err := os.CreateTemp(tmpSubdir, "")
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(tmpFile.Name())
		_ = tmpFile.Close()
	}()

	_, err = git.GetRepositoryPath(tmpdir)
	require.ErrorIs(t, err, giterror.ErrNotARepository)
	_, err = git.GetRepositoryPath(tmpFile.Name())
	require.Error(t, err)

	// Init git repo on root
	require.NoError(t, exec.Command("git", "init", "-q", tmpdir).Run())
	gitPath, err := git.GetRepositoryPath(tmpdir)
	require.NoError(t, err)
	require.Equal(t, tmpdir, gitPath)
	_, err = git.GetRepositoryPath(tmpFile.Name())
	require.Error(t, err)
	gitPath, err = git.GetRepositoryPath(tmpSubdir)
	require.NoError(t, err)
	require.Equal(t, tmpdir, gitPath)
}

func TestIgnore(t *testing.T) {
	const (
		excludedFile = ".excluded.ex"
		trackedFile  = "file1"
		marker       = "Test Marker"
	)
	tmpdir, err := filepath.EvalSymlinks(t.TempDir()) // on macOS tmpdir is a symlink
	require.NoError(t, err)
	// create sub dir and file
	tmpSubdir, err := os.MkdirTemp(tmpdir, "")
	require.NoError(t, err)
	tmpFile, err := os.CreateTemp(tmpSubdir, "")
	require.NoError(t, err)
	excludedPath := filepath.Join(tmpSubdir, excludedFile)

	// Test we can't ignore if not a git repo
	_, err = git.Ignore(tmpdir, []string{}, []string{}, marker)
	require.ErrorIs(t, err, giterror.ErrNotARepository)
	_, err = git.Ignore(tmpFile.Name(), []string{}, []string{}, marker)
	require.Error(t, err)

	// Init git repo on tmpdir
	err = exec.Command("git", "init", "-q", tmpdir).Run()
	require.NoError(t, err)
	ignorePath := filepath.Join(tmpdir, git.IgnoreFile)

	// Create files in repo
	for _, fn := range []string{
		filepath.Join(tmpdir, trackedFile),
		filepath.Join(tmpSubdir, "should_be_ignored"),
		excludedPath,
	} {
		err = os.WriteFile(fn, []byte("content\n"), 0o644)
		require.NoError(t, err, "failed to create file %s", fn)
	}

	// Changing the working directory
	require.NoError(t, os.Chdir(tmpdir))
	verifyPathTracked(t, []string{filepath.Base(tmpSubdir), trackedFile})

	_, err = git.Ignore(tmpFile.Name(), []string{}, []string{excludedPath}, marker)
	require.Error(t, err)
	result, err := git.Ignore(tmpdir, []string{}, []string{excludedPath}, marker)
	require.NoError(t, err)
	require.Equal(t, ignorePath, result)
	relExcludedPath, err := filepath.Rel(tmpdir, excludedPath)
	require.NoError(t, err)

	verifyPathTracked(t, []string{filepath.Base(tmpSubdir), trackedFile})

	_, err = git.Ignore(tmpSubdir, []string{tmpFile.Name()}, []string{excludedPath}, marker)
	require.NoError(t, err)
	relDataFile, err := filepath.Rel(tmpdir, tmpFile.Name())
	require.NoError(t, err)
	verifyPathTracked(t, []string{trackedFile})

	_, err = git.Ignore(tmpSubdir, []string{tmpSubdir, filepath.Join(tmpdir, trackedFile)}, []string{}, marker)
	require.NoError(t, err)
	relDataSubDir, err := filepath.Rel(tmpdir, tmpSubdir)
	require.NoError(t, err)
	verifyPathTracked(t, []string{git.IgnoreFile})

	_, err = git.Ignore(tmpSubdir, []string{tmpSubdir, filepath.Join(tmpdir, trackedFile), ignorePath}, []string{}, marker)
	require.NoError(t, err)
	require.Equal(t, ignorePath, result)

	contents, err := os.ReadFile(ignorePath)
	require.NoError(t, err)
	verifyPathTracked(t, nil)

	expectedGitIgnore := fmt.Sprintf("# %s\n%s\n!%s\n%s\n%s\n%s\n# End %s\n",
		marker,
		relDataFile, relExcludedPath, filepath.Join(relDataSubDir, "*"), trackedFile, git.IgnoreFile,
		marker)
	require.Equal(t, expectedGitIgnore, string(contents))
}

func verifyPathTracked(t *testing.T, paths []string) {
	cmd := exec.Command("git", "status")
	r, w, _ := os.Pipe()
	cmd.Stdout = w
	require.NoError(t, cmd.Run())
	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	outStr := string(out)

	if len(paths) == 0 {
		require.Contains(t, outStr, "nothing to commit")
	} else {
		for _, p := range paths {
			require.Contains(t, outStr, p)
		}
	}
}

func TestParseURL(t *testing.T) {
	cases := []struct {
		Url         string
		ExpectedUrl *git.URL
	}{
		{
			Url: "git@github.com:treeverse/lakeFS.git",
			ExpectedUrl: &git.URL{
				Server:  "github.com",
				Owner:   "treeverse",
				Project: "lakeFS",
			},
		},
		{
			Url: "ssh://git@github.com/tree/lake.git",
			ExpectedUrl: &git.URL{
				Server:  "github.com",
				Owner:   "tree",
				Project: "lake",
			},
		},
		{
			Url: "https://github.com/treeverse/lakeFS2.git",
			ExpectedUrl: &git.URL{
				Server:  "github.com",
				Owner:   "treeverse",
				Project: "lakeFS2",
			},
		},
		{
			Url: "git://git@192.168.1.20:MyGroup/MyProject.git",
			ExpectedUrl: &git.URL{
				Server:  "192.168.1.20",
				Owner:   "MyGroup",
				Project: "MyProject",
			},
		},
		{
			Url: "git://git@192.168.1.20:22:MyGroup/MyProject.git",
			ExpectedUrl: &git.URL{
				Server:  "192.168.1.20:22",
				Owner:   "MyGroup",
				Project: "MyProject",
			},
		},
		{
			Url: "git://git@github.com:Hyphened-Owner/Hyphened-Project.git",
			ExpectedUrl: &git.URL{
				Server:  "github.com",
				Owner:   "Hyphened-Owner",
				Project: "Hyphened-Project",
			},
		},
		{
			Url:         "bad_url",
			ExpectedUrl: nil,
		},
	}
	for _, tt := range cases {
		t.Run(tt.Url, func(t *testing.T) {
			parsed := git.ParseURL(tt.Url)
			require.Equal(t, tt.ExpectedUrl, parsed)
		})
	}
}
