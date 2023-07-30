package git

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/treeverse/lakefs/pkg/ioutils"
	"golang.org/x/exp/slices"
)

const (
	IgnoreFile        = ".gitignore"
	IgnoreDefaultMode = 0644
)

func git(dir string, args ...string) (int, string) {
	stdout := new(strings.Builder)
	stderr := new(strings.Builder)
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return exitError.ExitCode(), stderr.String()
		}
		return -1, err.Error()
	}
	return 0, stdout.String()
}

// IsGitRepository Return true if dir is a path to a directory in a git repository, false otherwise
func IsGitRepository(dir string) bool {
	rc, _ := git(dir, "rev-parse", "--is-inside-work-tree")
	return rc == 0
}

// GetGitRepositoryPath Returns the git repository root path if dir is a directory inside a git repository, otherwise returns error
func GetGitRepositoryPath(dir string) (string, error) {
	rc, out := git(dir, "rev-parse", "--show-toplevel")
	if rc == 0 {
		return strings.TrimSpace(out), nil
	}
	if strings.Contains(out, ErrNotARepository.Error()) {
		return "", ErrNotARepository
	}
	return "", fmt.Errorf("%w: exit code %d: %s", ErrGitError, rc, out)
}

// Ignore Modify .ignore file to include a section headed by the marker string and contains the provided paths
// If section exists, it will append paths to the given section, otherwise writes the sections at the end of the file.
// File paths must be absolute.
// Creates the .ignore file if it doesn't exist.
func Ignore(dir string, ignorePaths, excludePaths []string, marker string) (string, error) {
	gitDir, err := GetGitRepositoryPath(dir)
	if err != nil {
		return "", err
	}

	var ignoreEntries []string

	for _, p := range ignorePaths {
		pathInRepo, err := filepath.Rel(gitDir, p)
		if err != nil {
			return "", err
		}
		if ioutils.IsDir(p) {
			pathInRepo = filepath.Join(pathInRepo, "*")
		}
		ignoreEntries = append(ignoreEntries, pathInRepo)
	}
	for _, p := range excludePaths {
		pathInRepo, err := filepath.Rel(gitDir, p)
		if err != nil {
			return "", err
		}
		if ioutils.IsDir(p) {
			pathInRepo = filepath.Join(pathInRepo, "*")
		}
		ignoreEntries = append(ignoreEntries, "!"+pathInRepo)
	}

	ignoreFilePath := filepath.Join(gitDir, IgnoreFile)

	found := false
	var (
		mode          os.FileMode = IgnoreDefaultMode
		ignoreContent []string
	)
	markerLine := "# " + marker
	info, err := os.Stat(ignoreFilePath)
	switch {
	case err == nil: // ignore file exists
		mode = info.Mode()
		ignoreFile, err := os.Open(ignoreFilePath)
		if err != nil {
			return "", err
		}
		fileScanner := bufio.NewScanner(ignoreFile)
		for fileScanner.Scan() {
			line := strings.TrimSpace(fileScanner.Text())
			ignoreContent = append(ignoreContent, line)
			if line == markerLine {
				found = true
				for fileScanner.Scan() {
					line = strings.TrimSpace(fileScanner.Text())
					if line == "" {
						ignoreContent = append(ignoreContent, "")
						break
					}
					if !slices.Contains(ignoreEntries, line) {
						ignoreContent = append(ignoreContent, line)
					}
				}
				ignoreContent = append(ignoreContent, ignoreEntries...)
			}
		}

		if !found { // Add the marker and ignore list to the end of the file
			ignoreContent = append(ignoreContent, "")
			ignoreContent = append(ignoreContent, markerLine)
			ignoreContent = append(ignoreContent, ignoreEntries...)
		}

		err = ignoreFile.Close()
		if err != nil {
			return "", err
		}
	case !os.IsNotExist(err):
		return "", err
	default: // File doesn't exist
		ignoreContent = append(ignoreContent, markerLine)
		ignoreContent = append(ignoreContent, ignoreEntries...)
	}

	buffer := strings.Join(ignoreContent, "\n") + "\n"
	if err = os.WriteFile(ignoreFilePath, []byte(buffer), mode); err != nil {
		return "", err
	}

	return ignoreFilePath, nil
}
