package git

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/treeverse/lakefs/pkg/fileutil"
	"golang.org/x/exp/slices"
)

const (
	IgnoreFile        = ".gitignore"
	IgnoreDefaultMode = 0644
)

func git(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// IsRepository Return true if dir is a path to a directory in a git repository, false otherwise
func IsRepository(dir string) bool {
	_, err := git(dir, "rev-parse", "--is-inside-work-tree")
	return err == nil
}

// GetRepositoryPath Returns the git repository root path if dir is a directory inside a git repository, otherwise returns error
func GetRepositoryPath(dir string) (string, error) {
	out, err := git(dir, "rev-parse", "--show-toplevel")
	if err == nil {
		return strings.TrimSpace(out), nil
	}
	if strings.Contains(out, "not a git repository") {
		return "", ErrNotARepository
	}
	return "", fmt.Errorf("%s: %w", out, ErrGitError)
}

func createEntriesForIgnore(dir string, paths []string, exclude bool) ([]string, error) {
	var entries []string
	for _, p := range paths {
		pathInRepo, err := filepath.Rel(dir, p)
		if err != nil {
			return nil, fmt.Errorf("%s :%w", p, err)
		}
		isDir, err := fileutil.IsDir(p)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%s :%w", p, err)
		}
		if isDir {
			pathInRepo = filepath.Join(pathInRepo, "*")
		}
		if exclude {
			pathInRepo = "!" + pathInRepo
		}
		entries = append(entries, pathInRepo)
	}
	return entries, nil
}

func updateIgnoreFileSection(contents []byte, marker string, entries []string) []byte {
	var newContent []byte
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		newContent = append(newContent, []byte(fmt.Sprintln(line))...)
		if line == marker {
			for scanner.Scan() {
				line = strings.TrimSpace(scanner.Text())
				if line == "" {
					break
				}
				if !slices.Contains(entries, line) {
					newContent = append(newContent, []byte(fmt.Sprintln(line))...)
				}
			}
			buffer := strings.Join(entries, fmt.Sprintln("")) + fmt.Sprintln("")
			newContent = append(newContent, buffer...)
		}
	}

	return newContent
}

// Ignore modify/create .ignore file to include a section headed by the marker string and contains the provided ignore and exclude paths.
// If section exists, it will append paths to the given section, otherwise writes the section at the end of the file.
// All file paths must be absolute.
// dir is a path in the git repository, if a .gitignore file is not found, a new file will be created in the repository root
func Ignore(dir string, ignorePaths, excludePaths []string, marker string) (string, error) {
	gitDir, err := GetRepositoryPath(dir)
	if err != nil {
		return "", err
	}

	ignoreEntries, err := createEntriesForIgnore(gitDir, ignorePaths, false)
	if err != nil {
		return "", err
	}
	excludeEntries, err := createEntriesForIgnore(gitDir, excludePaths, true)
	if err != nil {
		return "", err
	}
	ignoreEntries = append(ignoreEntries, excludeEntries...)

	var (
		mode       os.FileMode = IgnoreDefaultMode
		ignoreFile []byte
	)
	ignoreFilePath := filepath.Join(gitDir, IgnoreFile)
	markerLine := "# " + marker
	info, err := os.Stat(ignoreFilePath)
	switch {
	case err == nil: // ignore file exists
		mode = info.Mode()
		ignoreFile, err = os.ReadFile(ignoreFilePath)
		if err != nil {
			return "", err
		}
		idx := bytes.Index(ignoreFile, []byte(markerLine))
		if idx == -1 {
			section := fmt.Sprintln(markerLine) + strings.Join(ignoreEntries, fmt.Sprintln("")) + fmt.Sprintln("")
			ignoreFile = append(ignoreFile, section...)
		} else { // Update section
			ignoreFile = updateIgnoreFileSection(ignoreFile, markerLine, ignoreEntries)
		}

	case !os.IsNotExist(err):
		return "", err
	default: // File doesn't exist
		section := fmt.Sprintln(markerLine) + strings.Join(ignoreEntries, fmt.Sprintln("")) + fmt.Sprintln("")
		ignoreFile = append(ignoreFile, []byte(section)...)
	}

	if err = os.WriteFile(ignoreFilePath, ignoreFile, mode); err != nil {
		return "", err
	}

	return ignoreFilePath, nil
}
