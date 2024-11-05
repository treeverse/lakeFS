package git

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/treeverse/lakefs/pkg/fileutil"
	giterror "github.com/treeverse/lakefs/pkg/git/errors"
	"github.com/treeverse/lakefs/pkg/git/internal"
	"golang.org/x/exp/slices"
)

const (
	IgnoreFile        = ".gitignore"
	IgnoreDefaultMode = 0o644
	NoRemoteRC        = 2
)

var (
	RemoteRegex     = regexp.MustCompile(`(?P<server>[\w.:]+)[/:](?P<owner>[\w.-]+)/(?P<project>[\w.-]+)\.git$`)
	CommitTemplates = map[string]string{
		"github.com":    "https://github.com/{{ .Owner }}/{{ .Project }}/commit/{{ .Ref }}",
		"gitlab.com":    "https://gitlab.com/{{ .Owner }}/{{ .Project }}/-/commit/{{ .Ref }}",
		"bitbucket.org": "https://bitbucket.org/{{ .Owner }}/{{ .Project }}/commits/{{ .Ref }}",
	}
)

type URL struct {
	Server  string
	Owner   string
	Project string
}

func git(dir string, args ...string) (string, int, error) {
	_, err := exec.LookPath("git") // assume git is in the path, otherwise consider as not having git support
	if err != nil {
		return "", 0, giterror.ErrNoGit
	}
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	rc := 0
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			rc = exitError.ExitCode()
		} else {
			rc = -1
		}
	}
	return string(out), rc, err
}

// IsRepository Return true if dir is a path to a directory in a git repository, false otherwise
func IsRepository(dir string) bool {
	_, _, err := git(dir, "rev-parse", "--is-inside-work-tree")
	return err == nil
}

// GetRepositoryPath Returns the git repository root path if dir is a directory inside a git repository, otherwise returns error
func GetRepositoryPath(dir string) (string, error) {
	out, _, err := git(dir, "rev-parse", "--show-toplevel")
	return internal.HandleOutput(out, err)
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

// updateIgnoreFileSection updates or inserts a section, identified by a marker, within a file's contents,
// and returns the modified contents as a byte slice. The section begins with "# [marker]" and ends with "# End [marker]".
// It retains existing entries and appends new entries from the provided slice.
func updateIgnoreFileSection(contents []byte, marker string, entries []string) []byte {
	var lines []string
	if len(contents) > 0 {
		lines = strings.Split(string(contents), "\n")
	}

	// point to the existing section or to the end of the file
	startIdx := slices.IndexFunc(lines, func(s string) bool {
		return strings.HasPrefix(s, "# "+marker)
	})
	var endIdx int
	if startIdx == -1 {
		startIdx = len(lines)
		endIdx = startIdx
	} else {
		endIdx = slices.IndexFunc(lines[startIdx:], func(s string) bool {
			return s == "" || strings.HasPrefix(s, "# End "+marker)
		})
		if endIdx == -1 {
			endIdx = len(lines)
		} else {
			endIdx += startIdx + 1
		}
	}

	// collect existing entries - entries found in the section that are not commented out
	var existing []string
	for i := startIdx; i < endIdx; i++ {
		if lines[i] == "" ||
			strings.HasPrefix(lines[i], "#") ||
			slices.Contains(entries, lines[i]) {
			continue
		}
		existing = append(existing, lines[i])
	}

	// delete and insert new content
	lines = slices.Delete(lines, startIdx, endIdx)
	newContent := []string{"# " + marker}
	newContent = append(newContent, existing...)
	newContent = append(newContent, entries...)
	newContent = append(newContent, "# End "+marker)
	lines = slices.Insert(lines, startIdx, newContent...)

	// join lines make sure content ends with new line
	if lines[len(lines)-1] != "" {
		lines = append(lines, "")
	}
	result := strings.Join(lines, "\n")
	return []byte(result)
}

// Ignore modify/create .ignore file to include a section headed by the marker string and contains the provided ignore and exclude paths.
// If the section exists, it will append paths to the given section, otherwise writes the section at the end of the file.
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

	// read ignore file content
	ignoreFilePath := filepath.Join(gitDir, IgnoreFile)
	ignoreFile, err := os.ReadFile(ignoreFilePath)
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}

	// get current file mode, if exists
	var mode os.FileMode = IgnoreDefaultMode
	if ignoreFile != nil {
		if info, err := os.Stat(ignoreFilePath); err == nil {
			mode = info.Mode()
		}
	}

	// update ignore file local section and write back
	ignoreFile = updateIgnoreFileSection(ignoreFile, marker, ignoreEntries)
	if err = os.WriteFile(ignoreFilePath, ignoreFile, mode); err != nil {
		return "", err
	}

	return ignoreFilePath, nil
}

func CurrentCommit(path string) (string, error) {
	out, _, err := git(path, "rev-parse", "--short", "HEAD")
	return internal.HandleOutput(out, err)
}

func MetadataFor(path, ref string) (map[string]string, error) {
	kv := make(map[string]string)
	kv["git_commit_id"] = ref
	originURL, err := Origin(path)
	if errors.Is(err, giterror.ErrRemoteNotFound) {
		return kv, nil // no additional data to add
	} else if err != nil {
		return kv, err
	}
	parsed := ParseURL(originURL)
	if parsed != nil {
		if tmpl, ok := CommitTemplates[parsed.Server]; ok {
			t := template.Must(template.New("url").Parse(tmpl))
			out := new(strings.Builder)
			_ = t.Execute(out, struct {
				Owner   string
				Project string
				Ref     string
			}{
				Owner:   parsed.Owner,
				Project: parsed.Project,
				Ref:     ref,
			})
			kv[fmt.Sprintf("::lakefs::%s::url[url:ui]", parsed.Server)] = out.String()
		}
	}
	return kv, nil
}

func Origin(path string) (string, error) {
	out, rc, err := git(path, "remote", "get-url", "origin")
	if rc == NoRemoteRC {
		// from Git's man page:
		// "When subcommands such as add, rename, and remove can’t find the remote in question,
		//	the exit status is 2"
		return "", nil
	}
	return internal.HandleOutput(out, err)
}

func ParseURL(raw string) *URL {
	matches := RemoteRegex.FindStringSubmatch(raw)
	if matches == nil { // TODO niro: How to handle better changes in templates?
		return nil
	}
	return &URL{
		Server:  matches[RemoteRegex.SubexpIndex("server")],
		Owner:   matches[RemoteRegex.SubexpIndex("owner")],
		Project: matches[RemoteRegex.SubexpIndex("project")],
	}
}
