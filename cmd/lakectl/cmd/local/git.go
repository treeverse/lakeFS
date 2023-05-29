package local

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
)

const (
	GitIgnoreFile        = ".gitignore"
	GitIgnoreDefaultMode = 0644
	GitIgnoreMarker      = "# ignored by lakectl local:"
)

var (
	ErrGitError       = errors.New("git error")
	ErrInvalidRemote  = fmt.Errorf("%w: invalid remote", ErrGitError)
	ErrRemoteNotFound = fmt.Errorf("%w: remote not found", ErrGitError)
	ErrNotARepository = fmt.Errorf("%w: not in a repository", ErrGitError)
)

var (
	GitRemoteRe = regexp.MustCompile(
		"(?P<server>[\\w\\.\\:]+)[\\/:](?P<owner>[\\w\\.]+)\\/(?P<project>[\\w\\.]+)\\.git$")
	GitCommitTemplates = map[string]string{
		"github.com":    "https://github.com/{{ .Owner }}/{{ .Project }}/commit/{{ .Ref }}",
		"gitlab.com":    "https://gitlab.com/{{ .Owner }}/{{ .Project }}/-/commit/{{ .Ref }}",
		"bitbucket.org": "https://bitbucket.org/{{ .Owner }}/{{ .Project }}/commits/{{ .Ref }}",
	}
)

type GitUrl struct {
	Server  string
	Owner   string
	Project string
}

func git(dir string, args ...string) (int, string) {
	stdout := new(strings.Builder)
	stderr := new(strings.Builder)
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			return exitError.ExitCode(), stderr.String()
		}
		return -1, err.Error()
	}
	return 0, stdout.String()
}

func IsGitRepository(path string) bool {
	rc, _ := git(path, "rev-parse", "--is-inside-work-tree")
	return rc == 0
}

func GetGitRepositoryPath(path string) (string, error) {
	rc, out := git(path, "rev-parse", "--show-toplevel")
	if rc == 0 {
		return strings.TrimSpace(out), nil
	}
	return "", fmt.Errorf("%w: exit code %d: %s", ErrGitError, rc, out)
}

func GetGitCurrentCommit(path string) (string, error) {
	rc, out := git(path, "rev-parse", "--short", "HEAD")
	if rc == 0 {
		return strings.TrimSpace(out), nil
	}
	return "", fmt.Errorf("%w: exit code %d: %s", ErrGitError, rc, out)
}

func GitMetadataFor(path, ref string) (map[string]string, error) {
	kv := make(map[string]string)
	kv["git_commit_id"] = ref
	originUrl, err := GetGitOrigin(path)
	if errors.Is(err, ErrRemoteNotFound) {
		return kv, nil // no additional data to add
	} else if err != nil {
		return kv, err
	}
	parsed, err := ParseGitUrl(originUrl)
	if err == nil {
		if tmpl, ok := GitCommitTemplates[parsed.Server]; ok {
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

func ParseGitUrl(raw string) (*GitUrl, error) {
	matches := GitRemoteRe.FindStringSubmatch(raw)
	if matches == nil {
		return nil, fmt.Errorf("%w: '%s'", ErrInvalidRemote, raw)
	}
	return &GitUrl{
		Server:  matches[GitRemoteRe.SubexpIndex("server")],
		Owner:   matches[GitRemoteRe.SubexpIndex("owner")],
		Project: matches[GitRemoteRe.SubexpIndex("project")],
	}, nil
}

func GetGitOrigin(path string) (string, error) {
	rc, out := git(path, "remote", "get-url", "origin")
	if rc == 2 {
		// from Git's man page:
		//" When subcommands such as add, rename, and remove can’t find the remote in question,
		//	the exit status is 2"
		return "", nil
	} else if rc != 0 {
		return "", fmt.Errorf("%w: exit code %d: %s", ErrGitError, rc, out)
	}
	return strings.TrimSpace(out), nil
}

func GitIgnore(path string) error {
	if !IsGitRepository(path) {
		return fmt.Errorf("%w: %s", ErrNotARepository, path)
	}
	gitDir, err := GetGitRepositoryPath(path)
	if err != nil {
		return err
	}

	pathInRepo, err := filepath.Rel(gitDir, path)
	if err != nil {
		return err
	}
	ignoreFilePath := filepath.Join(gitDir, GitIgnoreFile)
	pathToIgnore := filepath.ToSlash(filepath.Join(pathInRepo, "*"))
	pathToExclude := filepath.ToSlash(filepath.Join(pathInRepo, IndexFileName))
	found := false
	var mode os.FileMode = GitIgnoreDefaultMode
	info, err := os.Stat(ignoreFilePath)
	if err == nil {
		mode = info.Mode()
		ignoreFile, err := os.Open(ignoreFilePath)
		if err != nil {
			return err
		}
		fileScanner := bufio.NewScanner(ignoreFile)

		for fileScanner.Scan() {
			line := fileScanner.Text()
			if line == pathToIgnore {
				found = true
				break // already exists in the file!
			}
		}
		// if we got this far, line not in the file.
		err = ignoreFile.Close()
		if err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	if found {
		return nil
	}

	// otherwise, add it
	f, err := os.OpenFile(ignoreFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, mode)
	if err != nil {
		return err
	}

	_, err = f.WriteString(fmt.Sprintf(
		"\n%s\n%s\n!%s\n\n", GitIgnoreMarker, pathToIgnore, pathToExclude))
	if err != nil {
		return err
	}
	return f.Close()

}
