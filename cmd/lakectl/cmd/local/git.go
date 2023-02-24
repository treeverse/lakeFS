package local

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/go-git/go-git/v5"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage/filesystem"
)

var (
	ErrNotInRepository               = errors.New("not in a git repository")
	ErrGitConfiguration              = errors.New("configuration error")
	ErrGitConfigurationNotFilesystem = fmt.Errorf("%w: not a valid local git repository", ErrGitConfiguration)
	ErrInvalidKeyValueFormat         = errors.New("invalid key=value format")
)

type Repository struct {
	root       string
	repository *git.Repository
}

func findRepository(location string) (*git.Repository, error) {
	repository, err := git.PlainOpenWithOptions(location, &git.PlainOpenOptions{
		DetectDotGit:          true,
		EnableDotGitCommonDir: true,
	})
	if err != nil {
		return nil, err
	}
	return repository, nil
}

func findRepositoryRoot(repo *git.Repository) (string, error) {
	s, ok := repo.Storer.(*filesystem.Storage)
	if !ok {
		return "", ErrGitConfigurationNotFilesystem
	}
	return strings.TrimSuffix(
		s.Filesystem().Root(),
		string(os.PathSeparator)+".git",
	), nil
}

func FindRepository(p string) (*Repository, error) {
	repo, err := findRepository(p)
	if err != nil {
		return nil, err
	}
	root, err := findRepositoryRoot(repo)
	if err != nil {
		return nil, err
	}
	return &Repository{
		root:       root,
		repository: repo,
	}, nil
}

func (c *Repository) GetRemoteURLs() (map[string]string, error) {
	remotes, err := c.repository.Remotes()
	if err != nil {
		return nil, err
	}
	remoteMapping := make(map[string]string)
	for _, remote := range remotes {
		urls := remote.Config().URLs
		if remote.Config().IsFirstURLLocal() && len(urls) > 1 {
			urls = urls[1:]
		}
		remoteUrl := strings.Join(urls, " ; ")
		remoteMapping[remote.Config().Name] = remoteUrl
	}
	return remoteMapping, nil
}

func (c *Repository) CurrentCommitId() (string, error) {
	head, err := c.repository.Head()
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return "", ErrNoCommitFound
		}
		return "", fmt.Errorf("could not get current git HEAD: %w", err)
	}
	return head.Hash().String(), nil
}

func (c *Repository) Root() string {
	return c.root
}

// RelativeToRoot returns a relative path from p that is relative to the root of the current git repository
func (c *Repository) RelativeToRoot(p string) (string, error) {
	if !path.IsAbs(p) {
		// make it relative to where we are
		var err error
		p, err = filepath.Abs(p)
		if err != nil {
			return "", err
		}
	}
	relativePath, err := filepath.Rel(c.root, p)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(relativePath, "..") || strings.HasPrefix(relativePath, "/") {
		return "", fmt.Errorf("path is not in repository")
	}
	return relativePath, nil
}

func (c *Repository) AddIgnorePattern(pattern string) error {
	if !strings.HasPrefix(pattern, PathSeparator) {
		// prepend a leading '/' since we want to ignore a fully qualified path
		//  within the git repository
		pattern = PathSeparator + pattern
	}
	gitIgnoreLocation := path.Join(c.root, ".gitignore")
	var mode os.FileMode = DefaultFileMask
	info, err := os.Stat(gitIgnoreLocation)
	if err == nil {
		mode = info.Mode()
		ignoreFile, err := os.Open(gitIgnoreLocation)
		if err != nil {
			return err
		}
		fileScanner := bufio.NewScanner(ignoreFile)
		for fileScanner.Scan() {
			line := fileScanner.Text()
			if line == pattern {
				return nil // already exists in the file!
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
	f, err := os.OpenFile(gitIgnoreLocation, os.O_APPEND|os.O_WRONLY|os.O_CREATE, mode)
	if err != nil {
		return err
	}
	_, err = f.WriteString(fmt.Sprintf("# versioned in lakeFS:\n%s\n", pattern))
	if err != nil {
		return err
	}
	return f.Close()
}
