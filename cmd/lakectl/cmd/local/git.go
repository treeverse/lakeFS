package local

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"gopkg.in/yaml.v3"
)

const (
	ConfigFileName = "data.yaml"
)

var (
	ErrGitConfiguration              = errors.New("configuration error")
	ErrGitConfigurationNotFilesystem = fmt.Errorf("%w: not a valid local git repository", ErrGitConfiguration)
	ErrInvalidKeyValueFormat         = errors.New("invalid key=value format")
)

type Conf struct {
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

func Config() (*Conf, error) {
	currentLocation, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return PathConfig(currentLocation)
}

func PathConfig(p string) (*Conf, error) {
	repo, err := findRepository(p)
	if err != nil {
		return nil, err
	}
	root, err := findRepositoryRoot(repo)
	if err != nil {
		return nil, err
	}
	return &Conf{
		root:       root,
		repository: repo,
	}, nil
}

func (c *Conf) Repository() *git.Repository {
	return c.repository
}

func (c *Conf) IsClean() (bool, error) {
	repo := c.Repository()
	workTree, err := repo.Worktree()
	if err != nil {
		return false, err
	}
	status, err := workTree.Status()
	if err != nil {
		return false, err
	}
	return status.IsClean(), nil
}

func (c *Conf) CurrentCommitId() (string, error) {
	repo := c.Repository()
	head, err := repo.Head()
	if err != nil {
		return "", err
	}
	return head.Hash().String(), nil
}

func (c *Conf) GetRemote(name string) (string, error) {
	repo := c.Repository()
	remote, err := repo.Remote(name)
	if err != nil {
		return "", err
	}
	if remote.Config().IsFirstURLLocal() {
		if len(remote.Config().URLs) > 1 {
			return remote.Config().URLs[1], nil
		}
	}
	if len(remote.Config().URLs) > 0 {
		return remote.Config().URLs[0], nil
	}
	return "", fmt.Errorf("could not find remote URL for '%s'", name)
}

func (c *Conf) HasRemote(name string) (bool, error) {
	repo := c.Repository()
	_, err := repo.Remote(name)
	if errors.Is(err, git.ErrRemoteNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *Conf) Root() string {
	return c.root
}

// RelativeToRoot returns a relative path from p that is relative to the root of the current git repository
func (c *Conf) RelativeToRoot(p string) (string, error) {
	if !path.IsAbs(p) {
		// make it relative to where we are
		var err error
		p, err = filepath.Abs(p)
		if err != nil {
			return "", err
		}
	}
	relativePath, err := filepath.Rel(c.Root(), p)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(relativePath, "..") || strings.HasPrefix(relativePath, "/") {
		return "", fmt.Errorf("path is not in repository")
	}
	return relativePath, nil
}

func (c *Conf) getConfigPath() string {
	root := c.Root()
	return path.Join(root, ConfigFileName)
}

func (c *Conf) Initialized() (bool, error) {
	return FileExists(c.getConfigPath())
}

func (c *Conf) GetSourcesConfig() (*SourcesConfig, error) {
	cfg := &SourcesConfig{}
	initialized, err := c.Initialized()
	if err != nil {
		return nil, err
	}
	if !initialized {
		return cfg, nil
	}
	location := c.getConfigPath()
	data, err := os.ReadFile(location)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Conf) HasSource(target string) (bool, error) {
	conf, err := c.GetSourcesConfig()
	if err != nil {
		return false, err
	}
	_, exists := conf.Sources[target]
	return exists, nil
}

func (c *Conf) AddSource(target, remote, atVersion string) error {
	conf, err := c.GetSourcesConfig()
	if err != nil {
		return err
	}
	if conf.Sources == nil {
		conf.Sources = make(map[string]Source)
	}
	conf.Sources[target] = Source{
		Remote:    remote,
		AtVersion: atVersion,
	}
	return c.SaveSourcesConfig(conf)
}

func (c *Conf) UpdateSourceVersion(target, atVersion string) error {
	conf, err := c.GetSourcesConfig()
	if err != nil {
		return err
	}
	source := conf.Sources[target]
	source.AtVersion = atVersion
	conf.Sources[target] = source
	return c.SaveSourcesConfig(conf)
}

func (c *Conf) GetSource(target string) (Source, error) {
	var src Source
	conf, err := c.GetSourcesConfig()
	if err != nil {
		return src, err
	}
	return conf.Sources[target], nil
}

func (c *Conf) SaveSourcesConfig(cfg *SourcesConfig) error {
	// ensure directory exists first
	location := c.getConfigPath()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(location, data, DefaultFileMask)
}

func (c *Conf) CheckoutGitCommitId(gitCommitId string) error {
	wt, err := c.repository.Worktree()
	if err != nil {
		return err
	}
	commitHashBytes, err := hex.DecodeString(gitCommitId)
	if err != nil {
		return err
	}
	var gitHash plumbing.Hash
	copy(gitHash[:], commitHashBytes)
	return wt.Checkout(&git.CheckoutOptions{
		Hash:  gitHash,
		Force: true,
	})
}

func (c *Conf) GitIgnore(pattern string) error {
	if !strings.HasPrefix(pattern, PathSeparator) {
		// prepend a leading '/' since we want to ignore a fully qualified path
		//  within the git repository
		pattern = PathSeparator + pattern
	}
	root := c.Root()
	gitIgnoreLocation := path.Join(root, ".gitignore")
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
	_, err = f.WriteString(fmt.Sprintf("# ignored because versioned in lakeFS (i.e. do `git data pull`):\n%s\n", pattern))
	if err != nil {
		return err
	}
	return f.Close()
}
