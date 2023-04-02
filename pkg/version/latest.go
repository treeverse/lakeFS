package version

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/go-github/v50/github"
)

const (
	latestVersionTimeout = 10 * time.Second

	// releases URL
	DefaultReleasesURL = "https://github.com/treeverse/lakeFS/releases"
	GithubRepoOwner    = "treeverse"
	GithubRepoName     = "lakeFS"
)

var (
	ErrInvalidConfig = errors.New("invalid configuration for version source")
	ErrHTTPStatus    = errors.New("unexpected HTTP status code")
)

type LatestVersionResponse struct {
	CheckTime      time.Time `json:"check_time"`
	Outdated       bool      `json:"outdated"`
	LatestVersion  string    `json:"latest_version"`
	CurrentVersion string    `json:"current_version"`
}

type VersionSource interface {
	FetchLatestVersion() (string, error)
}

type CachedVersionSource struct {
	Source        VersionSource
	lastCheck     time.Time
	cachePeriod   time.Duration
	fetchErr      error
	fetchResponse string
}

func NewDefaultVersionSource(cachePeriod time.Duration) VersionSource {
	gh := NewGithubReleases(GithubRepoOwner, GithubRepoName)
	return NewCachedSource(gh, latestVersionTimeout)
}

func NewCachedSource(src VersionSource, cachePeriod time.Duration) *CachedVersionSource {
	return &CachedVersionSource{
		Source:      src,
		cachePeriod: cachePeriod,
	}
}

func (cs *CachedVersionSource) FetchLatestVersion() (string, error) {
	if time.Since(cs.lastCheck) > cs.cachePeriod {
		cs.fetchResponse, cs.fetchErr = cs.Source.FetchLatestVersion()
		cs.lastCheck = time.Now()
	}
	return cs.fetchResponse, cs.fetchErr
}

type GithubReleases struct {
	owner      string
	repository string
	client     *github.Client
}

func NewGithubReleases(owner, repository string) *GithubReleases {
	httpClient := &http.Client{Timeout: latestVersionTimeout}

	return &GithubReleases{
		owner:      owner,
		repository: repository,
		client:     github.NewClient(httpClient),
	}
}

func (gh *GithubReleases) FetchLatestVersion() (string, error) {
	release, resp, err := gh.client.Repositories.GetLatestRelease(context.Background(), gh.owner, gh.repository)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected HTTP response %d: %w", resp.StatusCode, ErrHTTPStatus)
	}

	return release.GetTagName(), nil
}
