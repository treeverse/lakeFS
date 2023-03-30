package version

import (
	"regexp"
	"strings"
	"time"

	"github.com/tcnksm/go-latest"
)

const (
	// version start with v is optional and followed by 3 numbers with digits between them.	e.g v0.1.2
	validStableRelease  = `^(v*)(0|[1-9]+[0-9]*)\.(0|[1-9]+[0-9]*)\.(0|[1-9]+[0-9]*)$`
	defaultVersionCheck = "0.0.1"
	// releases URL
	DefaultReleasesURL = "https://github.com/treeverse/lakeFS/releases"
	GithubRepoOwner    = "treeverse"
	GithubRepoName     = "lakeFS"
)

var (
	UnreleasedVersion = "dev"
	// Version is the current git version of the code.  It is filled in by "make build".
	// Make sure to change that target in Makefile if you change its name or package.
	Version = "dev"
)

type githubVersionSource struct {
	githubTag     *latest.GithubTag
	fetchResponse *latest.FetchResponse
	fetchErr      error
	cachePeriod   time.Duration
	lastCheck     time.Time
}

func NewReleasesSource(cachePeriod time.Duration) latest.Source {
	return newGithubVersionSource(validStableRelease, cachePeriod)
}

func newGithubVersionSource(validVersionPattern string, cachePeriod time.Duration) *githubVersionSource {
	return &githubVersionSource{
		cachePeriod: cachePeriod,
		githubTag: &latest.GithubTag{
			Owner:      GithubRepoOwner,
			Repository: GithubRepoName,
			TagFilterFunc: func(remoteVersion string) bool {
				match, _ := regexp.MatchString(validVersionPattern, remoteVersion)
				return match
			},
		},
	}
}

func (g *githubVersionSource) Validate() error {
	return g.githubTag.Validate()
}

func (g *githubVersionSource) Fetch() (*latest.FetchResponse, error) {
	// check if never fetched yet or cache expired
	if time.Since(g.lastCheck) > g.cachePeriod {
		g.fetchResponse, g.fetchErr = g.githubTag.Fetch()
		g.lastCheck = time.Now()
	}
	return g.fetchResponse, g.fetchErr
}

func CheckLatestVersion(s latest.Source) (*latest.CheckResponse, error) {
	target := Version
	if IsVersionUnreleased() {
		target = defaultVersionCheck
	}
	return latest.Check(s, target)
}

func IsVersionUnreleased() bool {
	return strings.HasPrefix(Version, UnreleasedVersion)
}
