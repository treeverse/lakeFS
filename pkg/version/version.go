package version

import (
	"regexp"

	"github.com/tcnksm/go-latest"
)

const (
	// version start with v is optional and followed by 3 numbers with digits between them.	e.g v0.1.2
	validRelease string = `^(v*)(0|[1-9]+[0-9]*)\.(0|[1-9]+[0-9]*)\.(0|[1-9]+[0-9]*)$`
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
}

func NewReleasesSource() latest.Source {
	return newGithubVersionSource(validRelease)
}

func newGithubVersionSource(validVersionPattern string) *githubVersionSource {
	return &githubVersionSource{
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
	if g.fetchResponse == nil && g.fetchErr == nil {
		g.fetchResponse, g.fetchErr = g.githubTag.Fetch()
	}
	return g.fetchResponse, g.fetchErr
}

func CheckLatestVersion(s latest.Source, targetVersion string) (*latest.CheckResponse, error) {
	// TODO(isan) what to do when the version is invalid / not comperable? dev, local etc
	if match, err := regexp.MatchString(validRelease, targetVersion); !match || err != nil {
		targetVersion = "0.1.0"
	}
	return latest.Check(s, targetVersion)
}
