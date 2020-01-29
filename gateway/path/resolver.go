package path

import (
	"fmt"
	"regexp"

	pth "github.com/treeverse/lakefs/index/path"

	"golang.org/x/xerrors"
)

const (
	Separator = "/"

	CreateRepoMatch = "{repo:[^\\/]+}"

	RepoMatch   = "{repo:[a-z0-9\\-]+}"
	RepoReMatch = "(?P<repo>[a-z0-9\\-]+)"

	PathMatch   = "{path:.*}"
	PathReMatch = "(?P<path>.*)"

	RefspecMatch   = "{refspec:[a-z0-9\\-]+}"
	RefspecReMatch = "(?P<refspec>[a-z0-9\\-]+)"
)

var (
	EncodedPathRe    = regexp.MustCompile(fmt.Sprintf("/?%s/%s", RefspecReMatch, PathReMatch))
	EncodedAbsPathRe = regexp.MustCompile(fmt.Sprintf("/?%s/%s/%s", RepoReMatch, RefspecReMatch, PathReMatch))

	ErrPathMalformed = xerrors.New("encoded path is malformed")
)

type ResolvedPath struct {
	Path    string
	Refspec string
}

type ResolvedAbsolutePath struct {
	Repo    string
	Path    string
	Refspec string
}

func ResolveAbsolutePath(encodedPath string) (ResolvedAbsolutePath, error) {
	r := ResolvedAbsolutePath{}
	match := EncodedAbsPathRe.FindStringSubmatch(encodedPath)
	if len(match) == 0 {
		return r, ErrPathMalformed
	}
	result := make(map[string]string)
	for i, name := range EncodedAbsPathRe.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}
	r.Repo = result["repo"]
	r.Path = result["path"]
	r.Refspec = result["refspec"]
	return r, nil
}

func ResolvePath(encodedPath string) (ResolvedPath, error) {
	r := ResolvedPath{}
	match := EncodedPathRe.FindStringSubmatch(encodedPath)
	if len(match) == 0 {
		return r, ErrPathMalformed
	}
	result := make(map[string]string)
	for i, name := range EncodedPathRe.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}
	r.Path = result["path"]
	r.Refspec = result["refspec"]
	return r, nil
}

func WithRefspec(path, refspec string) string {
	return pth.Join([]string{refspec, path})
}
