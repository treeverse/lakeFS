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

	RepoMatch   = "{repo:[a-zA-Z0-9\\-]+}"
	RepoReMatch = "(?P<repo>[a-zA-Z0-9\\-]+)"

	PathMatch   = "{path:.*}"
	PathReMatch = "(?P<path>.*)"

	RefMatch   = "{ref:[a-z0-9\\-]+}"
	RefReMatch = "(?P<ref>[a-z0-9\\-]+)"
)

var (
	EncodedPathRe    = regexp.MustCompile(fmt.Sprintf("/?%s/%s", RefReMatch, PathReMatch))
	EncodedPathRefRe = regexp.MustCompile(fmt.Sprintf("/?%s", RefReMatch))
	EncodedAbsPathRe = regexp.MustCompile(fmt.Sprintf("/?%s/%s/%s", RepoReMatch, RefReMatch, PathReMatch))

	ErrPathMalformed = xerrors.New("encoded path is malformed")
)

type ResolvedPath struct {
	Path     string
	Ref      string
	WithPath bool
}

type ResolvedAbsolutePath struct {
	Repo string
	Path string
	Ref  string
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
	r.Ref = result["ref"]
	return r, nil
}

func ResolvePath(encodedPath string) (ResolvedPath, error) {
	result := make(map[string]string)
	r := ResolvedPath{}
	if len(encodedPath) == 0 {
		return r, nil // empty path.
	}
	match := EncodedPathRe.FindStringSubmatch(encodedPath)
	if len(match) == 0 {
		// attempt to see if this is a ref only
		match = EncodedPathRefRe.FindStringSubmatch(encodedPath)
		if len(match) > 0 {
			for i, name := range EncodedPathRefRe.SubexpNames() {
				if i != 0 && name != "" {
					result[name] = match[i]
				}
			}
			r.Ref = result["ref"]
			return r, nil
		}
		r.WithPath = false
		return r, ErrPathMalformed
	}
	for i, name := range EncodedPathRe.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}
	r.Path = result["path"]
	r.Ref = result["ref"]
	r.WithPath = true
	return r, nil
}

func WithRef(path, ref string) string {
	return pth.Join([]string{ref, path})
}
