package path

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
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

	ErrPathMalformed = errors.New("encoded path is malformed")
)

type Ref struct {
	Branch   string
	CommitID catalog.CommitID
}

type ResolvedPath struct {
	Ref      string
	Path     string
	WithPath bool
}

type ResolvedAbsolutePath struct {
	Repo      string
	Reference string
	Path      string
}

//func ResolveRef(ref string) (Ref, error) {
//	if !strings.HasPrefix(ref, "#") {
//		return Ref{Branch: ref}, nil
//	}
//	refData, err := base58.Decode(ref[1:])
//	if err != nil {
//		return Ref{}, fmt.Errorf("%w: ref decode", ErrPathMalformed)
//	}
//	if !utf8.Valid(refData) {
//		return Ref{}, fmt.Errorf("%w: ref utf8", ErrPathMalformed)
//	}
//	const refPartsCount = 2
//	parts := strings.SplitN(string(refData), ":", refPartsCount)
//	if len(parts) != refPartsCount {
//		return Ref{}, fmt.Errorf("%w: missing commit id", ErrPathMalformed)
//	}
//	id, err := strconv.Atoi(parts[1])
//	if err != nil {
//		return Ref{}, fmt.Errorf("%w: invalid commit id", ErrPathMalformed)
//	}
//	return Ref{
//		Branch:   parts[0],
//		CommitID: catalog.CommitID(id),
//	}, nil
//}

//func ResolveAbsolutePath(encodedPath string) (ResolvedAbsolutePath, error) {
//	r := ResolvedAbsolutePath{}
//	match := EncodedAbsPathRe.FindStringSubmatch(encodedPath)
//	if len(match) == 0 {
//		return r, ErrPathMalformed
//	}
//	result := make(map[string]string)
//	for i, name := range EncodedAbsPathRe.SubexpNames() {
//		if i != 0 && name != "" {
//			result[name] = match[i]
//		}
//	}
//	r.Repo = result["repo"]
//	r.Path = result["path"]
//	r.Ref = result["ref"]
//	return r, nil
//}

func ResolveAbsolutePath(encodedPath string) (ResolvedAbsolutePath, error) {
	const encodedPartsCount = 3
	encodedPath = strings.TrimLeft(encodedPath, "/")
	parts := strings.SplitN(encodedPath, "/", encodedPartsCount)
	if len(parts) != encodedPartsCount {
		return ResolvedAbsolutePath{}, ErrPathMalformed
	}
	return ResolvedAbsolutePath{
		Repo:      parts[0],
		Reference: parts[1],
		Path:      parts[2],
	}, nil
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
	return block.JoinPathParts([]string{ref, path})
}
