package path

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/treeverse/lakefs/graveler"

	"github.com/treeverse/lakefs/block"
)

const (
	Separator = "/"

	rePath      = "(?P<path>.*)"
	reReference = `(?P<ref>\w[-\w]*)`
)

var (
	EncodedPathRe          = regexp.MustCompile(fmt.Sprintf("^/?%s/%s", reReference, rePath))
	EncodedPathReferenceRe = regexp.MustCompile(fmt.Sprintf("^/?%s", reReference))

	ErrPathMalformed = errors.New("encoded path is malformed")
)

type Ref struct {
	Branch   string
	CommitID graveler.CommitID
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
	r := ResolvedPath{}
	if len(encodedPath) == 0 {
		return r, nil // empty path.
	}
	// try reference with path or just reference regexp
	for _, re := range []*regexp.Regexp{EncodedPathRe, EncodedPathReferenceRe} {
		match := re.FindStringSubmatch(encodedPath)
		if len(match) == 0 {
			continue
		}
		for i, name := range re.SubexpNames() {
			switch name {
			case "ref":
				r.Ref = match[i]
			case "path":
				r.Path = match[i]
				r.WithPath = true
			}
		}
		return r, nil
	}
	return r, ErrPathMalformed
}

func WithRef(path, ref string) string {
	return block.JoinPathParts([]string{ref, path})
}
