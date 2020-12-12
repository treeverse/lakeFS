package path

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/treeverse/lakefs/catalog/mvcc"

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
	CommitID mvcc.CommitID
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
	result := make(map[string]string)
	r := ResolvedPath{}
	if len(encodedPath) == 0 {
		return r, nil // empty path.
	}
	match := EncodedPathRe.FindStringSubmatch(encodedPath)
	if len(match) == 0 {
		// attempt to see if this is a ref only
		match = EncodedPathReferenceRe.FindStringSubmatch(encodedPath)
		if len(match) > 0 {
			for i, name := range EncodedPathReferenceRe.SubexpNames() {
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
