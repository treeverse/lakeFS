package uri

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/treeverse/lakefs/pkg/validator"
)

const (
	LakeFSSchema          = "lakefs"
	LakeFSSchemaSeparator = "://"
	PathSeparator         = "/"
)

var (
	ErrMalformedURI     = errors.New("malformed lakefs uri")
	ErrInvalidRepoURI   = errors.New("not a valid repo uri")
	ErrInvalidRefURI    = errors.New("not a valid ref uri")
	ErrInvalidBranchURI = errors.New("not a valid branch uri")
	ErrInvalidPathURI   = errors.New("not a valid path uri")
)

type URI struct {
	// Repository is the name of the repository being addressed
	Repository string
	// Ref represents the reference in the repository (commit, tag, branch, etc.)
	Ref string
	// Path is a path to an object (or prefix of such) in lakeFS. It *could* be null since there's a difference between
	// 	an empty path ("lakefs://repo/branch/", and no path at all e.g. "lakefs://repo/branch").
	// 	Since path is the only URI part that is allowed to be empty, it is represented as a pointer.
	Path *string
}

func (u *URI) IsRepository() bool {
	return len(u.Repository) > 0 && len(u.Ref) == 0 && u.Path == nil && validator.ReValidRepositoryID.MatchString(u.Repository)
}

func (u *URI) IsRef() bool {
	return len(u.Repository) > 0 && len(u.Ref) > 0 && u.Path == nil && validator.ReValidRepositoryID.MatchString(u.Repository) && validator.ReValidRef.MatchString(u.Ref)
}

func (u *URI) IsBranch() bool {
	return len(u.Repository) > 0 && len(u.Ref) > 0 && u.Path == nil && validator.ReValidRepositoryID.MatchString(u.Repository) && validator.ReValidBranchID.MatchString(u.Ref)
}

func (u *URI) IsFullyQualified() bool {
	return len(u.Repository) > 0 && len(u.Ref) > 0 && u.Path != nil && validator.ReValidRepositoryID.MatchString(u.Repository) && validator.ReValidRef.MatchString(u.Ref)
}

func (u *URI) GetPath() string {
	if u.Path == nil {
		return ""
	}
	return *u.Path
}

func (u *URI) String() string {
	var buf strings.Builder
	buf.WriteString(LakeFSSchema)
	buf.WriteString(LakeFSSchemaSeparator)
	buf.WriteString(u.Repository)
	if len(u.Ref) == 0 {
		return buf.String()
	}
	buf.WriteString(PathSeparator)
	buf.WriteString(u.Ref)
	if u.Path == nil {
		return buf.String()
	}
	buf.WriteString(PathSeparator)
	buf.WriteString(*u.Path)
	return buf.String()
}

// ParseWithBaseURI parse URI uses base URI as prefix when set and input doesn't start with lakeFS protocol
func ParseWithBaseURI(s string, baseURI string) (*URI, error) {
	if len(baseURI) > 0 && !strings.HasPrefix(s, LakeFSSchema+LakeFSSchemaSeparator) {
		s = baseURI + s
	}
	u, err := Parse(s)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", s, err)
	}
	return u, nil
}

func Parse(s string) (*URI, error) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme != LakeFSSchema || u.User != nil {
		return nil, ErrMalformedURI
	}
	repository := u.Hostname()
	if len(repository) == 0 {
		return nil, ErrMalformedURI
	}
	var ref string
	var path *string
	if len(u.Path) > 0 {
		if !strings.HasPrefix(u.Path, PathSeparator) {
			return nil, ErrMalformedURI
		}
		const refAndPathParts = 2
		levels := strings.SplitN(u.Path[1:], PathSeparator, refAndPathParts)
		if len(levels) == refAndPathParts {
			ref = levels[0]
			path = &levels[1]
		} else if len(levels) == 1 {
			ref = levels[0]
		}
	}
	return &URI{
		Repository: repository,
		Ref:        ref,
		Path:       path,
	}, nil
}

func Equals(a, b *URI) bool {
	return a.Repository == b.Repository &&
		a.Ref == b.Ref &&
		// either both contain no path, or both do, and that path is equal
		((a.Path == nil && b.Path == nil) || (a.Path != nil && b.Path != nil && *a.Path == *b.Path))
}

func IsValid(str string) bool {
	_, err := Parse(str)
	return err == nil
}

func Must(u *URI, e error) *URI {
	if e != nil {
		panic(e)
	}
	return u
}
