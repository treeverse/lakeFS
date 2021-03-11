package uri

import (
	"errors"
	"strings"
)

type state int

const (
	ProtocolSeparator = "://"
	LakeFSProtocol    = "lakefs"

	RefSeparator  = '@'
	PathSeparator = '/'

	stateInRepo state = iota
	stateInRef
	stateInPath
)

var (
	ErrMalformedURI   = errors.New("malformed lakefs uri")
	ErrInvalidRepoURI = errors.New("not a valid repo uri")
	ErrInvalidRefURI  = errors.New("not a valid ref uri")
	ErrInvalidPathURI = errors.New("not a valid path uri")
)

type URI struct {
	// Protocol must always match "lakefs" to be considered valid
	Protocol string
	// Repository is the name of the repository being addressed
	Repository string
	// Ref represents the reference in the repository (commit, tag, branch, etc.)
	Ref string
	// Path is a path to an object (or prefix of such) in lakeFS. It *could* be null since there's a difference between
	// 	an empty path ("lakefs://repo@branch/", and no path at all e.g. "lakefs://repo@branch").
	// 	Since path is the only URI part that is allowed to be empty, it is represented as a pointer.
	Path *string
}

func (u *URI) IsRepository() bool {
	return len(u.Repository) > 0 && len(u.Ref) == 0 && u.Path == nil
}

func (u *URI) IsRef() bool {
	return len(u.Repository) > 0 && len(u.Ref) > 0 && u.Path == nil
}

func (u *URI) IsFullyQualified() bool {
	return len(u.Repository) > 0 && len(u.Ref) > 0 && u.Path != nil
}

func (u *URI) String() string {
	var buf strings.Builder
	buf.WriteString(u.Protocol)
	buf.WriteString(ProtocolSeparator)
	buf.WriteString(u.Repository)

	if len(u.Ref) == 0 {
		return buf.String()
	}
	buf.WriteRune(RefSeparator)
	buf.WriteString(u.Ref)

	if u.Path == nil {
		return buf.String()
	}
	buf.WriteRune(PathSeparator)
	buf.WriteString(*u.Path)

	return buf.String()
}

func Parse(str string) (*URI, error) {
	// start with protocol
	protoParts := strings.Split(str, ProtocolSeparator)
	const uriParts = 2
	if len(protoParts) != uriParts {
		return nil, ErrMalformedURI
	}
	if !strings.EqualFold(protoParts[0], LakeFSProtocol) {
		return nil, ErrMalformedURI
	}

	var uri URI
	uri.Protocol = protoParts[0]
	var path string

	var state = stateInRepo
	var buf strings.Builder
	for _, ch := range protoParts[1] {
		switch {
		case ch == RefSeparator && state == stateInRepo:
			uri.Repository = buf.String()
			state = stateInRef
			buf.Reset()
		case ch == PathSeparator && state == stateInRef:
			uri.Ref = buf.String()
			state = stateInPath
			uri.Path = &path
			buf.Reset()
		default:
			buf.WriteRune(ch)
		}
	}
	if buf.Len() > 0 {
		switch state {
		case stateInRepo:
			uri.Repository = buf.String()
		case stateInRef:
			uri.Ref = buf.String()
		case stateInPath:
			path = buf.String()
			uri.Path = &path
		}
	}
	return &uri, nil
}

func Equals(a, b *URI) bool {
	// same protocol
	return strings.EqualFold(a.Protocol, b.Protocol) &&
		// same repository
		strings.EqualFold(a.Repository, b.Repository) &&
		// same ref
		strings.EqualFold(a.Ref, b.Ref) &&
		// either both contain no path, or both do, and that path is equal
		((a.Path == nil && b.Path == nil) ||
			(a.Path != nil && b.Path != nil && *a.Path == *b.Path))
}

func ValidateRepoURI(str string) error {
	u, err := Parse(str)
	if err != nil {
		return err
	}
	if !u.IsRepository() {
		return ErrInvalidRepoURI
	}
	return nil
}

func ValidatePathURI(str string) error {
	u, err := Parse(str)
	if err != nil {
		return err
	}
	if !u.IsFullyQualified() {
		return ErrInvalidPathURI
	}
	return nil
}

func ValidateRefURI(str string) error {
	u, err := Parse(str)
	if err != nil {
		return err
	}
	if !u.IsRef() {
		return ErrInvalidRefURI
	}
	return nil
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
