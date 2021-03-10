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
	Protocol   string
	Repository string
	Ref        string
	Path       string
}

func (u *URI) IsRepository() bool {
	return len(u.Repository) > 0 && len(u.Ref) == 0 && len(u.Path) == 0
}

func (u *URI) IsRef() bool {
	return len(u.Repository) > 0 && len(u.Ref) > 0 && len(u.Path) == 0
}

func (u *URI) IsFullyQualified() bool {
	return len(u.Repository) > 0 && len(u.Ref) > 0 && len(u.Path) > 0
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

	if len(u.Path) == 0 {
		return buf.String()
	}
	buf.WriteRune(PathSeparator)
	buf.WriteString(u.Path)

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
			uri.Path = buf.String()
		}
	}
	return &uri, nil
}

func Equals(a, b *URI) bool {
	return strings.EqualFold(a.Protocol, b.Protocol) &&
		strings.EqualFold(a.Repository, b.Repository) &&
		strings.EqualFold(a.Ref, b.Ref) &&
		strings.EqualFold(a.Path, b.Path)
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
