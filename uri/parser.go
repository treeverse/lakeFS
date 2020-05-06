package uri

import (
	"errors"
	"strings"
)

const (
	ProtocolSeparator = "://"
	LakeFSProtocol    = "lakefs"

	RefSeparator  = '@'
	PathSeparator = '/'

	stateInRepo = iota
	stateInRef
	stateInPath
)

var (
	ErrMalformedURI = errors.New("malformed lakefs uri")
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
	var uri URI

	// start with protocol
	protoParts := strings.Split(str, ProtocolSeparator)
	if len(protoParts) != 2 {
		return nil, ErrMalformedURI
	}
	if !strings.EqualFold(protoParts[0], LakeFSProtocol) {
		return nil, ErrMalformedURI
	}
	uri.Protocol = protoParts[0]

	var state = stateInRepo
	var buf strings.Builder
	for _, ch := range protoParts[1] {
		if ch == RefSeparator && state == stateInRepo {
			uri.Repository = buf.String()
			state = stateInRef
			buf.Reset()
		} else if ch == PathSeparator && state == stateInRef {
			uri.Ref = buf.String()
			state = stateInPath
			buf.Reset()
		} else {
			buf.WriteRune(ch)
		}
	}
	if buf.Len() > 0 && state == stateInRepo {
		uri.Repository = buf.String()
	} else if buf.Len() > 0 && state == stateInRef {
		uri.Ref = buf.String()
	} else if buf.Len() > 0 && state == stateInPath {
		uri.Path = buf.String()
	}
	return &uri, nil
}

func Equals(a, b *URI) bool {
	return strings.EqualFold(a.Protocol, b.Protocol) &&
		strings.EqualFold(a.Repository, b.Repository) &&
		strings.EqualFold(a.Ref, b.Ref) &&
		strings.EqualFold(a.Path, b.Path)
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
