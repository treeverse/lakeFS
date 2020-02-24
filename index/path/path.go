package path

import (
	"fmt"
	"strings"
)

const Separator = "/"

type Path struct {
	str string
}

func Join(parts []string) string {
	var buf strings.Builder
	for pos, part := range parts {
		buf.WriteString(part)
		if pos != len(parts)-1 && !strings.HasSuffix(part, Separator) {
			// if it's not the last part, and there's no separator at the end, add it
			buf.WriteString(Separator)
		}
	}
	return buf.String()
}

func New(str string) *Path {
	return &Path{str}
}

func (p *Path) HasParent() bool {
	// should return true if the path is not empty (disregarding trailing separator)
	// and the path has a separator splitting at least 2 non empty parts
	return len(p.SplitParts()) > 1
}

func (p *Path) String() string {
	if p == nil {
		return ""
	}
	joined := Join(p.SplitParts())
	return strings.TrimPrefix(joined, Separator)
}

func (p *Path) Basename() string {
	parts := p.SplitParts()
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

func (p *Path) Equals(other *Path) bool {
	if p == nil && other == nil {
		return true
	}
	if other == nil {
		return false
	}
	mine := p.SplitParts()
	theirs := other.SplitParts()
	if len(mine) != len(theirs) {
		return false
	}
	for i, part := range mine {
		if !strings.EqualFold(part, theirs[i]) {
			return false
		}
	}
	return true
}

func (p *Path) SplitParts() []string {
	// trim first / if it exists
	parts := strings.Split(p.str, Separator)
	if len(parts) >= 2 && len(parts[0]) == 0 {
		parts = parts[1:]
	}
	suffixedParts := make([]string, len(parts))
	for i, part := range parts {
		suffixedPart := part
		if i < len(parts)-1 {
			suffixedPart = fmt.Sprintf("%s%s", part, Separator)
		}
		suffixedParts[i] = suffixedPart
	}
	return suffixedParts
}

func (p *Path) BaseName() string {
	var baseName string
	parts := p.SplitParts()
	if len(parts) > 0 {
		baseName = parts[len(parts)-1]
	}
	return baseName
}

func (p *Path) DirName() string {
	var dirName string
	parts := p.SplitParts()
	if len(parts) > 1 && len(parts[len(parts)-1]) == 0 {
		return parts[len(parts)-2]
	}
	return dirName
}

func (p *Path) IsRoot() bool {
	root := New("")
	return p.Equals(root)
}
