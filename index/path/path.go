package path

import "strings"

const Separator = '/'

type Path struct {
	str string
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
	return p.str
}

func (p *Path) Pop() (*Path, string) {
	parts := p.SplitParts()
	if len(parts) > 1 {
		return New(strings.Join(parts[:len(parts)-1], string(Separator))), parts[len(parts)-1]
	}
	if len(parts) == 1 {
		return nil, parts[0]
	}
	return nil, ""
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
	parts := make([]string, 0)
	var buf strings.Builder
	separated := true
	for _, current := range p.str {
		if current != Separator {
			buf.WriteRune(current)
			separated = false
			continue
		}
		if !separated {
			parts = append(parts, buf.String())
			buf.Reset()
			separated = true
		}
	}
	if buf.Len() > 0 {
		parts = append(parts, buf.String())
	}
	return parts
}
