package path

import "strings"

const Separator = "/"

type Path struct {
	str string
}

func Join(parts []string) string {
	return strings.Join(parts, Separator)
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

func (p *Path) Pop() (*Path, string) {
	parts := p.SplitParts()
	if len(parts) > 1 {
		return New(Join(parts[:len(parts)-1])), parts[len(parts)-1]
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
	// trim first / if it exists
	parts := strings.Split(p.str, Separator)
	if len(parts) >= 2 && len(parts[0]) == 0 {
		return parts[1:]
	}
	return parts
}

func (p *Path) Add(child string) *Path {
	return New(Join(append(p.SplitParts(), New(child).SplitParts()...)))

}
