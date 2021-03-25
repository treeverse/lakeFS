package block

import (
	"fmt"
	"strings"
)

const (
	Separator = "/"

	EntryTypeTree   = "tree"
	EntryTypeObject = "object"
)

type Path struct {
	str       string
	entryType string
}

var RootPath = NewPath("", EntryTypeTree)

func NewPath(str, entryType string) *Path {
	return &Path{str, entryType}
}

func (p *Path) String() string {
	if p == nil {
		return ""
	}
	joined := JoinPathParts(p.Split())
	return strings.TrimPrefix(joined, Separator)
}

func (p *Path) Equals(other *Path) bool {
	if p == nil && other == nil {
		return true
	}
	if other == nil {
		return false
	}
	if p.entryType != other.entryType {
		return false
	}
	mine := p.Split()
	theirs := other.Split()
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

func (p *Path) Split() []string {
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
	if len(suffixedParts) >= 2 && p.entryType == EntryTypeTree && len(suffixedParts[len(suffixedParts)-1]) == 0 {
		// remove empty suffix for tree type
		suffixedParts = suffixedParts[:len(suffixedParts)-1]
	}
	return suffixedParts
}

func (p *Path) BaseName() string {
	var baseName string
	parts := p.Split()
	if len(parts) > 0 {
		if len(parts) > 1 && len(parts[len(parts)-1]) == 0 && p.entryType == EntryTypeTree {
			baseName = parts[len(parts)-2]
		} else {
			baseName = parts[len(parts)-1]
		}
	}
	return baseName
}

func (p *Path) ParentPath() string {
	if p.IsRoot() {
		return ""
	}
	parts := p.Split()
	if len(parts) <= 1 {
		return ""
	}
	if len(parts[len(parts)-1]) == 0 && p.entryType == EntryTypeTree {
		return JoinPathParts(parts[:len(parts)-2])
	}
	return JoinPathParts(parts[:len(parts)-1])
}

func (p *Path) IsRoot() bool {
	return p.Equals(RootPath)
}

func JoinPathParts(parts []string) string {
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
