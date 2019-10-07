package path

import "strings"

const Seperator = "/"

type Path struct {
	str string
}

func New(str string) *Path {
	return &Path{str}
}

func (p *Path) IsDir() bool {

}

func (p *Path) HasParent() bool {

}

func (p *Path) Pop() (*Path, string) {

}

func (p *Path) SplitParts() []string {
	parts := strings.Split(p.str, Seperator)
	nonempty := make([]string, 0)
	for _, part := range parts {
		if strings.EqualFold(part, "") {
			continue
		}
		nonempty = append(nonempty, part)
	}
	return nonempty
}
