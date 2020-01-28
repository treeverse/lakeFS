package merkle

import (
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/path"
)

type changeTree struct {
	depth int
	data  map[int]map[string][]*model.WorkspaceEntry
}

func newChangeTree(entries []*model.WorkspaceEntry) *changeTree {
	changes := &changeTree{
		data: make(map[int]map[string][]*model.WorkspaceEntry),
	}
	for _, entry := range entries {
		p := path.New(entry.GetPath())
		parts := p.SplitParts()
		container := path.Join(parts[0 : len(parts)-1])
		depth := len(p.SplitParts())
		changes.Add(depth, container, entry)
	}
	return changes
}

func (c *changeTree) Add(depth int, path string, chg *model.WorkspaceEntry) {
	if depth > c.depth {
		c.depth = depth
	}
	if _, exists := c.data[depth]; !exists {
		c.data[depth] = make(map[string][]*model.WorkspaceEntry)
	}
	if _, exists := c.data[depth][path]; !exists {
		c.data[depth][path] = make([]*model.WorkspaceEntry, 0)
	}
	c.data[depth][path] = append(c.data[depth][path], chg)
}

func (c *changeTree) AtDepth(depth int) map[string][]*model.WorkspaceEntry {
	paths, exists := c.data[depth]
	if exists {
		return paths
	}
	empty := make(map[string][]*model.WorkspaceEntry)
	return empty
}

func (c *changeTree) MaxDepth() int {
	return c.depth
}
