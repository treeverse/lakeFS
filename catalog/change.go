package catalog

import (
	"sort"
)

type changeTree struct {
	depth int
	data  map[int]map[string][]*WorkspaceEntry
}

func newChangeTree(entries []*WorkspaceEntry) *changeTree {
	changes := &changeTree{
		data: make(map[int]map[string][]*WorkspaceEntry),
	}
	for _, entry := range entries {
		p := NewPath(entry.Path, *entry.EntryType)
		entry.ObjectCount = 1
		container := p.ParentPath()
		depth := len(p.SplitParts())
		changes.Add(depth, container, entry)
	}
	return changes
}

func (c *changeTree) Add(depth int, path string, chg *WorkspaceEntry) {
	if depth > c.depth {
		c.depth = depth
	}
	if _, exists := c.data[depth]; !exists {
		c.data[depth] = make(map[string][]*WorkspaceEntry)
	}
	if _, exists := c.data[depth][path]; !exists {
		c.data[depth][path] = make([]*WorkspaceEntry, 0)
	}
	c.data[depth][path] = append(c.data[depth][path], chg)
	//TODO: consider change to smart insert
	sort.Slice(c.data[depth][path], func(i, j int) bool {
		return CompareEntries(c.data[depth][path][i].Entry(), c.data[depth][path][j].Entry()) <= 0
	})

}

func (c *changeTree) AtDepth(depth int) map[string][]*WorkspaceEntry {
	paths, exists := c.data[depth]
	if exists {
		return paths
	}
	empty := make(map[string][]*WorkspaceEntry)
	return empty
}

func (c *changeTree) MaxDepth() int {
	return c.depth
}
