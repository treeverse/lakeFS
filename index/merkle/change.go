package merkle

import (
	"sort"

	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/path"
)

type changeTree struct {
	depth int
	data  map[int]map[string][]*model.WorkspaceEntry
}

// returns the container depth and path
func getContainer(entry *model.WorkspaceEntry) (int, string) {
	p := path.New(entry.GetPath())
	parts := p.SplitParts()
	var container string
	if entry.GetEntry().GetType() == model.Entry_TREE {
		container = path.Join(parts[0 : len(parts)-2])
	} else {
		container = path.Join(parts[0 : len(parts)-1])
	}
	depth := len(p.SplitParts())
	return depth, container
}

func newChangeTree(entries []*model.WorkspaceEntry) *changeTree {
	changes := &changeTree{
		data: make(map[int]map[string][]*model.WorkspaceEntry),
	}
	for _, entry := range entries {
		depth, container := getContainer(entry)
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
	//TODO: consider change to smart insert
	sort.Slice(c.data[depth][path], func(i, j int) bool {
		return CompareEntries(c.data[depth][path][i].Entry, c.data[depth][path][j].Entry) <= 0
	})

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
