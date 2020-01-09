package merkle

import (
	"treeverse-lake/ident"
	"treeverse-lake/index/model"
	"treeverse-lake/index/path"
)

type change struct {
	Type      model.Entry_Type
	Name      string
	Address   string
	Tombstone bool
	Object    *model.Object
}

func (c *change) GetType() model.Entry_Type {
	return c.Type
}

func (c *change) GetName() string {
	return c.Name
}

func (c *change) GetAddress() string {
	return c.Address
}

func (c *change) AsEntry() *model.Entry {
	if c.Tombstone || c.Type == model.Entry_TREE {
		return &model.Entry{
			Name:    c.Name,
			Address: c.Address,
			Type:    c.Type,
		}
	}
	// for object writes we also include the model's size and timestamp
	return &model.Entry{
		Name:      c.Name,
		Address:   c.Address,
		Type:      c.Type,
		Timestamp: c.Object.GetTimestamp(),
		Size:      c.Object.GetSize(),
		Checksum:  c.Object.GetBlob().GetChecksum(),
	}
}

type changeTree struct {
	depth int
	data  map[int]map[string][]*change
}

func newChangeTree(entries []*model.WorkspaceEntry) *changeTree {
	changes := &changeTree{
		data: make(map[int]map[string][]*change),
	}
	for _, entry := range entries {
		var chg *change
		p := path.New(entry.GetPath())
		container, name := p.Pop()
		depth := len(p.SplitParts()) - 1
		if entry.GetTombstone() != nil {
			chg = &change{
				Type:      model.Entry_OBJECT,
				Name:      name,
				Tombstone: true,
			}
		} else {
			chg = &change{
				Type:      model.Entry_OBJECT,
				Name:      name,
				Address:   ident.Hash(entry.GetObject()),
				Object:    entry.GetObject(),
				Tombstone: false,
			}
		}
		changes.Add(depth, container.String(), chg)
	}
	return changes
}

func (c *changeTree) Add(depth int, path string, chg *change) {
	if depth > c.depth {
		c.depth = depth
	}
	if _, exists := c.data[depth]; !exists {
		c.data[depth] = make(map[string][]*change)
	}
	if _, exists := c.data[depth][path]; !exists {
		c.data[depth][path] = make([]*change, 0)
	}
	c.data[depth][path] = append(c.data[depth][path], chg)
}

func (c *changeTree) AtDepth(depth int) map[string][]*change {
	paths, exists := c.data[depth]
	if exists {
		return paths
	}
	empty := make(map[string][]*change)
	return empty
}

func (c *changeTree) MaxDepth() int {
	return c.depth
}
