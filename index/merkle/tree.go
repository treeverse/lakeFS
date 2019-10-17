package merkle

import (
	"fmt"
	"strings"
	"versio-index/ident"
	"versio-index/index/errors"
	"versio-index/index/model"
	"versio-index/index/path"
	"versio-index/index/store"

	"golang.org/x/xerrors"
)

type entryLike interface {
	GetType() model.Entry_Type
	GetName() string
	GetAddress() string
}

type change struct {
	Type      model.Entry_Type
	Name      string
	Address   string
	Tombstone bool
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
	return &model.Entry{
		Name:    c.GetName(),
		Address: c.GetAddress(),
		Type:    c.GetType(),
	}
}

type changeTree struct {
	depth int
	data  map[int]map[string][]*change
}

func newChangeTree(entries []*model.WorkspaceEntry) *changeTree {
	changes := &changeTree{
		depth: 0,
		data:  make(map[int]map[string][]*change),
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
				Address:   entry.GetAddress(),
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

type Merkle struct {
	root string
}

func New(root string) *Merkle {
	return &Merkle{root: root}
}

func (m *Merkle) GetAddress(tx store.RepoReadOnlyOperations, pth string, nodeType model.Entry_Type) (string, error) {
	currentAddress := m.root
	parts := path.New(pth).SplitParts()

	for i, part := range parts {
		fmt.Printf("getting part %d (%s) of %s (%d)\n", i, part, parts, len(parts))
		typ := model.Entry_TREE
		if nodeType == model.Entry_OBJECT && i == len(parts)-1 {
			typ = model.Entry_OBJECT
		}
		entry, err := tx.ReadTreeEntry(currentAddress, part, typ)
		if err != nil {
			return "", err
		}
		currentAddress = entry.GetAddress()
		fmt.Printf("got entry: %s (addr = %s)\n", part, currentAddress)
	}
	return currentAddress, nil
}

func (m *Merkle) GetEntries(tx store.RepoReadOnlyOperations, pth string) ([]*model.Entry, error) {
	addr, err := m.GetAddress(tx, pth, model.Entry_TREE)
	if xerrors.Is(err, errors.ErrNotFound) {
		empty := make([]*model.Entry, 0)
		return empty, nil
	}
	return tx.ListTree(addr, "", -1)
}

func (m *Merkle) GetObject(tx store.RepoReadOnlyOperations, pth string) (*model.Object, error) {
	addr, err := m.GetAddress(tx, pth, model.Entry_OBJECT)
	if err != nil {
		return nil, err
	}
	return tx.ReadObject(addr)
}

func (m *Merkle) writeTree(tx store.RepoOperations, entries []*model.Entry) (string, error) {
	entryHashes := make([]string, len(entries))
	for i, entry := range entries {
		entryHashes[i] = ident.Hash(entry)
	}
	id := ident.MultiHash(entryHashes...)
	err := tx.WriteTree(id, entries)
	return id, err
}

func (m *Merkle) Update(tx store.RepoOperations, entries []*model.WorkspaceEntry) (*Merkle, error) {

	// get the max depth
	changeTree := newChangeTree(entries)
	var rootAddr string
	for i := changeTree.MaxDepth(); i >= 0; i-- {
		// get the changes at this depth
		changesAtLevel := changeTree.AtDepth(i)
		for treePath, changes := range changesAtLevel {
			currentEntries, err := m.GetEntries(tx, treePath)
			if err != nil {
				return nil, err
			}
			mergedEntries := mergeChanges(currentEntries, changes)

			if i == 0 {
				// this is the root node, write it no matter what and return
				addr, err := m.writeTree(tx, mergedEntries)
				if err != nil {
					return nil, err
				}
				rootAddr = addr
			}
			parent, name := path.New(treePath).Pop()
			if len(mergedEntries) == 0 {
				// we need to add a change to the level above us saying this folder should be removed
				// TODO
				changeTree.Add(i-1, parent.String(), &change{
					Type:      model.Entry_TREE,
					Name:      name,
					Tombstone: true,
				})
			} else {
				// write tree
				addr, err := m.writeTree(tx, mergedEntries)
				if err != nil {
					return nil, err
				}
				changeTree.Add(i-1, parent.String(), &change{
					Type:      model.Entry_TREE,
					Name:      name,
					Address:   addr,
					Tombstone: false,
				})
			}
		}
	}
	return &Merkle{root: rootAddr}, nil
}

func (m *Merkle) Root() string {
	return m.root
}

func compareEntries(a, b entryLike) int {
	// directories first
	if a.GetType() != b.GetType() {
		if a.GetType() < b.GetType() {
			return -1
		} else if a.GetType() > b.GetType() {
			return 1
		}
		return 0
	}
	// then sort by name
	return strings.Compare(a.GetName(), b.GetName())
}

func mergeChanges(current []*model.Entry, changes []*change) []*model.Entry {
	merged := make([]*model.Entry, 0)
	nextCurrent := 0
	nextChange := 0
	for {
		// if both lists still have values, compare
		if nextChange < len(changes) && nextCurrent < len(current) {
			currEntry := current[nextCurrent]
			currChange := changes[nextChange]
			comparison := compareEntries(currEntry, currChange)
			if comparison == 0 {
				// this is an override or deletion

				// overwrite
				if !currChange.Tombstone {
					merged = append(merged, currChange.AsEntry())
				}
				// otherwise, skip both
				nextCurrent++
				nextChange++
			} else if comparison == -1 {
				nextCurrent++
				// current entry comes first
				merged = append(merged, currEntry)
			} else {
				nextChange++
				// changed entry comes first
				merged = append(merged, currChange.AsEntry())
			}
		} else if nextChange < len(changes) {
			// only changes left
			currChange := changes[nextChange]
			if currChange.Tombstone {
				// this is an override or deletion
				nextChange++
				continue // remove.
			}
			merged = append(merged, currChange.AsEntry())
			nextChange++
		} else if nextCurrent < len(current) {
			// only current entries left
			currEntry := current[nextCurrent]
			merged = append(merged, currEntry)
			nextCurrent++
		} else {
			// done with both
			break
		}
	}
	return merged
}
