package merkle

import (
	"fmt"
	"os"
	"strings"
	"text/template"
	"time"
	"treeverse-lake/db"
	"treeverse-lake/ident"
	"treeverse-lake/index/model"
	"treeverse-lake/index/path"
	"treeverse-lake/index/store"

	"golang.org/x/xerrors"
)

type Merkle struct {
	root string
}

func New(root string) *Merkle {
	return &Merkle{root: root}
}

func (m *Merkle) GetEntry(tx store.RepoReadOnlyOperations, pth string, nodeType model.Entry_Type) (*model.Entry, error) {
	currentAddress := m.root
	if len(pth) == 0 && nodeType == model.Entry_TREE {
		return &model.Entry{Address: currentAddress}, nil
	}
	parts := path.New(pth).SplitParts()
	if nodeType == model.Entry_TREE && len(parts[len(parts)-1]) == 0 {
		parts = parts[0 : len(parts)-1]
	}
	var entry *model.Entry
	for i := 1; i <= len(parts); i++ {
		fullPath := path.Join(parts[0:i])
		ent, err := tx.ReadTreeEntry(currentAddress, fullPath)
		if err != nil {
			return entry, err
		}
		currentAddress = ent.GetAddress()
		entry = ent
	}
	return entry, nil
}

func (m *Merkle) GetEntries(tx store.RepoReadOnlyOperations, pth string) ([]*model.Entry, error) {
	entry, err := m.GetEntry(tx, pth, model.Entry_TREE)
	if xerrors.Is(err, db.ErrNotFound) {
		empty := make([]*model.Entry, 0)
		return empty, nil
	}
	res, _, err := tx.ListTree(entry.GetAddress(), "", -1) // request all results
	return res, err
}

func (m *Merkle) GetObject(tx store.RepoReadOnlyOperations, pth string) (*model.Object, error) {
	entry, err := m.GetEntry(tx, pth, model.Entry_OBJECT)
	if err != nil {
		return nil, err
	}
	return tx.ReadObject(entry.GetAddress())
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

type col struct {
	data []*model.Entry
}

func (m *Merkle) PrefixScan(tx store.RepoReadOnlyOperations, prefix, from string, amount int, descend bool) ([]*model.Entry, bool, error) {
	if descend {
		// dfs it
		return m.walk(tx, prefix, from, amount, &col{data: make([]*model.Entry, 0)}, 0)
	}
	pfx := path.New(prefix).SplitParts()
	firstSubtreeAddr := m.root
	firstSubtreePath := prefix
	for {
		entry, err := m.GetEntry(tx, firstSubtreePath, model.Entry_TREE)
		if err == nil {
			firstSubtreeAddr = entry.GetAddress()
			break
		} else if xerrors.Is(err, db.ErrNotFound) {
			if len(pfx) == 1 {
				// no more pops to make
				empty := make([]*model.Entry, 0)
				return empty, false, nil
			}
			// pop the last element
			pfx = pfx[0 : len(pfx)-1]
			firstSubtreePath = path.New(path.Join(pfx)).String()
		} else {
			// actual error
			return nil, false, err
		}
	}
	// got a subtree
	return tx.ListTreeWithPrefix(firstSubtreeAddr, prefix, from, amount)
}

func (m *Merkle) walk(tx store.RepoReadOnlyOperations, prefix, from string, amount int, c *col, depth int) ([]*model.Entry, bool, error) {
	currentFrom := ""
	if len(from) > 0 {
		fromParts := path.New(from).SplitParts()
		if depth >= len(fromParts) {
			currentFrom = from
		} else {
			currentFrom = path.Join(fromParts[0 : depth+1])
		}
	}

	// scan from the root of the tree, every time passing the relevant "from" key that's relevant for the current depth
	entries, hasMore, err := tx.ListTreeWithPrefix(m.root, prefix, currentFrom, amount-len(c.data)) // need no more than that
	if err != nil {
		return nil, false, err
	}

	collectedHasMore := false
	if hasMore {
		collectedHasMore = true // there's more matches that we pulled from storage
	}

	for i, entry := range entries {
		switch entry.GetType() {
		case model.Entry_TREE:
			t := Merkle{root: entry.GetAddress()}
			_, hadMore, err := t.walk(tx, prefix, from, amount, c, depth+1)
			if err != nil {
				return nil, false, err
			}
			if hadMore {
				collectedHasMore = true
			}
		default:
			c.data = append(c.data, entry)
			if len(c.data) == amount {
				// we are full!
				if i < len(entries)-1 {
					collectedHasMore = true
				}
				return c.data, collectedHasMore, nil
			}
		}
	}

	return c.data, collectedHasMore, nil
}

func (m *Merkle) Update(tx store.RepoOperations, entries []*model.WorkspaceEntry) (*Merkle, error) {
	// get the max depth
	changeTree := newChangeTree(entries)
	rootAddr := m.root
	for i := changeTree.MaxDepth(); i >= 0; i-- {
		// get the changes at this depth
		changesAtLevel := changeTree.AtDepth(i)
		for treePath, changes := range changesAtLevel {
			currentEntries, err := m.GetEntries(tx, treePath)
			if err != nil {
				return nil, err
			}
			mergedEntries := mergeChanges(currentEntries, changes)

			parts := path.New(treePath).SplitParts()

			if len(parts) == 1 {
				// this is the root node, write it no matter what and return
				addr, err := m.writeTree(tx, mergedEntries)
				if err != nil {
					return nil, err
				}
				rootAddr = addr
				break // no more changes to make
			}

			// I'm  now getting a parent dir - it's always the non empty part
			// all that's happening here is for the containing directory
			if len(parts[len(parts)-1]) == 0 {
				parts = parts[0 : len(parts)-1]
			}
			parent := path.Join(parts[0 : len(parts)-1]) // all previous parts

			if len(mergedEntries) == 0 {
				// Add a change to the level above us saying this folder should be removed
				changeTree.Add(i-1, parent, &model.WorkspaceEntry{
					Path: treePath,
					Entry: &model.Entry{
						Path: treePath,
						Type: model.Entry_TREE,
					},
					Tombstone: true,
				})
			} else {
				// write tree
				addr, err := m.writeTree(tx, mergedEntries)
				if err != nil {
					return nil, err
				}
				// Add a change to the level above us saying this folder should be updated
				changeTree.Add(i-1, parent, &model.WorkspaceEntry{
					Path: treePath,
					Entry: &model.Entry{
						Path:    treePath,
						Address: addr,
						Type:    model.Entry_TREE,
					},
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

type WalkFn func(path, name, typ string) bool

var format, _ = template.New("treeFormat").Parse("{{.Indent}}{{.Hash}} {{.Type}} {{.Time}} {{.Size}} {{.Name}}\n")

func (m *Merkle) WalkAll(tx store.RepoReadOnlyOperations) {
	_ = format.Execute(os.Stdout, struct {
		Indent string
		Hash   string
		Type   string
		Time   string
		Size   string
		Name   string
	}{
		strings.Repeat("  ", 0),
		m.root[:8],
		model.Entry_TREE.String(),
		time.Now().Format(time.RFC3339),
		fmt.Sprintf("%.10d", 0),
		"\"\"",
	})
	m.walkall(tx, 1, m.root)
}

func (m *Merkle) walkall(tx store.RepoReadOnlyOperations, depth int, root string) {

	children, _, err := tx.ListTree(root, "", -1)
	if err != nil {
		panic(err) // TODO: properly handle errors
	}
	for _, child := range children {
		name := child.GetPath()
		if child.GetType() == model.Entry_TREE {
			name = fmt.Sprintf("%s", name)
		}
		_ = format.Execute(os.Stdout, struct {
			Indent string
			Hash   string
			Type   string
			Time   string
			Size   string
			Name   string
		}{
			strings.Repeat(" - ", depth),
			child.GetAddress()[:8],
			child.GetType().String(),
			time.Unix(child.GetTimestamp(), 0).Format(time.RFC3339),
			fmt.Sprintf("%.10d", child.GetSize()),
			fmt.Sprintf("\"%s\"", name),
		})
		if child.GetType() == model.Entry_TREE {
			m.walkall(tx, depth+1, child.GetAddress())
		}
	}
}
