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
	pth "treeverse-lake/index/path"
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
	var entry *model.Entry
	for i, part := range parts {
		typ := model.Entry_TREE
		if nodeType == model.Entry_OBJECT && i == len(parts)-1 {
			typ = model.Entry_OBJECT
		}
		ent, err := tx.ReadTreeEntry(currentAddress, part, typ)
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

func (m *Merkle) PrefixScan(tx store.RepoReadOnlyOperations, path, from string, amount int) ([]*model.Entry, bool, error) {
	// let's think about the alogirthm
	// example inputs:
	// - foo/bar (an existing directory)
	// - foo/b (a prefix that has a directory(ies and files) under it
	// - foo/bar/file.json (an existing file)
	// - foo/bar/file.jsonnnnn (nothing under this path)
	// - foo/b.file.json (a file that should also match for foo/b for example, lexicographically before b/)

	// Algorithm 1:
	// 1. start from beginning of the string, take a path part every time and look for directories (i.e. find the deepest tree that can satisfy this query)
	// 2. let's say we found foo/ included in the path, we now reduce that part from the prefix we received
	// 3. we now have (bar, b, file.json, file.jsonnnnn, b.file.json)
	// 4. actually for foo/bar we have (''), since the directory itself is included - recurse through all of it
	// 5. now we've reduced the input to the deepest tree - from here, BFS.
	// 	  For every substree we need to get all children and sort lexicographically ourselves since the dirs and files are sorted independently
	// 6. that intermediate folder where we have a partial match is super annoying because we also need to filter files and dirs by prefix to avoid scanning it all
	// 7. the api should probably change to reflect a more meaningful continuation token, saving some of that work ("from")

	var p *pth.Path
	if len(from) > 0 {
		p = pth.New(from)
	} else {
		p = pth.New(path)
	}
	parts := p.SplitParts()
	prefixParts := make([]string, 0)

	firstSubtreeAddr := m.root
	var firstSubtreePath string

	for i, part := range parts {
		isLast := i == len(parts)-1
		prefixParts = append(prefixParts, part)
		currentPrefix := pth.Join(prefixParts)
		entry, err := m.GetEntry(tx, currentPrefix, model.Entry_TREE)
		if isLast && xerrors.Is(err, db.ErrNotFound) {
			break
		} else if xerrors.Is(err, db.ErrNotFound) && !isLast {
			// we can return an empty response, no such path exists
			return make([]*model.Entry, 0), false, nil
		}
		if err != nil {
			return nil, false, err
		}
		firstSubtreePath = currentPrefix
		firstSubtreeAddr = entry.GetAddress()
	}
	t := Merkle{root: firstSubtreeAddr}
	prefix := strings.TrimPrefix(strings.TrimPrefix(path, firstSubtreePath), string(pth.Separator))
	return t.bfs(tx, prefix, amount, &col{[]*model.Entry{}}, firstSubtreePath)
}

type col struct {
	data []*model.Entry
}

func (m *Merkle) bfs(tx store.RepoReadOnlyOperations, prefix string, amount int, c *col, currentPath string) ([]*model.Entry, bool, error) {
	entries, hasMore, err := tx.ListTreeWithPrefix(m.root, prefix, "", amount)
	if err != nil {
		return nil, false, err
	}
	for _, entry := range entries {
		var fullPath string
		if len(currentPath) > 0 {
			fullPath = pth.Join([]string{currentPath, entry.GetName()})
		} else {
			fullPath = entry.GetName()
		}
		if entry.GetType() == model.Entry_TREE {
			t := Merkle{root: entry.GetAddress()}
			t.bfs(tx, "", amount, c, fullPath)
		} else {
			c.data = append(c.data, &model.Entry{
				Name:      fullPath,
				Address:   entry.GetAddress(),
				Type:      entry.GetType(),
				Timestamp: entry.GetTimestamp(),
				Size:      entry.GetSize(),
				Checksum:  entry.GetChecksum(),
			})
		}
	}
	return c.data, hasMore, nil
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

			if i == 0 {
				// this is the root node, write it no matter what and return
				addr, err := m.writeTree(tx, mergedEntries)
				if err != nil {
					return nil, err
				}
				rootAddr = addr
				break // no more changes to make
			}
			parent, name := path.New(treePath).Pop()
			if len(mergedEntries) == 0 {
				// Add a change to the level above us saying this folder should be removed
				changeTree.Add(i-1, parent.String(), &model.WorkspaceEntry{
					Path: treePath,
					Entry: &model.Entry{
						Name: name,
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
				changeTree.Add(i-1, parent.String(), &model.WorkspaceEntry{
					Path: treePath,
					Entry: &model.Entry{
						Name:    name,
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
		"/",
	})
	m.walk(tx, 1, m.root)
}

func (m *Merkle) walk(tx store.RepoReadOnlyOperations, depth int, root string) {

	children, _, err := tx.ListTree(root, "", -1)
	if err != nil {
		panic(err) // TODO: properly handle errors
	}
	for _, child := range children {
		name := child.GetName()
		if child.GetType() == model.Entry_TREE {
			name = fmt.Sprintf("%s/", name)
		}
		if len(child.GetAddress()) < 6 {
			continue
		}
		_ = format.Execute(os.Stdout, struct {
			Indent string
			Hash   string
			Type   string
			Time   string
			Size   string
			Name   string
		}{
			strings.Repeat("  ", depth),
			child.GetAddress()[:8],
			child.GetType().String(),
			time.Unix(child.GetTimestamp(), 0).Format(time.RFC3339),
			fmt.Sprintf("%.10d", child.GetSize()),
			name,
		})
		if child.GetType() == model.Entry_TREE {
			m.walk(tx, depth+1, child.GetAddress())
		}
	}
}
