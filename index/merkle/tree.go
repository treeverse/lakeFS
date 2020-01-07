package merkle

import (
	"fmt"
	"strings"
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

func (m *Merkle) GetAddress(tx store.RepoReadOnlyOperations, pth string, nodeType model.Entry_Type) (string, error) {
	currentAddress := m.root
	parts := path.New(pth).SplitParts()
	for i, part := range parts {
		typ := model.Entry_TREE
		if nodeType == model.Entry_OBJECT && i == len(parts)-1 {
			typ = model.Entry_OBJECT
		}
		entry, err := tx.ReadTreeEntry(currentAddress, part, typ)
		if err != nil {
			return "", err
		}
		currentAddress = entry.GetAddress()
	}
	return currentAddress, nil
}

func (m *Merkle) GetEntries(tx store.RepoReadOnlyOperations, pth string) ([]*model.Entry, error) {
	addr, err := m.GetAddress(tx, pth, model.Entry_TREE)
	if xerrors.Is(err, db.ErrNotFound) {
		empty := make([]*model.Entry, 0)
		return empty, nil
	}
	res, _, err := tx.ListTree(addr, "", -1) // request all results
	return res, err
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

	for _, part := range parts {
		prefixParts = append(prefixParts, part)
		currentPrefix := pth.Join(prefixParts)
		addr, err := m.GetAddress(tx, currentPrefix, model.Entry_TREE)
		if xerrors.Is(err, db.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, false, err
		}
		firstSubtreePath = currentPrefix
		firstSubtreeAddr = addr
	}
	t := Merkle{root: firstSubtreeAddr}
	return t.bfs(tx, strings.TrimPrefix(path, firstSubtreePath), amount, &col{[]*model.Entry{}}, p.String())
}

type col struct {
	data []*model.Entry
}

func (m *Merkle) bfs(tx store.RepoReadOnlyOperations, prefix string, amount int, c *col, currentPath string) ([]*model.Entry, bool, error) {
	//fmt.Printf("doing bfs - path = %s ->(collected so far: %v)\n", currentPath, c.data)
	entries, hasMore, err := tx.ListTree(m.root, prefix, amount)
	if err != nil {
		return nil, false, err
	}
	for _, entry := range entries {
		fullPath := pth.Join([]string{currentPath, entry.GetName()})
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
			fmt.Printf("added %s to collected\n", fullPath)
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
				// TODO: we need to add a change to the level above us saying this folder should be removed
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
