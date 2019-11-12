package merkle

import (
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

func (m *Merkle) Walk(tx store.RepoReadOnlyOperations, prefix string, maxResults int) {

}
