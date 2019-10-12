package merkle

import (
	"sort"
	"versio-index/ident"
	"versio-index/index/errors"
	"versio-index/index/model"
	"versio-index/index/path"
	"versio-index/index/store"

	"golang.org/x/xerrors"
)

type ChangeType int

const (
	BlobWritten ChangeType = iota
	BlobDeleted
	TreeWritten
	TreeDeleted
)

type Change struct {
	changeType ChangeType
	name       string
	address    string
}

type Merkle struct {
	tx   store.Transaction
	root string
}

func (m *Merkle) getTreeAddress(pth string) (string, error) {
	currentAddress := m.root
	parts := path.New(pth).SplitParts()
	for _, part := range parts {
		entry, err := m.tx.ReadTreeEntry(currentAddress, part, model.Entry_TREE)
		if err != nil {
			return "", err
		}
		currentAddress = entry.GetAddress()
	}
	return currentAddress, nil
}

func (m *Merkle) getEntries(pth string) ([]*model.Entry, error) {
	addr, err := m.getTreeAddress(pth)
	if xerrors.Is(err, errors.ErrNotFound) {
		empty := make([]*model.Entry, 0)
		return empty, nil
	}
	return m.tx.ListTree(addr)
}

func (m *Merkle) writeTree(entries []*model.Entry) (string, error) {
	entryHashes := make([]string, 0)
	for i, entry := range entries {
		entryHashes[i] = ident.Hash(entry)
	}
	id := ident.MultiHash(entryHashes...)
	err := m.tx.WriteTree(id, entries)
	return id, err
}

func (m *Merkle) Update(changes []*model.WorkspaceEntry) (*Merkle, error) {
	// group changes by containing folder
	dirtyTrees := getDirtyTrees(changes)
	plan(dirtyTrees, changes)
	var lastWrite string
	for _, treePath := range dirtyTrees {
		currentEntries, err := m.getEntries(treePath)
		if err != nil {
			return nil, err
		}
		// merge is a simple merge of sorted lists
		// dirty being the new changes that need to appear in treePath
		newTree := merge(currentEntries, dirty[treePath])
		id, err := m.writeTree(newTree)
		if err != nil {
			return nil, err
		}
		lastWrite = id
	}
	return &Merkle{m.tx, lastWrite}, nil
}

func getDirtyTrees(entries []*model.WorkspaceEntry) (touchSet []string) {
	touchMap := make(map[string]struct{})
	for _, entry := range entries {
		p := path.New(entry.GetPath())
		for {
			treePath, _ := p.Pop()
			touchMap[treePath.String()] = struct{}{}
			if treePath != nil {
				break
			}
		}
	}
	keys := make([]string, len(touchMap))
	i := 0
	for key, _ := range touchMap {
		keys[i] = key
		i++
	}
	// sort by depth (deeper first)
	sort.SliceStable(keys, func(i, j int) bool {
		return len(path.New(keys[i]).SplitParts()) > len(path.New(keys[j]).SplitParts())
	})
	return keys
}
