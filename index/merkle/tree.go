package merkle

import (
	"errors"
	"strings"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/index/store"
	"github.com/treeverse/lakefs/logging"
)

type Merkle struct {
	root   string
	path   string
	logger logging.Logger
}

func WithLogger(l logging.Logger) func(*Merkle) {
	return func(m *Merkle) {
		m.logger = l
	}
}

func WithPath(path string) func(*Merkle) {
	return func(m *Merkle) {
		m.path = path
	}
}

func New(root string, opts ...func(*Merkle)) *Merkle {
	m := &Merkle{
		root:   root,
		logger: logging.Default(),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type TreeReader interface {
	ReadTreeEntry(treeAddress, name string) (*model.Entry, error)
	ListTree(addr, after string, results int) ([]*model.Entry, bool, error)
}

type TreeReaderWriter interface {
	TreeReader
	WriteTree(address string, entries []*model.Entry) error
}

func (m *Merkle) GetEntry(tx TreeReader, pth, typ string) (*model.Entry, error) {
	currentAddress := m.root
	if len(pth) == 0 && typ == model.EntryTypeTree {
		return &model.Entry{Address: currentAddress}, nil
	}
	parsed := path.New(pth, typ)
	parts := parsed.SplitParts()

	var entry *model.Entry
	for _, part := range parts {
		ent, err := tx.ReadTreeEntry(currentAddress, part)
		if err != nil {
			return entry, err
		}
		currentAddress = ent.Address
		entry = ent
	}
	return entry, nil
}

func (m *Merkle) GetEntries(tx TreeReader, pth string) ([]*model.Entry, error) {
	entry, err := m.GetEntry(tx, pth, model.EntryTypeTree)
	if errors.Is(err, db.ErrNotFound) {
		empty := make([]*model.Entry, 0)
		return empty, nil
	}
	res, _, err := tx.ListTree(entry.Address, "", -1) // request all results
	return res, err
}

func (m *Merkle) GetObject(tx store.RepoOperations, pth string) (*model.Object, error) {
	entry, err := m.GetEntry(tx, pth, model.EntryTypeObject)
	if err != nil {
		return nil, err
	}
	return tx.ReadObject(entry.Address)
}

func (m *Merkle) writeTree(tx TreeReaderWriter, entries []*model.Entry) (string, int, error) {
	entryHashes := make([]string, len(entries))
	var objectCount int
	for i, entry := range entries {
		entryHashes[i] = ident.Hash(entry)
		objectCount += entry.ObjectCount
	}
	id := ident.MultiHash(entryHashes...)
	err := tx.WriteTree(id, entries)
	return id, objectCount, err
}

type col struct {
	data []*model.Entry
}

func (m *Merkle) PrefixScan(tx store.RepoOperations, prefix, from string, amount int, descend bool) ([]*model.Entry, bool, error) {
	if descend {
		// dfs it
		return m.walk(tx, prefix, from, amount, &col{data: make([]*model.Entry, 0)}, 0)
	}
	pfx := path.New(prefix, model.EntryTypeObject).SplitParts()
	subtreeAddr := m.root
	subtreePath := ""
	for i, part := range pfx {
		entry, err := tx.ReadTreeEntry(subtreeAddr, part)
		if err == nil && entry.EntryType == model.EntryTypeTree {
			subtreeAddr = entry.Address
			if len(subtreePath) == 0 {
				subtreePath = part
			} else {
				subtreePath = path.Join([]string{subtreePath, part})
			}
		} else if i == len(pfx)-1 && (errors.Is(err, db.ErrNotFound) || entry.EntryType == model.EntryTypeObject) {
			// if this is the last part, it's ok if it's not found, its a prefix.
			// otherwise, there's nothing here.
			break
		} else {
			// actual error
			return nil, false, err
		}
	}

	var entries []*model.Entry
	var hasMore bool
	var err error

	// got a subtree
	relativeFrom := strings.TrimPrefix(from, prefix) // only the relative portion
	pfx = path.New(prefix, model.EntryTypeObject).SplitParts()
	relativePrefix := ""
	if len(pfx) > 0 {
		relativePrefix = pfx[len(pfx)-1]
	}
	if len(from) > 0 {
		entries, hasMore, err = tx.ListTreeWithPrefix(subtreeAddr, relativePrefix, relativeFrom, amount, false)
		if err != nil {
			return nil, false, err
		}
		// if we have entries and the first one == from
		if len(entries) > 0 && strings.EqualFold(entries[0].Name, relativeFrom) {
			// the API means we need to skip the "from" path
			if len(entries) > 1 {
				entries = entries[1:]
			} else {
				entries = []*model.Entry{}
			}
		}
	} else {
		entries, hasMore, err = tx.ListTreeWithPrefix(subtreeAddr, relativePrefix, relativeFrom, amount, false)
		if err != nil {
			return nil, false, err
		}
	}
	// assemble response as full path
	if len(subtreePath) > 0 {
		for _, entry := range entries {
			entry.Name = path.Join([]string{subtreePath, entry.Name})
		}
	}

	// return
	return entries, hasMore, err
}

func (m *Merkle) walk(tx store.RepoOperations, prefix, from string, amount int, c *col, depth int) ([]*model.Entry, bool, error) {
	currentFrom := ""
	fromInclusive := true
	if len(from) > 0 {
		fromParts := path.New(from, model.EntryTypeObject).SplitParts()
		if depth < len(fromParts) {
			currentFrom = fromParts[depth]
		}
		fromInclusive = depth < len(fromParts)-1 // until we reach the deepest path, we need to include the part in the result
	}

	// do the same with prefix
	currentPrefix := ""
	if len(prefix) > 0 {
		prefixParts := path.New(prefix, model.EntryTypeObject).SplitParts()
		if depth < len(prefixParts) {
			currentPrefix = prefixParts[depth]
		}
	}

	// scan from the root of the tree, every time passing the relevant "from" key that's relevant for the current depth
	// we add 1 to amount since if we received a marker, we explicitly skip it.
	entries, hasMore, err := tx.ListTreeWithPrefix(m.root, currentPrefix, currentFrom, amount-len(c.data)+1, fromInclusive) // need no more than that
	if err != nil {
		return nil, false, err
	}
	collectedHasMore := false
	if hasMore {
		collectedHasMore = true // there's more matches that we pulled from storage
	}

	for i, entry := range entries {
		switch entry.EntryType {
		case model.EntryTypeTree:
			dirPath := entry.Name
			if depth > 0 {
				dirPath = path.Join([]string{m.path, entry.Name})
			}
			t := New(entry.Address, WithLogger(m.logger), WithPath(dirPath))
			recursionFrom := from
			if entry.Name >= from {
				recursionFrom = ""
			}
			_, hadMore, err := t.walk(tx, prefix, recursionFrom, amount, c, depth+1)
			if err != nil {
				return nil, false, err
			}
			if hadMore {
				collectedHasMore = true
			}
		default:
			// reassemble path
			if len(m.path) > 0 {
				entry.Name = path.Join([]string{m.path, entry.Name})
			}
			if strings.EqualFold(entry.Name, from) {
				continue // we skip the marker
			}
			c.data = append(c.data, entry)
		}
		if len(c.data) == amount {
			// we are full!
			if i < len(entries)-1 {
				collectedHasMore = true
			}
			return c.data, collectedHasMore, nil
		}
	}

	return c.data, collectedHasMore, nil
}

func (m *Merkle) Update(tx TreeReaderWriter, entries []*model.WorkspaceEntry) (*Merkle, error) {
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
			lg := m.logger.WithFields(logging.Fields{
				"merge_depth": i,
				"merge_path":  treePath,
			})
			mergedEntries, err := mergeChanges(currentEntries, changes, lg)
			if err != nil {
				return nil, err
			}
			pth := path.New(treePath, model.EntryTypeTree)
			parts := pth.SplitParts()

			if pth.IsRoot() {
				// this is the root node, write it no matter what and return
				addr, _, err := m.writeTree(tx, mergedEntries)
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
				dirName := pth.BaseName()
				typ := model.EntryTypeTree
				changeTree.Add(i-1, parent, &model.WorkspaceEntry{
					Path:              treePath,
					EntryName:         &dirName,
					EntryType:         &typ,
					EntryCreationDate: nil,
					Tombstone:         true,
				})
			} else {
				// write tree
				addr, objectCount, err := m.writeTree(tx, mergedEntries)
				if err != nil {
					return nil, err
				}
				// Add a change to the level above us saying this folder should be updated
				dirName := pth.BaseName()
				typ := model.EntryTypeTree
				changeTree.Add(i-1, parent, &model.WorkspaceEntry{
					Path:              treePath,
					EntryName:         &dirName,
					EntryAddress:      &addr,
					EntryType:         &typ,
					EntrySize:         nil,
					EntryCreationDate: nil,
					Tombstone:         false,
					ObjectCount:       objectCount,
				})
			}
		}
	}
	return New(rootAddr, WithLogger(m.logger)), nil
}

func (m *Merkle) Root() string {
	return m.root
}
