package merkle

import (
	"fmt"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/index/store"

	"golang.org/x/xerrors"
)

type Merkle struct {
	root string
	path string
}

func New(root string) *Merkle {
	return &Merkle{root: root}
}

type TreeReader interface {
	ReadTreeEntry(treeAddress, name string) (*model.Entry, error)
	ListTree(addr, after string, results int) ([]*model.Entry, bool, error)
}

type TreeReaderWriter interface {
	TreeReader
	WriteTree(address string, entries []*model.Entry) error
	WriteRoot(address string, root *model.Root) error
}

func (m *Merkle) GetEntry(tx TreeReader, pth, typ string) (*model.Entry, error) {
	currentAddress := m.root
	if len(pth) == 0 {
		return &model.Entry{Address: currentAddress}, nil
	}
	parsed := path.New(pth)
	parts := parsed.SplitParts()
	if len(parts) > 1 && typ == model.EntryTypeTree && len(parsed.BaseName()) == 0 {
		parts = parts[0 : len(parts)-1]
	}
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
	if xerrors.Is(err, db.ErrNotFound) {
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

func (m *Merkle) writeTree(tx TreeReaderWriter, entries []*model.Entry) (string, int64, error) {
	entryHashes := make([]string, len(entries))
	var size int64
	for i, entry := range entries {
		entryHashes[i] = ident.Hash(entry)
		size += entry.Size
	}
	id := ident.MultiHash(entryHashes...)
	err := tx.WriteTree(id, entries)
	return id, size, err
}

type col struct {
	data []*model.Entry
}

func (m *Merkle) PrefixScan(tx store.RepoOperations, prefix, from string, amount int, descend bool) ([]*model.Entry, bool, error) {
	if descend {
		// dfs it
		return m.walk(tx, prefix, from, amount, &col{data: make([]*model.Entry, 0)}, 0)
	}
	pfx := path.New(prefix).SplitParts()
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
		} else if i == len(pfx)-1 && (xerrors.Is(err, db.ErrNotFound) || entry.EntryType == model.EntryTypeObject) {
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
	pfx = path.New(prefix).SplitParts()
	relativePrefix := ""
	if len(pfx) > 0 {
		relativePrefix = pfx[len(pfx)-1]
	}
	if len(from) > 0 {
		entries, hasMore, err = tx.ListTreeWithPrefix(subtreeAddr, relativePrefix, relativeFrom, amount+1)
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
		entries, hasMore, err = tx.ListTreeWithPrefix(subtreeAddr, relativePrefix, relativeFrom, amount)
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
	if len(from) > 0 {
		fromParts := path.New(from).SplitParts()
		if depth < len(fromParts) {
			currentFrom = fromParts[depth]
		}
	}

	// do the same with prefix
	currentPrefix := ""
	if len(prefix) > 0 {
		prefixParts := path.New(prefix).SplitParts()
		if depth < len(prefixParts) {
			currentPrefix = prefixParts[depth]
		}
	}

	// scan from the root of the tree, every time passing the relevant "from" key that's relevant for the current depth
	// we add 1 to amount since if we received a marker, we explicitly skip it.
	entries, hasMore, err := tx.ListTreeWithPrefix(m.root, currentPrefix, currentFrom, amount-len(c.data)+1) // need no more than that
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
			t := Merkle{root: entry.Address, path: dirPath}
			_, hadMore, err := t.walk(tx, prefix, from, amount, c, depth+1)
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
			mergedEntries, timestamp, err := mergeChanges(currentEntries, changes)
			if err != nil {
				return nil, err
			}
			pth := path.New(treePath)
			parts := pth.SplitParts()

			if len(parts) == 1 {
				// this is the root node, write it no matter what and return
				addr, size, err := m.writeTree(tx, mergedEntries)
				if err != nil {
					return nil, err
				}
				err = tx.WriteRoot(addr, &model.Root{
					Address:      addr,
					CreationDate: timestamp,
					Size:         size,
				})
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
				dirName := pth.DirName()
				typ := model.EntryTypeTree
				changeTree.Add(i-1, parent, &model.WorkspaceEntry{
					Path:              treePath,
					EntryName:         &dirName,
					EntryType:         &typ,
					EntryCreationDate: &timestamp,
					Tombstone:         true,
				})
			} else {
				// write tree
				addr, size, err := m.writeTree(tx, mergedEntries)
				if err != nil {
					return nil, err
				}
				// Add a change to the level above us saying this folder should be updated
				dirName := pth.DirName()
				typ := model.EntryTypeTree
				changeTree.Add(i-1, parent, &model.WorkspaceEntry{
					Path:              treePath,
					EntryName:         &dirName,
					EntryAddress:      &addr,
					EntryType:         &typ,
					EntrySize:         &size,
					EntryCreationDate: &timestamp,
					Tombstone:         false,
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

func (m *Merkle) WalkAll(tx TreeReader) {
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
		model.EntryTypeTree,
		time.Now().Format(time.RFC3339),
		fmt.Sprintf("%.10d", 0),
		"\"\"",
	})
	m.walkall(tx, 1, m.root)
}

func (m *Merkle) walkall(tx TreeReader, depth int, root string) {

	children, _, err := tx.ListTree(root, "", -1)
	if err != nil {
		panic(err) // TODO: properly handle errors
	}
	for _, child := range children {
		name := child.Name
		if child.EntryType == model.EntryTypeTree {
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
			child.Address[:8],
			child.EntryType,
			child.CreationDate.Format(time.RFC3339),
			fmt.Sprintf("%.10d", child.Size),
			fmt.Sprintf("\"%s\"", name),
		})
		if child.EntryType == model.EntryTypeTree {
			m.walkall(tx, depth+1, child.Address)
		}
	}
}
