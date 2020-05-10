package merkle

import (
	"errors"
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
			pth := path.New(treePath, model.EntryTypeTree)
			parts := pth.SplitParts()

			if pth.IsRoot() {
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
				dirName := pth.BaseName()
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
				dirName := pth.BaseName()
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
