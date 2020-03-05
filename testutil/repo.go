package testutil

import (
	"sort"
	"strings"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/merkle"
	"github.com/treeverse/lakefs/index/model"
)

type MockTree struct {
	kv      map[string][]*model.Entry
	roots   map[string]*model.Root
	readErr error
}

func (r *MockTree) WithReadError(err error) *MockTree {
	r.readErr = err
	return r
}

func (r *MockTree) ReadTreeEntry(treeAddress, name string) (*model.Entry, error) {
	if r.readErr != nil {
		return nil, r.readErr
	}
	entries, ok := r.kv[treeAddress]
	if !ok {
		return nil, db.ErrNotFound
	}
	for _, entry := range entries {
		if strings.EqualFold(entry.GetName(), name) {
			return entry, nil
		}
	}
	return nil, db.ErrNotFound
}

func (r *MockTree) ListTree(addr, after string, results int) ([]*model.Entry, bool, error) {
	entries, ok := r.kv[addr]

	if !ok {
		return nil, false, db.ErrNotFound
	}
	sortEntries(entries)
	res := make([]*model.Entry, 0)
	var amount int
	var done, hasMore bool
	for _, entry := range entries {
		if done {
			hasMore = true
			break
		}
		if strings.Compare(entry.GetName(), after) < 1 {
			continue // pagination
		}
		res = append(res, entry)
		amount++
		if amount == results {
			done = true
		}
	}
	return res, hasMore, nil
}
func sortEntries(entries []*model.Entry) []*model.Entry {
	sort.Slice(entries, func(i, j int) bool {
		return merkle.CompareEntries(entries[i], entries[j]) <= 0
	})
	return entries
}

func (r *MockTree) WriteTree(address string, entries []*model.Entry) error {
	r.kv[address] = entries
	sortEntries(r.kv[address])
	return nil
}

func (r *MockTree) WriteRoot(address string, root *model.Root) error {
	r.roots[address] = root
	return nil
}

func (r *MockTree) ReadRoot(address string) (*model.Root, error) {
	if val, ok := r.roots[address]; ok {
		return val, nil
	}
	return nil, db.ErrNotFound
}

func ConstructTree(kv map[string][]*model.Entry) *MockTree {
	roots := make(map[string]*model.Root)
	return &MockTree{kv: kv, roots: roots}
}
