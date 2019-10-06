package index

import (
	"versio-index/index/model"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type ReadQuery interface {
	ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error)
	ReadBranch(branch string) (*model.Branch, error)
	ReadBlob(addr string) (*model.Blob, error)
	ReadTree(addr string) (*model.Tree, error)
	ReadCommit(addr string) (*model.Commit, error)
	ListEntries(addr string) ([]*model.Entry, error)
	ReadEntry(treeAddress, entryType, name string) (*model.Entry, error)
}

type Query interface {
	ReadQuery
	WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error
	ClearWorkspace(branch string)
	WriteTree(addr string, tree *model.Tree) error
	WriteEntry(treeAddress, entryType, name string, entry *model.Entry) error
	WriteBlob(addr string, blob *model.Blob) error
	WriteCommit(addr string, commit *model.Commit) error
	WriteBranch(name string, branch *model.Branch) error
	DeleteBranch(name string)
}

type spaces struct {
	workspace subspace.Subspace // store WorkspaceEntry objects per branch
	trees     subspace.Subspace // stores tree metadata objects
	entries   subspace.Subspace // enumerates tree entries (blobs and other trees, directories first)
	blobs     subspace.Subspace // stores simple blobs
	commits   subspace.Subspace // store commit objects
	branches  subspace.Subspace // store branch pointer and metadata
	refCounts subspace.Subspace // trie reference counts
}

type readQuery struct {
	*spaces
	repo *model.Repo

	tx fdb.ReadTransaction
}

type query struct {
	*readQuery
	tx fdb.Transaction
}

func (q *readQuery) pack(space subspace.Subspace, parts ...tuple.TupleElement) fdb.Key {
	parts = append(tuple.Tuple{q.repo.GetClientId(), q.repo.GetRepoId()}, parts...)
	return space.Pack(parts)
}

func (q *readQuery) get(space subspace.Subspace, parts ...tuple.TupleElement) fdb.FutureByteSlice {
	return q.tx.Get(q.pack(space, parts...))
}

func (q *readQuery) rangePrefix(space subspace.Subspace, parts ...tuple.TupleElement) *fdb.RangeIterator {
	begin := q.pack(space, parts...)
	return q.tx.GetRange(fdb.KeyRange{
		Begin: begin,
		End:   append(begin, 0xFF),
	}, fdb.RangeOptions{}).Iterator()
}

func (q *query) set(data []byte, space subspace.Subspace, parts ...tuple.TupleElement) {
	q.tx.Set(q.pack(space, parts...), data)
}
func (q *query) clearPrefix(space subspace.Subspace, parts ...tuple.TupleElement) {
	begin := q.pack(space, parts...)
	end := append(begin, 0xFF)
	q.tx.ClearRange(&fdb.KeyRange{
		Begin: begin,
		End:   end,
	})
}

func (q *query) delete(space subspace.Subspace, parts ...tuple.TupleElement) {
	q.tx.Clear(q.pack(space, parts...))
}
