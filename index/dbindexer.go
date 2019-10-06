package index

import (
	"math/rand"
	"strings"
	"time"
	"versio-index/index/model"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"golang.org/x/xerrors"
)

const (
	PathSeparator        = "/"
	DefaultDirectoryName = "md_index"
)

func writeEntryToWorkspace(q Query, repo *model.Repo, branch, path string, entry *model.WorkspaceEntry) error {
	err := q.WriteToWorkspacePath(branch, path, entry)
	if err != nil {
		return err
	}
	if shouldPartiallyCommit(repo) {
		_, err = partialCommit(q, repo, branch)
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveReadRoot(q ReadQuery, repo *model.Repo, branch string) (string, error) {
	var empty string
	branchData, err := q.ReadBranch(branch)
	if xerrors.Is(err, ErrNotFound) {
		// fallback to default branch
		branchData, err = q.ReadBranch(repo.DefaultBranch)
		if err != nil {
			return empty, err
		}
	} else if err != nil {
		return empty, err // unexpected error
	}
	if strings.EqualFold(branchData.GetWorkspaceRoot(), "") {
		return branchData.GetCommitRoot(), nil
	}
	return branchData.GetWorkspaceRoot(), nil
}

func readFromTree(q ReadQuery, repo *model.Repo, branch, path string) (*model.Object, error) {
	// resolve tree root to read from
	root, err := resolveReadRoot(q, repo, branch)
	if err != nil {
		return nil, err
	}
	// get the tree
	return traverse(q, root, path)
}

func traverse(q ReadQuery, treeID, path string) (*model.Object, error) {
	currentAddress := treeID
	parts := strings.Split(path, PathSeparator)
	for i, part := range parts {
		if i == len(parts)-1 {
			// last item in the path is the blob
			entry, err := q.ReadEntry(currentAddress, "f", part)
			if err != nil {
				return nil, err
			}
			blob, err := q.ReadBlob(entry.GetAddress())
			if err != nil {
				return nil, err
			}
			return &model.Object{
				Blob:     blob,
				Metadata: entry.GetMetadata(),
			}, nil
		}
		entry, err := q.ReadEntry(currentAddress, "d", part)
		if err != nil {
			return nil, err
		}
		currentAddress = entry.GetAddress()
	}
	return nil, ErrNotFound
}

func traverseDir(q ReadQuery, treeID, path string) (string, error) {
	currentAddress := treeID
	parts := strings.Split(path, PathSeparator)
	for _, part := range parts {
		entry, err := q.ReadEntry(currentAddress, "d", part)
		if err != nil {
			return "", err
		}
		currentAddress = entry.GetAddress()
	}
	return currentAddress, nil
}

func shouldPartiallyCommit(repo *model.Repo) bool {
	chosen := rand.Float32()
	return chosen < repo.GetPartialCommitRatio()
}

func partialCommit(q Query, repo *model.Repo, branch string) (string, error) {
	// 1. iterate all changes in the current workspace
	// 2. Apply them to the Merkle root as exists in the branch pointer
	// 3. calculate new Merkle root
	// 4. save it in the branch pointer
	return "", nil
}

func gc(q Query, treeAddress string) {

}

func commitHash(commit *model.Commit) string {
	return "foo" // TODO: add real implementations to tree and commit
}

func treeHash(tree *model.Tree) string {
	return "bar"
}

type DBIndex struct {
	database  fdb.Database
	workspace subspace.Subspace
	trees     subspace.Subspace
	entries   subspace.Subspace
	blobs     subspace.Subspace
	commits   subspace.Subspace
	branches  subspace.Subspace
	refCounts subspace.Subspace
}

func NewDBIndex(db fdb.Database) (*DBIndex, error) {
	dir, err := directory.CreateOrOpen(db, []string{DefaultDirectoryName}, nil)
	if err != nil {
		return nil, err
	}
	return &DBIndex{
		db,
		dir.Sub("workspace"),
		dir.Sub("trees"),
		dir.Sub("entries"),
		dir.Sub("blobs"),
		dir.Sub("commits"),
		dir.Sub("branches"),
		dir.Sub("refCounts"),
	}, nil
}

func (db *DBIndex) ReadQuery(repo *model.Repo, tx fdb.ReadTransaction) ReadQuery {
	return &readQuery{
		workspace: db.workspace,
		trees:     db.trees,
		entries:   db.entries,
		blobs:     db.blobs,
		commits:   db.commits,
		branches:  db.branches,
		refCounts: db.refCounts,
		repo:      repo,
		tx:        tx,
	}
}

func (db *DBIndex) Query(repo *model.Repo, tx fdb.Transaction) Query {
	return &query{
		readQuery: &readQuery{
			workspace: db.workspace,
			trees:     db.trees,
			entries:   db.entries,
			blobs:     db.blobs,
			commits:   db.commits,
			branches:  db.branches,
			refCounts: db.refCounts,
			repo:      repo,
		},
		tx: tx,
	}
}

func (db *DBIndex) ReadTransact(repo *model.Repo, fn func(ReadQuery) (interface{}, error)) (interface{}, error) {
	return db.database.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		q := db.ReadQuery(repo, tx)
		return fn(q)
	})
}

func (db *DBIndex) Transact(repo *model.Repo, fn func(Query) (interface{}, error)) (interface{}, error) {
	return db.database.Transact(func(tx fdb.Transaction) (interface{}, error) {
		q := db.Query(repo, tx)
		return fn(q)
	})
}

// Business logic

func (db *DBIndex) Read(repo *model.Repo, branch, path string) (*model.Object, error) {
	obj, err := db.ReadTransact(repo, func(q ReadQuery) (interface{}, error) {
		var obj *model.Object
		we, err := q.ReadFromWorkspace(branch, path)
		if err != nil && !xerrors.Is(err, ErrNotFound) {
			// an actual error has occurred, return it.
			return nil, err
		}
		if we.GetTombstone() != nil {
			// object was deleted deleted
			return nil, ErrNotFound
		}
		if xerrors.Is(err, ErrNotFound) {
			// not in workspace, let's try reading it from branch tree
			obj, err = readFromTree(q, repo, branch, path)
		}
		// we found an object in the workspace
		obj = we.GetObject()
		return obj, nil
	})
	if err != nil {
		return nil, err
	}
	return obj.(*model.Object), nil
}

func (db *DBIndex) Write(repo *model.Repo, branch, path string, object *model.Object) error {
	_, err := db.Transact(repo, func(q Query) (interface{}, error) {
		err := writeEntryToWorkspace(q, repo, branch, path, &model.WorkspaceEntry{
			Data: &model.WorkspaceEntry_Object{Object: object},
		})
		return nil, err
	})
	return err
}

func (db *DBIndex) Delete(repo *model.Repo, branch, path string) error {
	_, err := db.Transact(repo, func(q Query) (interface{}, error) {
		err := writeEntryToWorkspace(q, repo, branch, path, &model.WorkspaceEntry{
			Data: &model.WorkspaceEntry_Tombstone{Tombstone: &model.Tombstone{}},
		})
		return nil, err
	})
	return err
}

func (db *DBIndex) List(repo *model.Repo, branch, path string) ([]*model.Entry, error) {
	entries, err := db.Transact(repo, func(q Query) (interface{}, error) {
		_, err := partialCommit(q, repo, branch)
		if err != nil {
			return nil, err
		}

		root, err := resolveReadRoot(q, repo, branch)
		if err != nil {
			return nil, err
		}
		addr, err := traverseDir(q, root, path)
		if err != nil {
			return nil, err
		}
		return q.ListEntries(addr)
	})
	if err != nil {
		return nil, err
	}
	return entries.([]*model.Entry), nil
}

func (db *DBIndex) Reset(repo *model.Repo, branch string) error {
	// clear workspace, set branch workspace root back to commit root
	_, err := db.Transact(repo, func(q Query) (interface{}, error) {
		q.ClearWorkspace(branch)
		branchData, err := q.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		gc(q, branchData.GetWorkspaceRoot())
		branchData.WorkspaceRoot = branchData.GetCommitRoot()
		return nil, q.WriteBranch(branch, branchData)
	})
	return err
}

func (db *DBIndex) Commit(repo *model.Repo, branch, message, committer string, metadata map[string]string) error {
	_, err := db.Transact(repo, func(q Query) (interface{}, error) {
		root, err := partialCommit(q, repo, branch)

		if err != nil {
			return nil, err
		}
		branchData, err := q.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		commit := &model.Commit{
			Tree:      root,
			Parents:   []string{branchData.GetCommit()},
			Committer: committer,
			Message:   message,
			Timestamp: time.Now().Unix(),
			Metadata:  metadata,
		}
		commitAddr := commitHash(commit)
		err = q.WriteCommit(commitAddr, commit)
		if err != nil {
			return nil, err
		}
		branchData.Commit = commitAddr
		branchData.CommitRoot = commit.GetTree()
		branchData.WorkspaceRoot = commit.GetTree()

		return nil, q.WriteBranch(branch, branchData)
	})
	return err
}

func (db *DBIndex) DeleteBranch(repo *model.Repo, branch string) error {
	_, err := db.Transact(repo, func(q Query) (interface{}, error) {
		branchData, err := q.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		q.ClearWorkspace(branch)
		gc(q, branchData.GetWorkspaceRoot()) // changes are destroyed here
		q.DeleteBranch(branch)
		return nil, nil
	})
	return err
}

func (db *DBIndex) Checkout(repo *model.Repo, branch, commit string) error {
	_, err := db.Transact(repo, func(q Query) (interface{}, error) {
		q.ClearWorkspace(branch)
		commitData, err := q.ReadCommit(commit)
		if err != nil {
			return nil, err
		}
		branchData, err := q.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		gc(q, branchData.GetWorkspaceRoot())
		branchData.Commit = commit
		branchData.CommitRoot = commitData.GetTree()
		branchData.WorkspaceRoot = commitData.GetTree()
		err = q.WriteBranch(branch, branchData)
		return nil, err
	})
	return err
}

func (db *DBIndex) Merge(repo *model.Repo, source, destination string) error {
	_, err := db.Transact(repo, func(q Query) (interface{}, error) {
		return nil, nil
	})
	return err
}
