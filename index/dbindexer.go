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

func (db *DBIndex) Query(repo *model.Repo) *Query {
	return &Query{
		workspace: db.workspace,
		trees:     db.trees,
		entries:   db.entries,
		blobs:     db.blobs,
		commits:   db.commits,
		branches:  db.branches,
		refCounts: db.refCounts,
		repo:      repo,
	}
}

func (db *DBIndex) Read(repo *model.Repo, branch, path string) (*model.Object, error) {
	obj, err := db.database.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		var obj *model.Object
		reader := db.Query(repo).Reader(tx)
		we, err := reader.ReadFromWorkspace(branch, path)
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
			obj, err = readFromTree(reader, repo, branch, path)
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

func resolveReadRoot(r *QueryReader, repo *model.Repo, branch string) (string, error) {
	var empty string
	branchData, err := r.ReadBranch(branch)
	if xerrors.Is(err, ErrNotFound) {
		// fallback to default branch
		branchData, err = r.ReadBranch(repo.DefaultBranch)
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

func readFromTree(r *QueryReader, repo *model.Repo, branch, path string) (*model.Object, error) {
	// resolve tree root to read from
	root, err := resolveReadRoot(r, repo, branch)
	if err != nil {
		return nil, err
	}
	// get the tree
	return traverse(r, root, path)
}

func traverse(r *QueryReader, treeID, path string) (*model.Object, error) {
	currentAddress := treeID
	parts := strings.Split(path, PathSeparator)
	for i, part := range parts {
		if i == len(parts)-1 {
			// last item in the path is the blob
			entry, err := r.ReadEntry(currentAddress, "f", part)
			if err != nil {
				return nil, err
			}
			blob, err := r.ReadBlob(entry.GetAddress())
			if err != nil {
				return nil, err
			}
			return &model.Object{
				Blob:     blob,
				Metadata: entry.GetMetadata(),
			}, nil
		}
		entry, err := r.ReadEntry(currentAddress, "d", part)
		if err != nil {
			return nil, err
		}
		currentAddress = entry.GetAddress()
	}
	return nil, ErrNotFound
}

func traverseDir(r *QueryReader, treeID, path string) (string, error) {
	currentAddress := treeID
	parts := strings.Split(path, PathSeparator)
	for _, part := range parts {
		entry, err := r.ReadEntry(currentAddress, "d", part)
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

func partialCommit(w *QueryWriter, repo *model.Repo, branch string) (string, error) {
	// 1. iterate all changes in the current workspace
	// 2. Apply them to the Merkle root as exists in the branch pointer
	// 3. calculate new Merkle root
	// 4. save it in the branch pointer
	return "", nil
}

func writeEntryToWorkspace(w *QueryWriter, repo *model.Repo, branch, path string, entry *model.WorkspaceEntry) error {
	err := w.WriteToWorkspacePath(branch, path, entry)
	if err != nil {
		return err
	}
	if shouldPartiallyCommit(repo) {
		_, err = partialCommit(w, repo, branch)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DBIndex) Write(repo *model.Repo, branch, path string, object *model.Object) error {
	_, err := db.database.Transact(func(tx fdb.Transaction) (interface{}, error) {
		w := db.Query(repo).Writer(tx)
		err := writeEntryToWorkspace(w, repo, branch, path, &model.WorkspaceEntry{
			Data: &model.WorkspaceEntry_Object{Object: object},
		})
		return nil, err
	})
	return err
}

func (db *DBIndex) Delete(repo *model.Repo, branch, path string) error {
	_, err := db.database.Transact(func(tx fdb.Transaction) (interface{}, error) {
		w := db.Query(repo).Writer(tx)
		err := writeEntryToWorkspace(w, repo, branch, path, &model.WorkspaceEntry{
			Data: &model.WorkspaceEntry_Tombstone{Tombstone: &model.Tombstone{}},
		})
		return nil, err
	})
	return err
}

func (db *DBIndex) List(repo *model.Repo, branch, path string) ([]*model.Entry, error) {
	entries, err := db.database.Transact(func(tx fdb.Transaction) (interface{}, error) {
		r := db.Query(repo).Reader(tx)
		w := db.Query(repo).Writer(tx)
		_, err := partialCommit(w, repo, branch)
		if err != nil {
			return nil, err
		}
		root, err := resolveReadRoot(r, repo, branch)
		if err != nil {
			return nil, err
		}
		addr, err := traverseDir(r, root, path)
		if err != nil {
			return nil, err
		}
		return r.ListEntries(addr)
	})
	if err != nil {
		return nil, err
	}
	return entries.([]*model.Entry), nil
}

func (db *DBIndex) Reset(repo *model.Repo, branch string) error {
	// clear workspace, set branch workspace root back to commit root
	_, err := db.database.Transact(func(tx fdb.Transaction) (interface{}, error) {
		r := db.Query(repo).Reader(tx)
		w := db.Query(repo).Writer(tx)

		w.clearPrefix(db.workspace, repo.GetClientId(), repo.GetRepoId(), branch)
		branchData, err := r.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		gc(r, w, branchData.GetWorkspaceRoot())
		branchData.WorkspaceRoot = branchData.GetCommitRoot()
		return nil, w.WriteBranch(branch, branchData)
	})
	return err
}

func commitHash(commit *model.Commit) string {
	return "foo"
}

func (db *DBIndex) Commit(repo *model.Repo, branch, message, committer string, metadata map[string]string) error {
	_, err := db.database.Transact(func(tx fdb.Transaction) (interface{}, error) {
		r := db.Query(repo).Reader(tx)
		w := db.Query(repo).Writer(tx)
		root, err := partialCommit(w, repo, branch)
		if err != nil {
			return nil, err
		}
		branchData, err := r.ReadBranch(branch)
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
		err = w.WriteCommit(commitAddr, commit)
		if err != nil {
			return nil, err
		}
		branchData.Commit = commitAddr
		branchData.CommitRoot = commit.GetTree()
		branchData.WorkspaceRoot = commit.GetTree()

		return nil, w.WriteBranch(branch, branchData)
	})
	return err
}

func gc(r *QueryReader, w *QueryWriter, treeAddress string) {

}

func (db *DBIndex) DeleteBranch(repo *model.Repo, branch string) error {
	_, err := db.database.Transact(func(tx fdb.Transaction) (interface{}, error) {
		r := db.Query(repo).Reader(tx)
		w := db.Query(repo).Writer(tx)

		w.clearPrefix(db.workspace, repo.GetClientId(), repo.GetRepoId(), branch)
		branchData, err := r.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		gc(r, w, branchData.GetCommitRoot())
		gc(r, w, branchData.GetWorkspaceRoot())
		w.DeleteBranch(branch)
		return nil, nil
	})
	return err
}

//func (w *DBIndex) Checkout(repo Repo, branch Branch, commit string) error {
//	return nil
//}
//

//func (w *DBIndex) Merge(repo Repo, source, destination Branch) error {
//	return nil
//}
