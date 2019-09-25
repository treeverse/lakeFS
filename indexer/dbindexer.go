package indexer

import (
	"golang.org/x/xerrors"
	"versio-index/model"

	"github.com/golang/protobuf/proto"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)


const DefaultBranch = "master"

type DBIndex struct {
	database    fdb.Database
	workspace   subspace.Subspace
	tries       subspace.Subspace
	trieEntries subspace.Subspace
	blobs       subspace.Subspace
	commits     subspace.Subspace
	branches    subspace.Subspace
	refcounts   subspace.Subspace
}

func NewDBIndex(db fdb.Database) (*DBIndex, error) {
	dir, err := directory.CreateOrOpen(db, []string{"md_index"}, nil)
	if err != nil {
		return nil, err
	}
	return &DBIndex{
		db,
		dir.Sub("workspace"),
		dir.Sub("tries"),
		dir.Sub("entries"),
		dir.Sub("blobs"),
		dir.Sub("commits"),
		dir.Sub("branches"),
		dir.Sub("refcounts"),
	}, nil
}

func (w *DBIndex) Read(repo Repo, branch Branch, path string) (*model.Blob, error) {
	blob := &model.Blob{}
	_, err := w.database.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		// read from workspace
		blobkey := w.workspace.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, branch, path})
		result := tr.Get(blobkey).MustGet()
		if result != nil {
			err := proto.Unmarshal(result, blob)
			if err != nil {
				return nil, err
			}
		}
		// if not found, read from tree
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	return blob, nil
}

func (w *DBIndex) readFromTree(repo Repo, branch Branch, path string, tr fdb.ReadTransaction) (*model.Blob, error) {
	// get branch commit hash
	branchBytes := tr.Get(w.branches.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, branch})).MustGet()
	if branchBytes == nil {
		return nil, xerrors.Errorf("%s: %w", branch. ErrNotFound
	}

	// get commit's staging_trie (or root_trie if there's no workspace)
	// split and traverse path
}

func (w *DBIndex) Write(Repo, Branch, string, *model.Blob) error {

}

func (w *DBIndex) Delete(Repo, Branch, string) error {

}

func (w *DBIndex) List(Repo, Branch, string) ([]*model.Entry, error) {

}

func (w *DBIndex) Reset(Repo, Branch) error {

}
