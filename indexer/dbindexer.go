package indexer

import (
	"math/rand"
	"strings"

	"versio-index/model"

	"golang.org/x/xerrors"

	"github.com/golang/protobuf/proto"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	DefaultBranch       = "master"
	PathSeperator       = "/"
	PartialCommitChance = 0.01
)

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
	var err error
	_, err = w.database.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
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
		blob, err = w.readFromTree(repo, branch, path, tr)
		return nil, err
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
		// branch not found we fall back to master..
		branchBytes := tr.Get(w.branches.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, DefaultBranch})).MustGet()
		if branchBytes == nil {
			return nil, xerrors.Errorf("%s: %w", path, ErrNotFound)
		}
	}
	// parse branch
	branchData := &model.Branch{}
	err := proto.Unmarshal(branchBytes, branchData)
	if err != nil {
		return nil, xerrors.Errorf("unable to read branch data: %w", ErrIndexMalformed)
	}
	// get commit's workspace trie (or root trie if there's no workspace)
	rootID := branchData.GetWorkspaceRootHash()
	if strings.EqualFold(rootID, "") {
		rootID = branchData.GetCommitRootHash()
	}

	// split and traverse path
	return w.traverse(repo, rootID, path, tr)
}

func (w *DBIndex) traverse(repo Repo, rootID, path string, tr fdb.ReadTransaction) (*model.Blob, error) {
	currentAddr := rootID
	parts := strings.Split(path, PathSeperator)
	for i, part := range parts {
		if i == len(parts)-1 {
			// get a blob
			blobkey := w.blobs.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, currentAddr})
			blobData := tr.Get(blobkey).MustGet()
			if len(blobkey) == 0 {
				return nil, xerrors.Errorf("%s: %w", path, ErrNotFound)
			}
			blob := &model.Blob{}
			err := proto.Unmarshal(blobData, blob)
			if err != nil {
				return nil, xerrors.Errorf("unable to read file: %w", err)
			}
			return blob, nil
		}
		// get sub trie
		entrykey := w.trieEntries.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, currentAddr, "d", part})
		entryData := tr.Get(entrykey).MustGet()
		if len(entryData) == 0 {
			return nil, xerrors.Errorf("%s: %w", path, ErrNotFound)
		}
		entry := &model.Entry{}
		err := proto.Unmarshal(entryData, entry)
		if err != nil {
			return nil, xerrors.Errorf("unable to read file: %w", err)
		}
		currentAddr = entry.Address
	}
	return nil, ErrNotFound
}

func shouldPartiallyCommit() bool {
	chosen := rand.Float64()
	return chosen < PartialCommitChance
}

func (w *DBIndex) partialCommit(repo Repo, branch Branch) {
	// TODO
}

func (w *DBIndex) Write(repo Repo, branch Branch, path string, blob *model.Blob) error {
	blobkey := w.workspace.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, branch, path})
	blobdata, err := proto.Marshal(blob)
	if err != nil {
		return ErrBadBlock
	}
	_, err = w.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(blobkey, blobdata)
		if shouldPartiallyCommit() {
			w.partialCommit(repo, branch)
		}
		return nil, nil
	})
	return err
}

func (w *DBIndex) Delete(repo Repo, branch Branch, path string) error {
	_, err := w.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
	})
	return err
}

func (w *DBIndex) List(repo Repo, branch Branch, path string) ([]*model.Entry, error) {

}

func (w *DBIndex) Reset(repo Repo, branch Branch) error {

}
