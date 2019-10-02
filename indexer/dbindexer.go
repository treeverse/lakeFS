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
	PathSeparator       = "/"
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
	parts := strings.Split(path, PathSeparator)
	for i, part := range parts {
		if i == len(parts)-1 {
			// get a blob
			blobKey := w.blobs.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, currentAddr})
			blobData := tr.Get(blobKey).MustGet()
			if len(blobData) == 0 {
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
		entryKey := w.trieEntries.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, currentAddr, "d", part})
		entryData := tr.Get(entryKey).MustGet()
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

func (w *DBIndex) partialCommit(repo Repo, branch Branch, tr fdb.Transaction) error {
	// 1. iterate all changes in the current workspace
	changes := make(map[string]*model.Blob)
	start := w.workspace.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, branch})
	end := w.workspace.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, branch, 0xFF})
	result := tr.GetRange(fdb.KeyRange{
		Begin: start,
		End:   end,
	}, fdb.RangeOptions{}).Iterator()
	for result.Advance() {
		kv := result.MustGet()
		keyTuple, err := w.workspace.Unpack(kv.Key)
		if err != nil {
			return err
		}
		// (client, repo, branch, path)
		blob := &model.Blob{}
		err = proto.Unmarshal(kv.Value, blob)
		if err != nil {
			return err
		}
		changes[keyTuple[3].(string)] = blob
	}

	// 2. Apply them to the Merkle root as exists in the branch pointer
	rootKey := w.branches.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, branch})
	branchData := tr.Get(rootKey).MustGet()
	branchMeta := &model.Branch{}
	err := proto.Unmarshal(branchData, branchMeta)
	if err != nil {
		return err
	}
	//root := branchMeta.WorkspaceRootHash

	//for path, blob := range changes {
	//
	//}

	// 3. calculate new Merkle root
	// 4. save it in the branch pointer
	return nil
}

func (w *DBIndex) Write(repo Repo, branch Branch, path string, blob *model.Blob) error {
	blobKey := w.workspace.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, branch, path})
	blobData, err := proto.Marshal(blob)
	if err != nil {
		return ErrBadBlock
	}
	_, err = w.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(blobKey, blobData)
		if shouldPartiallyCommit() {
			_ = w.partialCommit(repo, branch, tr)
		}
		return nil, nil
	})
	return err
}

func (w *DBIndex) Delete(repo Repo, branch Branch, path string) error {
	blobKey := w.workspace.Pack(tuple.Tuple{repo.ClientID, repo.RepoID, branch, path})
	tombStone, _ := proto.Marshal(&model.Blob{Metadata: map[string]string{"tombstone": "true"}})
	_, err := w.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(blobKey, tombStone)
		if shouldPartiallyCommit() {
			_ = w.partialCommit(repo, branch, tr)
		}
		return nil, nil
	})
	return err
}

func (w *DBIndex) List(repo Repo, branch Branch, path string) ([]*model.Entry, error) {
	_, err := w.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err := w.partialCommit(repo, branch, tr)
		if err != nil {
			return nil, err
		}
		w.readFromTree(repo, branch, path, tr) // TODO: refactor this method to allow listing.
		return nil, nil
	})
	return nil, err
}

func (w *DBIndex) Reset(repo Repo, branch Branch) error {
	_, err := w.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return nil, nil
	})
	return err
}

func (w *DBIndex) Commit(repo Repo, branch Branch, committer, message string) error {
	return nil
}
func (w *DBIndex) Merge(repo Repo, source, destination Branch) error {
	return nil
}
func (w *DBIndex) DeleteBranch(repo Repo, branch Branch) error {
	return nil
}
func (w *DBIndex) Checkout(repo Repo, branch Branch, commit string) error {
	return nil
}

func (w *DBIndex) gc(repo Repo, branch Branch, root string) error {
	return nil
}
