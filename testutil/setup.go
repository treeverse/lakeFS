package testutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/block"

	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/store"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
)

type nullLogger struct{}

func (l nullLogger) Errorf(string, ...interface{})   {}
func (l nullLogger) Warningf(string, ...interface{}) {}
func (l nullLogger) Infof(string, ...interface{})    {}
func (l nullLogger) Debugf(string, ...interface{})   {}

const TimeFormat = "Jan 2 15:04:05 2006 -0700"

func GetIndexStoreWithRepo(t *testing.T) (store.Store, *model.Repo, func()) {
	db, closer := GetDB(t)
	kv := store.NewKVStore(db)
	repoCreateDate, _ := time.Parse(TimeFormat, "Apr 7 15:13:13 2005 -0700")
	repo, err := kv.RepoTransact("myrepo", func(ops store.RepoOperations) (i interface{}, e error) {
		repo := &model.Repo{
			RepoId:        "myrepo",
			CreationDate:  repoCreateDate.Unix(),
			DefaultBranch: index.DefaultBranch,
		}
		err := ops.WriteRepo(repo)
		if err != nil {
			t.Fatal(err)
		}
		commit := &model.Commit{
			Tree:      ident.Empty(),
			Parents:   []string{},
			Timestamp: repoCreateDate.Unix(),
			Metadata:  make(map[string]string),
		}
		commitId := ident.Hash(commit)
		commit.Address = commitId
		err = ops.WriteCommit(commitId, commit)
		if err != nil {
			return nil, err
		}
		err = ops.WriteBranch(repo.GetDefaultBranch(), &model.Branch{
			Name:          repo.GetDefaultBranch(),
			Commit:        commitId,
			CommitRoot:    commit.GetTree(),
			WorkspaceRoot: commit.GetTree(),
		})
		return repo, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return kv, repo.(*model.Repo), closer
}

func GetDB(t *testing.T) (db.Store, func()) {
	dir, err := ioutil.TempDir("", fmt.Sprintf("badger-%s", uuid.Must(uuid.NewUUID()).String()))
	if err != nil {
		t.Fatal(err)
	}
	opts := badger.DefaultOptions(dir)
	opts.Logger = nullLogger{}
	opts.TableLoadingMode = options.LoadToRAM
	kv, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	return db.NewLocalDBStore(kv), func() {
		kv.Close()
		os.RemoveAll(dir)
	}
}

func GetBlockAdapter(t *testing.T) (block.Adapter, func()) {
	dir, err := ioutil.TempDir("", fmt.Sprintf("blocks-%s", uuid.Must(uuid.NewUUID()).String()))
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := block.NewLocalFSAdapter(dir)
	if err != nil {
		t.Fatal(err)
	}
	return adapter, func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func Must(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}
