package store_test

import (
	"strings"
	"testing"
	"time"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/store"

	"golang.org/x/xerrors"
)

func TestReadRepo(t *testing.T) {
	kv, close := GetIndexStore(t)
	defer close()

	n := time.Now()
	repoId := "something that doesnt exist"

	kv.ReadTransact(func(ops store.ClientReadOnlyOperations) (i interface{}, e error) {
		_, err := ops.ReadRepo(repoId)
		if !xerrors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected not found error, got %v instead", err)
		}
		return nil, nil
	})

	kv.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		err := ops.WriteRepo(&model.Repo{
			RepoId:             repoId,
			CreationDate:       n.Unix(),
			DefaultBranch:      "master",
			PartialCommitRatio: index.DefaultPartialCommitRatio,
		})
		if err != nil {
			t.Fatal(err)
		}
		return nil, nil
	})

	kv.ReadTransact(func(ops store.ClientReadOnlyOperations) (i interface{}, e error) {
		repo, err := ops.ReadRepo(repoId)
		if err != nil {
			t.Fatalf("expected repo to exist, got error: %v\n", err)
		}
		if !strings.EqualFold(repo.GetRepoId(), repoId) {
			t.Fatalf("expceted to get back the repo we wrote with ID: %s, got %s", repoId, repo.GetRepoId())
		}
		return nil, nil
	})
}

func TestKVClientReadOnlyOperations_ListRepos(t *testing.T) {
	kv, close := GetIndexStore(t)
	defer close()

	now := time.Now().Unix()

	_, err := kv.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		var err error
		err = ops.WriteRepo(&model.Repo{
			RepoId:             "repo1",
			CreationDate:       now,
			DefaultBranch:      index.DefaultBranch,
			PartialCommitRatio: index.DefaultPartialCommitRatio,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteRepo(&model.Repo{
			RepoId:             "repo2",
			CreationDate:       now,
			DefaultBranch:      index.DefaultBranch,
			PartialCommitRatio: index.DefaultPartialCommitRatio,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteRepo(&model.Repo{
			RepoId:             "repo3",
			CreationDate:       now,
			DefaultBranch:      index.DefaultBranch,
			PartialCommitRatio: index.DefaultPartialCommitRatio,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteRepo(&model.Repo{
			RepoId:             "yet another repo",
			CreationDate:       now,
			DefaultBranch:      index.DefaultBranch,
			PartialCommitRatio: index.DefaultPartialCommitRatio,
		})
		if err != nil {
			t.Fatal(err)
		}
		return nil, err
	})

	if err != nil {
		t.Fatal(err)
	}

	_, err = kv.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		repos, err := ops.ListRepos()
		if err != nil {
			t.Fatal(err)
		}
		if len(repos) != 4 {
			t.Fatalf("expected to get back 4 repos, got %d\n", len(repos))
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestKVClientOperations_DeleteRepo(t *testing.T) {
	kv, close := GetIndexStore(t)
	defer close()

	now := time.Now().Unix()

	_, err := kv.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		var err error
		//should not be deleted
		err = ops.WriteRepo(&model.Repo{
			RepoId:             "repo1",
			CreationDate:       now,
			DefaultBranch:      index.DefaultBranch,
			PartialCommitRatio: index.DefaultPartialCommitRatio,
		})
		if err != nil {
			t.Fatal(err)
		}
		// Should be deleted
		err = ops.WriteRepo(&model.Repo{
			RepoId:             "repo1asprefix",
			CreationDate:       now,
			DefaultBranch:      index.DefaultBranch,
			PartialCommitRatio: index.DefaultPartialCommitRatio,
		})
		if err != nil {
			t.Fatal(err)
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = kv.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		var err error
		err = ops.DeleteRepo("repo1")
		if err != nil {
			t.Fatal(err)
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = kv.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		_, err := ops.ReadRepo("repo1")
		if !xerrors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected repo to be deleted, instead got error: %v", err)
		}
		//check prefix
		_, err = ops.ReadRepo("repo1asprefix")
		if xerrors.Is(err, db.ErrNotFound) {
			t.Fatalf("did not expect repo to be deleted, instead got error: %v", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestKVClientOperations_WriteRepo(t *testing.T) {
	kv, close := GetIndexStore(t)
	defer close()

	now := time.Now().Unix()

	_, err := kv.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		var err error
		err = ops.WriteRepo(&model.Repo{
			RepoId:             "repo1",
			CreationDate:       now,
			DefaultBranch:      index.DefaultBranch,
			PartialCommitRatio: index.DefaultPartialCommitRatio,
		})
		if err != nil {
			t.Fatal(err)
		}

		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}

	// expect error on creating existing bucket
	_, err = kv.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		var err error
		err = ops.WriteRepo(&model.Repo{
			RepoId:             "repo1",
			CreationDate:       now,
			DefaultBranch:      index.DefaultBranch,
			PartialCommitRatio: index.DefaultPartialCommitRatio,
		})
		if err == nil {
			t.Errorf("expected to get error when creating existing bucket")
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = kv.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		_, err := ops.ReadRepo("repo1")
		if xerrors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected to read created repo, instead got error: %v", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

}
