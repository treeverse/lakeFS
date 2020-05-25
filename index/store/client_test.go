package store_test

import (
	"errors"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/store"
	"github.com/treeverse/lakefs/testutil"
)

var (
	pool        *dockertest.Pool
	databaseUri string
)

func TestMain(m *testing.M) {
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	databaseUri, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

func TestReadRepo(t *testing.T) {
	mdb, _ := testutil.GetDB(t, databaseUri, "lakefs_index")
	str := store.NewDBStore(mdb)

	n := time.Now()
	repoId := "something that doesnt exist"

	str.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		_, err := ops.ReadRepo(repoId)
		if !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected not found error, got %v instead", err)
		}
		return nil, nil
	}, db.ReadOnly())

	str.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		err := ops.WriteRepo(&model.Repo{
			Id:            repoId,
			CreationDate:  n,
			DefaultBranch: "master",
		})
		if err != nil {
			t.Fatal(err)
		}
		return nil, nil
	})

	str.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		repo, err := ops.ReadRepo(repoId)
		if err != nil {
			t.Fatalf("expected repo to exist, got error: %v\n", err)
		}
		if !strings.EqualFold(repo.Id, repoId) {
			t.Fatalf("expceted to get back the repo we wrote with ID: %s, got %s", repoId, repo.Id)
		}
		return nil, nil
	}, db.ReadOnly())
}

func TestKVClientReadOnlyOperations_ListRepos(t *testing.T) {
	mdb, _ := testutil.GetDB(t, databaseUri, "lakefs_index")
	str := store.NewDBStore(mdb)
	now := time.Now()

	_, err := str.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		var err error
		err = ops.WriteRepo(&model.Repo{
			Id:            "repo1",
			CreationDate:  now,
			DefaultBranch: index.DefaultBranch,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteRepo(&model.Repo{
			Id:            "repo2",
			CreationDate:  now,
			DefaultBranch: index.DefaultBranch,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteRepo(&model.Repo{
			Id:            "repo3",
			CreationDate:  now,
			DefaultBranch: index.DefaultBranch,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteRepo(&model.Repo{
			Id:            "yet another repo",
			CreationDate:  now,
			DefaultBranch: index.DefaultBranch,
		})
		if err != nil {
			t.Fatal(err)
		}
		return nil, err
	})

	if err != nil {
		t.Fatal(err)
	}

	_, err = str.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		repos, _, err := ops.ListRepos(-1, "")
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
	mdb, _ := testutil.GetDB(t, databaseUri, "lakefs_index")
	str := store.NewDBStore(mdb)

	now := time.Now()

	_, err := str.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		var err error
		//should not be deleted
		err = ops.WriteRepo(&model.Repo{
			Id:            "repo1",
			CreationDate:  now,
			DefaultBranch: index.DefaultBranch,
		})
		if err != nil {
			t.Fatal(err)
		}
		// Should be deleted
		err = ops.WriteRepo(&model.Repo{
			Id:            "repo1asprefix",
			CreationDate:  now,
			DefaultBranch: index.DefaultBranch,
		})
		if err != nil {
			t.Fatal(err)
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = str.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
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

	_, err = str.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		_, err := ops.ReadRepo("repo1")
		if !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected repo to be deleted, instead got error: %v", err)
		}
		//check prefix
		_, err = ops.ReadRepo("repo1asprefix")
		if errors.Is(err, db.ErrNotFound) {
			t.Fatalf("did not expect repo to be deleted, instead got error: %v", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestKVClientOperations_WriteRepo(t *testing.T) {
	mdb, _ := testutil.GetDB(t, databaseUri, "lakefs_index")
	str := store.NewDBStore(mdb)

	now := time.Now()

	_, err := str.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		var err error
		err = ops.WriteRepo(&model.Repo{
			Id:            "repo1",
			CreationDate:  now,
			DefaultBranch: index.DefaultBranch,
		})
		if err != nil {
			t.Fatal(err)
		}

		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = str.Transact(func(ops store.ClientOperations) (i interface{}, e error) {
		_, err := ops.ReadRepo("repo1")
		if errors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected to read created repo, instead got error: %v", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
