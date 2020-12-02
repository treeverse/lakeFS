package rocks_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/treeverse/lakefs/catalog"

	"github.com/treeverse/lakefs/testutil"

	"github.com/treeverse/lakefs/catalog/rocks"
)

func TestPGRefManager_GetRepository(t *testing.T) {
	r := testRefManager(t)
	t.Run("repo_doesnt_exist", func(t *testing.T) {
		_, err := r.GetRepository(context.Background(), "example-repo")
		if err != rocks.ErrNotFound {
			t.Fatalf("expected ErrNotFound got error: %v", err)
		}
	})
	t.Run("repo_exists", func(t *testing.T) {
		testutil.Must(t, r.CreateRepository(context.Background(), "example-repo", rocks.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "weird-branch",
		}, rocks.Branch{}))

		repo, err := r.GetRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if repo.DefaultBranchID != "weird-branch" {
			t.Fatalf("got unexpected branch ID: %s", repo.DefaultBranchID)
		}
	})
}

func TestPGRefManager_ListRepositories(t *testing.T) {
	r := testRefManager(t)
	repos := []rocks.RepositoryID{"a", "aa", "b", "c", "e", "d"}
	for _, repoId := range repos {
		testutil.Must(t, r.CreateRepository(context.Background(), repoId, rocks.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "master",
		}, rocks.Branch{}))
	}

	t.Run("listing all repos", func(t *testing.T) {
		iter, err := r.ListRepositories(context.Background(), "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		repoIds := make([]rocks.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(repoIds, []rocks.RepositoryID{"a", "aa", "b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})

	t.Run("listing repos from prefix", func(t *testing.T) {
		iter, err := r.ListRepositories(context.Background(), "aaa")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		repoIds := make([]rocks.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(repoIds, []rocks.RepositoryID{"b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})
}

func TestPGRefManager_DeleteRepository(t *testing.T) {
	r := testRefManager(t)
	t.Run("repo_exists", func(t *testing.T) {
		testutil.Must(t, r.CreateRepository(context.Background(), "example-repo", rocks.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "weird-branch",
		}, rocks.Branch{}))

		_, err := r.GetRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = r.DeleteRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = r.GetRepository(context.Background(), "example-repo")
		if err != rocks.ErrNotFound {
			t.Fatalf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("repo_does_not_exists", func(t *testing.T) {
		// delete repo always returns success even if the repo doesn't exist
		err := r.DeleteRepository(context.Background(), "example-repo11111")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestPGRefManager_GetBranch(t *testing.T) {
	r := testRefManager(t)
	t.Run("get_branch_exists", func(t *testing.T) {
		testutil.Must(t, r.CreateRepository(context.Background(), "repo1", rocks.Repository{
			StorageNamespace: "s3://",
			CreationDate:     time.Now(),
			DefaultBranchID:  "master",
		}, rocks.Branch{
			CommitID: "c1",
		}))
		branch, err := r.GetBranch(context.Background(), "repo1", "master")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if branch.CommitID != "c1" {
			t.Fatalf("unexpected branch recevied: %s - expected c1", branch.CommitID)
		}
	})

	t.Run("get_branch_doesnt_exists", func(t *testing.T) {
		_, err := r.GetBranch(context.Background(), "repo1", "masterrrrr")
		if err != rocks.ErrNotFound {
			t.Fatalf("expected ErrNotFound, got error: %v", err)
		}
	})
}

func TestPGRefManager_SetBranch(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", rocks.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, rocks.Branch{
		CommitID: "c1",
	}))

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", rocks.Branch{
		CommitID: "c2",
	}))

	b, err := r.GetBranch(context.Background(), "repo1", "branch2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b.CommitID != "c2" {
		t.Fatalf("unexpected commit for branch2: %s - expected: c2", b.CommitID)
	}

	// overwrite
	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", rocks.Branch{
		CommitID: "c3",
	}))

	b, err = r.GetBranch(context.Background(), "repo1", "branch2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b.CommitID != "c3" {
		t.Fatalf("unexpected commit for branch2: %s - expected: c3", b.CommitID)
	}

}

func TestPGRefManager_DeleteBranch(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", rocks.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, rocks.Branch{
		CommitID: "c1",
	}))

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", rocks.Branch{
		CommitID: "c2",
	}))

	testutil.Must(t, r.DeleteBranch(context.Background(), "repo1", "branch2"))

	_, err := r.GetBranch(context.Background(), "repo1", "branch2")
	if err != rocks.ErrNotFound {
		t.Fatalf("unexpected error: %v", err)
	}

}

func TestPGRefManager_ListBranches(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", rocks.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, rocks.Branch{
		CommitID: "c1",
	}))

	for _, b := range []rocks.BranchID{"a", "aa", "c", "b", "z", "f"} {
		testutil.Must(t, r.SetBranch(context.Background(), "repo1", b, rocks.Branch{
			CommitID: "c2",
		}))
	}

	iter, err := r.ListBranches(context.Background(), "repo1", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var bs []rocks.BranchID
	for iter.Next() {
		b := iter.Value()
		bs = append(bs, b.BranchID)
	}
	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}
	if !reflect.DeepEqual(bs, []rocks.BranchID{"a", "aa", "b", "c", "f", "master", "z"}) {
		t.Fatalf("unexpected branch list: %v", bs)
	}
}

func TestPGRefManager_AddCommit(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", rocks.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, rocks.Branch{
		CommitID: "c1",
	}))

	ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z00:00")
	c := rocks.Commit{
		Committer:    "user1",
		Message:      "message1",
		TreeID:       "deadbeef123",
		CreationDate: ts,
		Parents:      rocks.CommitParents{"deadbeef1", "deadbeef12"},
		Metadata:     catalog.Metadata{"foo": "bar"},
	}

	cid, err := r.AddCommit(context.Background(), "repo1", c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cid != "cd3ed2b2ccaf6dae8a2f2e514899cd78b5f917b6687cc4425624dfd6cd1858b1" {
		t.Fatalf("unexpected commit ID: %s", cid)
	}

	commit, err := r.GetCommit(context.Background(), "repo1", cid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if commit.Parents[0] != "deadbeef1" {
		t.Fatalf("expected parent1 to be deadbeef1, got %v", commit.Parents)
	}

	if commit.Metadata["foo"] != "bar" {
		t.Fatalf("unexpected metadata value for foo: %v", commit.Metadata["foo"])
	}
}

func TestPGRefManager_Log(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", rocks.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, rocks.Branch{
		CommitID: "c1",
	}))

	ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z")
	var previous rocks.CommitID
	for i := 0; i < 20; i++ {
		c := rocks.Commit{
			Committer:    "user1",
			Message:      "message1",
			TreeID:       "deadbeef123",
			CreationDate: ts,
			Parents:      rocks.CommitParents{previous},
			Metadata:     catalog.Metadata{"foo": "bar"},
		}
		cid, err := r.AddCommit(context.Background(), "repo1", c)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		previous = cid
	}

	iter, err := r.Log(context.Background(), "repo1", previous)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ids := make([]rocks.CommitID, 0)
	for iter.Next() {
		c := iter.Value()
		ids = append(ids, c.CommitID)
	}
	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}

	expected := rocks.CommitParents{
		"c607be9a1103b8bac51399a386648729e2bc9b7503dde5cd3de6318c58b79fd6",
		"c3c52217e0b35f3abdffdbe2d304e4114a6232b3d3063622eff0212f4e384321",
		"19d18225090199f0054588771a85df04c5dc13dda396b6d2b096e2ce0e95c654",
		"fc51d43fb19e5b28410663ff1df1a34665b36d86c684ddbcdef520ec170081cb",
		"d0891d8aef006e532e4816ada3babe8dfb0c382f0f941458abc0a608af48c96e",
		"958a88c4c7506772b15fde2b4095ce984a0cfa101cc6fc1a50573f68dc289872",
		"25898c1fcb316d782f9e522be6e4a0c7c316ec5a4b9bfecdd6f3577de70cfd34",
		"b3794a3cdd7fad101ed64e07b7d42010362adb0228b64b7db92b29699c06ffa7",
		"9385029ea6e0171ae75557fda1ea55774c5e57745a56bff6630b050a80d0370a",
		"c6b747a1caafef68942a03af99f274174cbbe593d6d1c9cbc836b3a127c79581",
		"f42efb7a556bc638c94ea46e121bb388bff2dbf39be01226274ced570417d3c8",
		"0c6efef612a3d6a6a6b3985208d27d456c76c7338839d51bb49ce33d06fbaae8",
		"84d304a943f4ecac7e3ad5f7489bef55ec0034c659d8c2b16680d1a9d2fe4fcc",
		"48714265f7e6d219e32e5ac7ac011056e2837638711b61fb0a3b5baf2ec0c78d",
		"689d81325fecd7bea9ef3acc1f0975476f40996ad0ca57703556b512aff2a1f7",
		"24f9a7d15802c298430185dafb35b1c3c625385525d973c855152e23eef47ca2",
		"95a5b23c5cc6a890de6868b07dc787b13fca078429cf69c41bcce04cd4094f1e",
		"a4085647a9868ae5b1c13abee15918377cdba065e26c37b1fc9e4d868851ea98",
		"a404202e948cad8dfbeed76a6686079fc63960484120017b7f9b387bab996b38",
		"5acae1bce3f1ebbc67371dab00b1474323e776bf09fd4a1bdee3de1549566938",
	}

	if len(expected) != len(ids) {
		t.Fatalf("wrong size of log: %d - expected %d", len(ids), len(expected))
	}

	for i, cid := range ids {
		if cid != expected[i] {
			t.Fatalf("wrong commit ID at index %d: got %v expected %v", i, cid, expected[i])
		}
	}
}
