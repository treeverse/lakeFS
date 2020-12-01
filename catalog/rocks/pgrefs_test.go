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

	if cid != "d85b54adf6c0b15503dc6cdea4c5323753e8b246618f47af6a502326da956793" {
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
		"c5262f504dd81a34e6e7578ad66a8395bee18ea3d198c5f5d74830eb43098279",
		"f2712b1cace8d1d6aa2638a1fe12d2428a1a001943f890c086702eb6eb0a5606",
		"1f3a725fe62d9e5bfe5abc74ad75162d27416935d16d842afa808c6f2f880084",
		"992ac1278647b8a7f374b1bfed8d8bed82e6cb16f8d50f272fc48d5f998182f0",
		"d5e1ff6470911b0a5d3f587a3db09504f8d3fef9a34ab9025a003b3bc1636023",
		"5ce8db7eeacafa343783adcb885aa6239f5df70c416826a5d78519a3ca2a6313",
		"a42282eab74cfd036ab0fa6ddc070fd3541632ee008f3a3f7de611ad4f395653",
		"798300f78bf9c1c6f4df8b87976fa9efa1b317070d49b7c54cd282b25c4db39c",
		"a4f672a9a1ceb49729dfd159fc0d9786765a09c8b178abd731953cf4c72e1542",
		"2fee5fd15a7bc6802ba4d4f8febc46519a8334012cba5b942fb03ed1ebd2b2b9",
		"cbebc01677dd5f39736f48e59620258aef5603de91fc713c5002330c0ed0103d",
		"680d546cf6cb876a6def445f8b15dc6ceab61a4193af9b623dcdfa1abc0a6d01",
		"36669c265362c2e07f8ffd413c125eac2ac90eacf65cf51b7f79c56f815813b2",
		"2077138a1aa3fb43c1ab7e2ba616c56d46bd069a1197ba00de8744c79f493c5c",
		"fb63f72ba0994dcbcbab8d63ce166dbff114ad2f390a175902134bc85d96fbb2",
		"7fc0e540820668488479f86c41828a85144993b196a5bf2405d37847438a78f4",
		"128233c82d80f91d62cb9bb49fd589ad8249fb4567a915f2a7dafd88d82c7527",
		"0dcaed021298fa2c4a4d99bbecb5630646363b6a0acc64e9113251d1511260c7",
		"f17975abb9442d7cd838c3e5ef63449f4ace261bd6975a51ddd343a2e654e2ad",
		"a66bb92bd9b389f942adb2d5e9e50e63d04d510dd2945da104b7f35c1302378c",
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
