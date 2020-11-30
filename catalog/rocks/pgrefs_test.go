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

	if cid != "6c34828ac8e4802048c4baa5f1e18801dd17d0dbb17088ee62f15fe76873400a" {
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
		"a5899fab11823646608324bb98549df39b335c515a5b6d19372a01e707400943",
		"7abd46b1ad3ad508091840a51fc6dd932501d676d16006f8129b215c51405db9",
		"ee222be6e5398fbc6b0a955142a819e4af4537da59f65b06cee002607ba75085",
		"95137b38639b2902354e7cb7d1aa3df7bf826ebce3fb69a109e1c01086f62fc8",
		"80398db2757f5ea0cb6c7dce906fa1e3a03d4a4c220a88a852102d2f371d61ec",
		"cb0fc65fe0e4b08af9dae54b1749050969dd49cf2f070c82c65fde5cd0b98116",
		"c6915a2386812c43094ed3fb56845b76d2a4d88ed5074a7ba67196551c8e008c",
		"f0dcb06adb3fd79e827e641ffbda68d9712853c3dce02b0bfc0e266f44f5ab9d",
		"d05d6fd07d095190cac6e3f3f3e11e842ea6c0e694c2cd369f1026fb767110e8",
		"ef48f58cb5dba2754c8b1271f18e85fb58fc5cdccec3e9459c41f21c41de471b",
		"459aba33665c6eba4e9fa537a6b48347ad022d2a5af196a3f4f8341e6a105f5b",
		"903cdfe6bbb36fd60c790e52d808015ab5530df0bea23c07aef80cf87f8fb958",
		"95bb67123ad61e68688bd47b51a9a61b001b7c0aa378bbcde1435d164df8a896",
		"38fb704cf1b40daf8dc83672a5bfd879339d209786baf97c12866756252b4470",
		"22dcbfb5814efb714930ddb24946c4a465f7a666d82cda30c02c82d3fdd5a0ea",
		"e4ded0a935313e5f26d787e6f9cb2af4cc9d812c29d3ea5d990b6fd8e9f9660d",
		"706e04df978b0e8fa636fd8e13a775ae77303ee36fc69f2d5649751228b7f42e",
		"230e258e0bb8c337b92e1073eff3d92dc4c4331b5ccaccd4244c468c03725973",
		"1eecf99f174ee0f3abc0e04a73b534392dbff0b7645cfa9ef1a75963a93d5aeb",
		"5ee7f7268d93d97f51fde483f3d26869c42c2e18e2d690b49a82df7442c14836",
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
