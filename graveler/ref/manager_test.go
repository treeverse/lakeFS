package ref_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/testutil"
)

func TestManager_GetRepository(t *testing.T) {
	r := testRefManager(t)
	t.Run("repo_doesnt_exist", func(t *testing.T) {
		_, err := r.GetRepository(context.Background(), "example-repo")
		if err != graveler.ErrNotFound {
			t.Fatalf("expected ErrNotFound got error: %v", err)
		}
	})
	t.Run("repo_exists", func(t *testing.T) {
		testutil.Must(t, r.CreateRepository(context.Background(), "example-repo", graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "weird-branch",
		}, graveler.Branch{}))

		repo, err := r.GetRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if repo.DefaultBranchID != "weird-branch" {
			t.Fatalf("got unexpected branch ID: %s", repo.DefaultBranchID)
		}
	})
}

func TestManager_ListRepositories(t *testing.T) {
	r := testRefManager(t)
	repos := []graveler.RepositoryID{"a", "aa", "b", "c", "e", "d"}
	for _, repoId := range repos {
		testutil.Must(t, r.CreateRepository(context.Background(), repoId, graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "master",
		}, graveler.Branch{}))
	}

	t.Run("listing all repos", func(t *testing.T) {
		iter, err := r.ListRepositories(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(repoIds, []graveler.RepositoryID{"a", "aa", "b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})

	t.Run("listing repos from prefix", func(t *testing.T) {
		iter, err := r.ListRepositories(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		iter.SeekGE("aaa")

		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(repoIds, []graveler.RepositoryID{"b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})
}

func TestManager_DeleteRepository(t *testing.T) {
	r := testRefManager(t)
	t.Run("repo_exists", func(t *testing.T) {
		testutil.Must(t, r.CreateRepository(context.Background(), "example-repo", graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "weird-branch",
		}, graveler.Branch{}))

		_, err := r.GetRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = r.DeleteRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = r.GetRepository(context.Background(), "example-repo")
		if err != graveler.ErrNotFound {
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

func TestManager_GetBranch(t *testing.T) {
	r := testRefManager(t)
	t.Run("get_branch_exists", func(t *testing.T) {
		testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
			StorageNamespace: "s3://",
			CreationDate:     time.Now(),
			DefaultBranchID:  "master",
		}, graveler.Branch{
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
		if err != graveler.ErrNotFound {
			t.Fatalf("expected ErrNotFound, got error: %v", err)
		}
	})
}

func TestManager_SetBranch(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{
		CommitID: "c1",
	}))

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", graveler.Branch{
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
	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", graveler.Branch{
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

func TestManager_DeleteBranch(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{
		CommitID: "c1",
	}))

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", graveler.Branch{
		CommitID: "c2",
	}))

	testutil.Must(t, r.DeleteBranch(context.Background(), "repo1", "branch2"))

	_, err := r.GetBranch(context.Background(), "repo1", "branch2")
	if err != graveler.ErrNotFound {
		t.Fatalf("unexpected error: %v", err)
	}

}

func TestManager_ListBranches(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{
		CommitID: "c1",
	}))

	for _, b := range []graveler.BranchID{"a", "aa", "c", "b", "z", "f"} {
		testutil.Must(t, r.SetBranch(context.Background(), "repo1", b, graveler.Branch{
			CommitID: "c2",
		}))
	}

	iter, err := r.ListBranches(context.Background(), "repo1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var bs []graveler.BranchID
	for iter.Next() {
		b := iter.Value()
		bs = append(bs, b.BranchID)
	}
	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}
	if !reflect.DeepEqual(bs, []graveler.BranchID{"a", "aa", "b", "c", "f", "master", "z"}) {
		t.Fatalf("unexpected branch list: %v", bs)
	}
}

func TestManager_GetTag(t *testing.T) {
	r := testRefManager(t)
	t.Run("exists", func(t *testing.T) {
		ctx := context.Background()
		err := r.CreateRepository(ctx, "repo1", graveler.Repository{
			StorageNamespace: "s3://",
			CreationDate:     time.Now(),
			DefaultBranchID:  "master",
		}, graveler.Branch{
			CommitID: "c1",
		})
		testutil.MustDo(t, "create repo", err)
		err = r.CreateTag(ctx, "repo1", "v1.0", "c1")
		testutil.MustDo(t, "set tag", err)
		commitID, err := r.GetTag(context.Background(), "repo1", "v1.0")
		testutil.MustDo(t, "get existing tag", err)
		if commitID == nil {
			t.Fatal("get tag, missing commit id")
		}
		if *commitID != "c1" {
			t.Fatalf("get tag, commit id: %s, expected c1", *commitID)
		}
	})

	t.Run("not_exists", func(t *testing.T) {
		commitID, err := r.GetTag(context.Background(), "repo1", "v1.bad")
		if !errors.Is(err, graveler.ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got error: %v", err)
		}
		if commitID != nil {
			t.Fatalf("get not existing commitID: %s, expected nil", *commitID)
		}
	})
}

func TestManager_CreateTag(t *testing.T) {
	r := testRefManager(t)
	ctx := context.Background()
	testutil.Must(t, r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{
		CommitID: "c1",
	}))

	err := r.CreateTag(ctx, "repo1", "v2", "c2")
	testutil.MustDo(t, "create tag v2", err)

	commit, err := r.GetTag(ctx, "repo1", "v2")
	testutil.MustDo(t, "get v2 tag", err)
	if commit == nil {
		t.Fatal("get tag got nil")
	}
	if *commit != "c2" {
		t.Fatalf("unexpected commit for tag v2: %s - expected: c2", *commit)
	}

	// check we can't create existing
	err = r.CreateTag(ctx, "repo1", "v2", "c5")
	if !errors.Is(err, graveler.ErrTagAlreadyExists) {
		t.Fatalf("CreateTag() err = %s, expected already exists", err)
	}
	// overwrite by delete and create
	err = r.DeleteTag(ctx, "repo1", "v2")
	testutil.MustDo(t, "delete tag v2", err)

	err = r.CreateTag(ctx, "repo1", "v2", "c3")
	testutil.MustDo(t, "re-create tag v2", err)

	commit, err = r.GetTag(ctx, "repo1", "v2")
	testutil.MustDo(t, "get tag v2", err)
	if commit == nil {
		t.Fatal("get tag got nil")
	}
	if *commit != "c3" {
		t.Fatalf("unexpected commit for v2: %s - expected: c3", *commit)
	}
}

func TestManager_DeleteTag(t *testing.T) {
	r := testRefManager(t)
	ctx := context.Background()
	testutil.Must(t, r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{
		CommitID: "c1",
	}))

	testutil.Must(t, r.CreateTag(ctx, "repo1", "v1", "c2"))

	testutil.Must(t, r.DeleteTag(ctx, "repo1", "v1"))

	commitID, err := r.GetTag(ctx, "repo1", "v1")
	if !errors.Is(err, graveler.ErrNotFound) {
		t.Fatal("unexpected error:", err)
	}
	if commitID != nil {
		t.Fatal("expected commit ID:", *commitID)
	}
}

func TestManager_ListTags(t *testing.T) {
	r := testRefManager(t)
	ctx := context.Background()
	testutil.Must(t, r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{
		CommitID: "c1",
	}))

	var commitsTagged []graveler.CommitID
	tags := []string{"tag-a", "tag-b", "the-end", "v1", "v1.1"}
	sort.Strings(tags)
	for i, tag := range tags {
		commitID := graveler.CommitID(fmt.Sprintf("c%d", i))
		commitsTagged = append(commitsTagged, commitID)
		err := r.CreateTag(ctx, "repo1", graveler.TagID(tag), commitID)
		testutil.MustDo(t, "set tag "+tag, err)
	}

	iter, err := r.ListTags(ctx, "repo1")
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	var commits []graveler.CommitID
	for iter.Next() {
		commits = append(commits, iter.Value().CommitID)
	}
	testutil.MustDo(t, "list tags completed", iter.Err())

	if diff := deep.Equal(commits, commitsTagged); diff != nil {
		t.Fatal("ListTags found mismatch:", diff)
	}
}

func TestManager_AddCommit(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{
		CommitID: "c1",
	}))

	ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z00:00")
	c := graveler.Commit{
		Committer:    "user1",
		Message:      "message1",
		RangeID:      "deadbeef123",
		CreationDate: ts,
		Parents:      graveler.CommitParents{"deadbeef1", "deadbeef12"},
		Metadata:     graveler.Metadata{"foo": "bar"},
	}

	cid, err := r.AddCommit(context.Background(), "repo1", c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cid != "2277b5abd2d3ba6b4d35c48a0e358b0c4bcf5cd6d891c67437fb4c4af0d2fd4b" {
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

func TestManager_Log(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{
		CommitID: "c1",
	}))

	ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z")
	var previous graveler.CommitID
	for i := 0; i < 20; i++ {
		c := graveler.Commit{
			Committer:    "user1",
			Message:      "message1",
			RangeID:      "deadbeef123",
			CreationDate: ts,
			Parents:      graveler.CommitParents{previous},
			Metadata:     graveler.Metadata{"foo": "bar"},
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

	ids := make([]graveler.CommitID, 0)
	for iter.Next() {
		c := iter.Value()
		ids = append(ids, c.CommitID)
	}
	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}

	expected := graveler.CommitParents{
		"c3f815d633789cd7c1325352277d4de528844c758a9beedfa8a3cfcfb5c75627",
		"8549d7544244ba1b63b5967b6b328b331658f627369cb89bd442684719c318ae",
		"13dafa9c45bcf67e6997776039cbf8ab571ace560ce9e13665f383434a495774",
		"7de38592b9e6046ffb55915a40848f05749f168531f0cd6a2aa61fe6e8d92d02",
		"94c7773c89650e99671c33d46e31226230cdaeed79a77cdbd7c7419ea68b91ca",
		"0efcb6e81db6bdd2cfeb77664b6573a7d69f555bbe561fd1fd018a4e4cac7603",
		"d85e4ae46b63f641b439afde9ebab794a3c39c203a42190c0b9d7773ab71a60e",
		"a766cfdb311fe5f18f489d90d283f65ed522e719fe1ad5397277339eee0d1964",
		"67ea954d570e20172775f41ac9763905d16d73490d9b72731d353db33f85d437",
		"d3b16c2cf7f5b9adc2770976bcabe463a5bdd3b5dbf740034f09a9c663620aed",
		"d420fbf793716d6d53798218d7a247f38a5bbed095d57df71ee79e05446e46ec",
		"cc72bda1adade1a72b3de617472c16af187063c79e7edc7921c04e883b44de4c",
		"752581ac60bd8e38a2e65a754591a93a1703dc6c658f91380b8836013188c566",
		"3cf70857454c71fd0bbf69af8a5360671ba98f6ac9371b047144208c58c672a2",
		"bfa1e0382ff3c51905dc62ced0a67588b5219c1bba71a517ae7e7857f0c26afe",
		"d2248dcc1a4de004e10e3bc6b820655e649b8d986d983b60ec98a357a0df194b",
		"a2d98d820f6ff3f221223dbe6a22548f78549830d3b19286b101f13a0ee34085",
		"4f13621ec00d4e44e8a0f0ad340224f9d51db9b6518ee7bef17f598aea9e0431",
		"df87d5329f4438662d6ecb9b90ee17c0bdc9a78a884acc93c0c4fe9f0f79d059",
		"29706d36de7219e0796c31b278f87201ef835e8cdafbcc3c907d292cd31f77d5",
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
