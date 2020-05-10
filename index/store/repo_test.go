package store_test

import (
	"errors"
	"testing"
	"time"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/store"
	"github.com/treeverse/lakefs/testutil"
)

func TestKVRepoReadOnlyOperations_ListBranches(t *testing.T) {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	str := store.NewDBStore(mdb)
	_, repo := testutil.GetIndexWithRepo(t, mdb)

	str.RepoTransact(repo.Id, func(ops store.RepoOperations) (i interface{}, e error) {
		master, err := ops.ReadBranch("master")
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteBranch("dev-work1", &model.Branch{
			Id:         "dev-work1",
			CommitId:   master.CommitId,
			CommitRoot: master.CommitRoot,
		})
		if err != nil {
			t.Fatal(err)
		}

		err = ops.WriteBranch("dev-work2", &model.Branch{
			Id:         "dev-work2",
			CommitId:   master.CommitId,
			CommitRoot: master.CommitRoot,
		})
		if err != nil {
			t.Fatal(err)
		}
		return nil, nil
	})

	str.RepoTransact(repo.Id, func(ops store.RepoOperations) (i interface{}, e error) {
		branches, _, err := ops.ListBranches("", -1, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(branches) != 3 {
			t.Fatalf("expected master + 2 branches (3), got %d", len(branches))
		}
		return nil, nil
	})
}

func dstr(d string) *string {
	return &d
}

func TestKVRepoOperations_ClearWorkspace(t *testing.T) {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	str := store.NewDBStore(mdb)
	_, repo := testutil.GetIndexWithRepo(t, mdb)

	n := time.Now()

	str.RepoTransact(repo.Id, func(ops store.RepoOperations) (i interface{}, e error) {
		var err error
		err = ops.WriteToWorkspacePath(repo.DefaultBranch, "/foo/", "/foo/bar", &model.WorkspaceEntry{
			Path:              "/foo/bar",
			EntryName:         dstr("bar"),
			EntryAddress:      dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryType:         dstr(model.EntryTypeObject),
			EntryChecksum:     dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryCreationDate: &n,
		})

		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteToWorkspacePath(repo.DefaultBranch, "/foo/baz/", "/foo/baz/bar", &model.WorkspaceEntry{
			Path:              "/foo/baz/bar",
			EntryName:         dstr("bar"),
			EntryAddress:      dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryType:         dstr(model.EntryTypeObject),
			EntryCreationDate: &n,
			EntryChecksum:     dstr("d41d8cd98f00b204e9800998ecf8427e"),
		})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteToWorkspacePath(repo.DefaultBranch, "/foo/baz/", "/foo/baz/barrrr", &model.WorkspaceEntry{
			Path:              "/foo/baz/barrrr",
			EntryName:         dstr("barrrr"),
			EntryAddress:      dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryType:         dstr(model.EntryTypeObject),
			EntryCreationDate: &n,
			EntryChecksum:     dstr("d41d8cd98f00b204e9800998ecf8427e"),
		})
		if err != nil {
			t.Fatal(err)
		}

		wsEntries, err := ops.ListWorkspace(repo.DefaultBranch)
		if err != nil {
			t.Fatal(err)
		}

		if len(wsEntries) != 3 {
			t.Fatalf("expected 3 workspace entries, got %d", len(wsEntries))
		}

		return nil, nil
	})

	str.RepoTransact(repo.Id, func(ops store.RepoOperations) (i interface{}, e error) {
		wsEntries, err := ops.ListWorkspace(repo.DefaultBranch)
		if err != nil {
			t.Fatal(err)
		}
		if len(wsEntries) != 3 {
			t.Fatalf("expected 3 workspace entries, got %d", len(wsEntries))
		}
		err = ops.ClearWorkspace(repo.DefaultBranch)
		if err != nil {
			t.Fatal(err)
		}
		wsEntries, err = ops.ListWorkspace(repo.DefaultBranch)
		if err != nil {
			t.Fatal(err)
		}

		if len(wsEntries) != 0 {
			t.Fatalf("expected 0 workspace entries, got %d", len(wsEntries))
		}
		return nil, nil
	})

	str.RepoTransact(repo.Id, func(ops store.RepoOperations) (i interface{}, e error) {
		wsEntries, err := ops.ListWorkspace(repo.DefaultBranch)
		if err != nil {
			t.Fatal(err)
		}

		if len(wsEntries) != 0 {
			t.Fatalf("expected 0 workspace entries, got %d", len(wsEntries))
		}
		return nil, nil
	})

}

func TestKVRepoReadOnlyOperations_ReadFromWorkspace(t *testing.T) {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	str := store.NewDBStore(mdb)
	_, repo := testutil.GetIndexWithRepo(t, mdb)

	n := time.Now()

	_, err := str.RepoTransact(repo.Id, func(ops store.RepoOperations) (i interface{}, e error) {
		var err error
		err = ops.WriteToWorkspacePath(repo.DefaultBranch, "foo", "/foo/bar", &model.WorkspaceEntry{
			Path:              "/foo/bar",
			EntryName:         dstr("bar"),
			EntryAddress:      dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryType:         dstr(model.EntryTypeObject),
			EntryCreationDate: &n,
			EntryChecksum:     dstr("d41d8cd98f00b204e9800998ecf8427e"),
		})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteToWorkspacePath(repo.DefaultBranch, "/foo/baz/", "/foo/baz/bar", &model.WorkspaceEntry{
			Path:              "/foo/baz/bar",
			EntryName:         dstr("bar"),
			EntryAddress:      dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryType:         dstr(model.EntryTypeObject),
			EntryCreationDate: &n,
			EntryChecksum:     dstr("d41d8cd98f00b204e9800998ecf8427e"),
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = ops.ReadFromWorkspace(repo.DefaultBranch, "/foo/bbbbb")
		if !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected a not found error got %v instead", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
