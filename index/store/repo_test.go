package store_test

import (
	"testing"
	"time"

	"github.com/treeverse/lakefs/testutil"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/store"

	"golang.org/x/xerrors"
)

func TestKVRepoReadOnlyOperations_ListBranches(t *testing.T) {
	kv, repo, closer := testutil.GetIndexStoreWithRepo(t, 1)
	defer closer()

	kv.RepoTransact(repo.GetRepoId(), func(ops store.RepoOperations) (i interface{}, e error) {
		master, err := ops.ReadBranch("master")
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteBranch("dev-work1", &model.Branch{
			Name:       "dev-work1",
			Commit:     master.GetCommit(),
			CommitRoot: master.GetCommitRoot(),
		})
		if err != nil {
			t.Fatal(err)
		}

		err = ops.WriteBranch("dev-work2", &model.Branch{
			Name:       "dev-work2",
			Commit:     master.GetCommit(),
			CommitRoot: master.GetCommitRoot(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return nil, nil
	})

	kv.RepoTransact(repo.GetRepoId(), func(ops store.RepoOperations) (i interface{}, e error) {
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

func TestKVRepoOperations_ClearWorkspace(t *testing.T) {
	kv, repo, closer := testutil.GetIndexStoreWithRepo(t, 1)
	defer closer()

	n := time.Now().Unix()

	kv.RepoTransact(repo.GetRepoId(), func(ops store.RepoOperations) (i interface{}, e error) {
		var err error
		err = ops.WriteToWorkspacePath(repo.GetDefaultBranch(), "/foo/bar", &model.WorkspaceEntry{
			Path: "/foo/bar",
			Entry: &model.Entry{
				Name:      "bar",
				Address:   "d41d8cd98f00b204e9800998ecf8427e",
				Type:      model.Entry_OBJECT,
				Timestamp: n,
				Checksum:  "d41d8cd98f00b204e9800998ecf8427e",
			},
		})

		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteToWorkspacePath(repo.GetDefaultBranch(), "/foo/baz/bar", &model.WorkspaceEntry{
			Path: "/foo/baz/bar",
			Entry: &model.Entry{
				Name:      "bar",
				Address:   "d41d8cd98f00b204e9800998ecf8427e",
				Type:      model.Entry_OBJECT,
				Timestamp: n,
				Checksum:  "d41d8cd98f00b204e9800998ecf8427e",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteToWorkspacePath(repo.GetDefaultBranch(), "/foo/baz/barrrr", &model.WorkspaceEntry{
			Path: "/foo/baz/barrrr",
			Entry: &model.Entry{
				Name:      "barrrr",
				Address:   "d41d8cd98f00b204e9800998ecf8427e",
				Type:      model.Entry_OBJECT,
				Timestamp: n,
				Checksum:  "d41d8cd98f00b204e9800998ecf8427e",
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		wsEntries, err := ops.ListWorkspace(repo.GetDefaultBranch())
		if err != nil {
			t.Fatal(err)
		}

		if len(wsEntries) != 3 {
			t.Fatalf("expected 3 workspace entries, got %d", len(wsEntries))
		}

		return nil, nil
	})

	kv.RepoTransact(repo.GetRepoId(), func(ops store.RepoOperations) (i interface{}, e error) {
		wsEntries, err := ops.ListWorkspace(repo.GetDefaultBranch())
		if err != nil {
			t.Fatal(err)
		}
		if len(wsEntries) != 3 {
			t.Fatalf("expected 3 workspace entries, got %d", len(wsEntries))
		}
		err = ops.ClearWorkspace(repo.GetDefaultBranch())
		if err != nil {
			t.Fatal(err)
		}
		wsEntries, err = ops.ListWorkspace(repo.GetDefaultBranch())
		if err != nil {
			t.Fatal(err)
		}

		if len(wsEntries) != 0 {
			t.Fatalf("expected 0 workspace entries, got %d", len(wsEntries))
		}
		return nil, nil
	})

	kv.RepoTransact(repo.GetRepoId(), func(ops store.RepoOperations) (i interface{}, e error) {
		wsEntries, err := ops.ListWorkspace(repo.GetDefaultBranch())
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
	kv, repo, closer := testutil.GetIndexStoreWithRepo(t, 1)
	defer closer()

	n := time.Now().Unix()

	_, err := kv.RepoTransact(repo.GetRepoId(), func(ops store.RepoOperations) (i interface{}, e error) {
		var err error
		err = ops.WriteToWorkspacePath(repo.GetDefaultBranch(), "/foo/bar", &model.WorkspaceEntry{
			Path: "/foo/bar",
			Entry: &model.Entry{
				Name:      "bar",
				Address:   "d41d8cd98f00b204e9800998ecf8427e",
				Type:      model.Entry_OBJECT,
				Timestamp: n,
				Checksum:  "d41d8cd98f00b204e9800998ecf8427e",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteToWorkspacePath(repo.GetDefaultBranch(), "/foo/baz/bar", &model.WorkspaceEntry{
			Path: "/foo/baz/bar",
			Entry: &model.Entry{
				Name:      "bar",
				Address:   "d41d8cd98f00b204e9800998ecf8427e",
				Type:      model.Entry_OBJECT,
				Timestamp: n,
				Checksum:  "d41d8cd98f00b204e9800998ecf8427e",
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = ops.ReadFromWorkspace(repo.GetDefaultBranch(), "/foo/bbbbb")
		if !xerrors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected a not found error got %v instead", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
