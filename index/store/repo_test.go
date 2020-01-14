package store_test

import (
	"testing"
	"time"
	"treeverse-lake/db"
	"treeverse-lake/ident"
	"treeverse-lake/index"
	"treeverse-lake/index/model"
	"treeverse-lake/index/store"

	"golang.org/x/xerrors"
)

const TimeFormat = "Jan 2 15:04:05 2006 -0700"

func GetIndexStoreWithRepo(t *testing.T) (store.Store, *model.Repo, func()) {
	kv, closer := GetIndexStore(t)
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

func TestKVRepoReadOnlyOperations_ListBranches(t *testing.T) {
	kv, repo, closer := GetIndexStoreWithRepo(t)
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
		branches, err := ops.ListBranches()
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
	kv, repo, closer := GetIndexStoreWithRepo(t)
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
	kv, repo, closer := GetIndexStoreWithRepo(t)
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
