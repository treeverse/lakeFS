package index_test

import (
	"testing"

	"github.com/treeverse/lakefs/index/path"

	"github.com/treeverse/lakefs/ident"

	"github.com/treeverse/lakefs/db"
	"golang.org/x/xerrors"

	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/testutil"

	"github.com/treeverse/lakefs/index"
)

const testBranch = "testBranch"

type Command int

const (
	write Command = iota
	commit
	revertPath
	revertObj
)

func TestKVIndex_RevertCommit(t *testing.T) {
	kv, repo, closer := testutil.GetIndexStoreWithRepo(t)
	defer closer()

	kvIndex := index.NewKVIndex(kv)
	firstEntry := &model.Entry{
		Name: "bar",
	}
	err := kvIndex.WriteEntry(repo.RepoId, repo.DefaultBranch, "", firstEntry)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := kvIndex.Commit(repo.RepoId, repo.DefaultBranch, "test msg", "committer", nil)
	if err != nil {
		t.Fatal(err)
	}
	commitId := ident.Hash(commit)
	err = kvIndex.CreateBranch(repo.RepoId, testBranch, commitId)
	if err != nil {
		t.Fatal(err)
	}
	secondEntry := &model.Entry{
		Name: "foo",
	}
	// commit second entry to default branch
	err = kvIndex.WriteEntry(repo.RepoId, repo.DefaultBranch, "", secondEntry)
	if err != nil {
		t.Fatal(err)
	}
	_, err = kvIndex.Commit(repo.RepoId, repo.DefaultBranch, "test msg", "committer", nil)
	if err != nil {
		t.Fatal(err)
	}
	//commit second entry to test branch
	err = kvIndex.WriteEntry(repo.RepoId, testBranch, "", secondEntry)
	if err != nil {
		t.Fatal(err)
	}
	_, err = kvIndex.Commit(repo.RepoId, testBranch, "test msg", "committer", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = kvIndex.RevertCommit(repo.RepoId, repo.DefaultBranch, commitId)
	if err != nil {
		t.Fatal(err)
	}

	// test entry1 exists
	te, err := kvIndex.ReadEntry(repo.RepoId, repo.DefaultBranch, "bar")
	if err != nil {
		t.Fatal(err)
	}
	if te.Name != firstEntry.Name {
		t.Fatalf("missing data from requested commit")
	}
	// test secondEntry does not exist
	_, err = kvIndex.ReadEntry(repo.RepoId, repo.DefaultBranch, "foo")
	if !xerrors.Is(err, db.ErrNotFound) {
		t.Fatalf("missing data from requested commit")
	}

	// test secondEntry exists on test branch
	_, err = kvIndex.ReadEntry(repo.RepoId, testBranch, "foo")
	if err != nil {
		if xerrors.Is(err, db.ErrNotFound) {
			t.Fatalf("errased data from test branch after revert from defult branch")
		} else {
			t.Fatal(err)
		}
	}

	//test no commit
}

func TestKVIndex_RevertPath(t *testing.T) {

	type Action struct {
		command Command
		path    string
	}
	testData := []struct {
		Name           string
		Actions        []Action
		ExpectExisting []string
		ExpectMissing  []string
	}{
		{
			"commit and revert",
			[]Action{
				{write, "a/foo"},
				{commit, ""},
				{write, "a/bar"},
				{revertPath, "a/"},
			},

			[]string{"a/foo"},
			[]string{"a/bar"},
		},
		{
			"reset - commit and revert on root",
			[]Action{
				{write, "foo"},
				{commit, ""},
				{write, "bar"},
				{revertPath, ""},
			},
			[]string{"foo"},
			[]string{"bar"},
		},
		{
			"only revert",
			[]Action{
				{write, "foo"},
				{write, "a/foo"},
				{write, "a/bar"},
				{revertPath, "a/"},
			},
			[]string{"foo"},
			[]string{"a/bar", "a/foo"},
		},
		{
			"only revert different path",
			[]Action{
				{write, "a/foo"},
				{write, "b/bar"},
				{revertPath, "a/"},
			},
			[]string{"b/bar"},
			[]string{"a/bar", "a/foo"},
		},
		{
			"revert on Object",
			[]Action{
				{write, "a/foo"},
				{write, "a/bar"},
				{revertObj, "a/foo"},
			},
			[]string{"a/bar"},
			[]string{"a/foo"},
		},
	}

	for _, tc := range testData {
		t.Run(tc.Name, func(t *testing.T) {
			kv, repo, closer := testutil.GetIndexStoreWithRepo(t)
			defer closer()

			kvIndex := index.NewKVIndex(kv)
			for _, action := range tc.Actions {
				switch action.command {
				case write:
					err := kvIndex.WriteEntry(repo.RepoId, repo.DefaultBranch, action.path, &model.Entry{
						Name: path.New(action.path).Basename(),
						Type: model.Entry_OBJECT,
					})
					if err != nil {
						t.Fatal(err)
					}
				case commit:
					_, err := kvIndex.Commit(repo.RepoId, repo.DefaultBranch, "test msg", "committer", nil)
					if err != nil {
						t.Fatal(err)
					}

				case revertPath:
					err := kvIndex.RevertPath(repo.RepoId, repo.DefaultBranch, action.path)
					if err != nil {
						t.Fatal(err)
					}
				case revertObj:
					err := kvIndex.RevertObject(repo.RepoId, repo.DefaultBranch, action.path)
					if err != nil {
						t.Fatal(err)
					}
				}
				t.Fatal(xerrors.Errorf("unknown command"))
			}
			for _, path := range tc.ExpectExisting {
				_, err := kvIndex.ReadEntry(repo.RepoId, repo.DefaultBranch, path)
				if err != nil {
					if xerrors.Is(err, db.ErrNotFound) {
						t.Fatalf("files added before commit should be available after revert")
					} else {
						t.Fatal(err)
					}
				}
			}
			for _, path := range tc.ExpectMissing {
				_, err := kvIndex.ReadEntry(repo.RepoId, repo.DefaultBranch, path)
				if !xerrors.Is(err, db.ErrNotFound) {
					t.Fatalf("files added after commit should be removed after revert")
				}
			}
		})
	}

	// test non existing path

	// test no branch

	//
}
