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
	revertTree
	revertObj
)

func TestKVIndex_RevertCommit(t *testing.T) {
	kv, repo, closer := testutil.GetIndexStoreWithRepo(t)
	defer closer()

	kvIndex := index.NewKVIndex(kv)
	firstEntry := &model.Entry{
		Name: "bar",
		Type: model.Entry_OBJECT,
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
		Type: model.Entry_OBJECT,
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
		ExpectedError  error
	}{
		{
			"commit and revert",
			[]Action{
				{write, "a/foo"},
				{commit, ""},
				{write, "a/bar"},
				{revertTree, "a/"},
			},

			[]string{"a/foo"},
			[]string{"a/bar"},
			nil,
		},
		{
			"reset - commit and revert on root",
			[]Action{
				{write, "foo"},
				{commit, ""},
				{write, "bar"},
				{revertTree, ""},
			},
			[]string{"foo"},
			[]string{"bar"},
			nil,
		},
		{
			"only revert",
			[]Action{
				{write, "foo"},
				{write, "a/foo"},
				{write, "a/bar"},
				{revertTree, "a/"},
			},
			[]string{"foo"},
			[]string{"a/bar", "a/foo"},
			nil,
		},
		{
			"only revert different path",
			[]Action{
				{write, "a/foo"},
				{write, "b/bar"},
				{revertTree, "a/"},
			},
			[]string{"b/bar"},
			[]string{"a/bar", "a/foo"},
			nil,
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
			nil,
		},
		{
			"revert non existing object",
			[]Action{
				{write, "a/foo"},
				{revertObj, "a/bar"},
			},
			nil,
			nil,
			db.ErrNotFound,
		},
		{
			"revert non existing tree",
			[]Action{
				{write, "a/foo"},
				{revertTree, "b/"},
			},
			nil,
			nil,
			db.ErrNotFound,
		},
	}

	for _, tc := range testData {
		t.Run(tc.Name, func(t *testing.T) {
			kv, repo, closer := testutil.GetIndexStoreWithRepo(t)
			defer closer()

			kvIndex := index.NewKVIndex(kv)
			var err error
			for _, action := range tc.Actions {
				switch action.command {
				case write:
					err = kvIndex.WriteEntry(repo.RepoId, repo.DefaultBranch, action.path, &model.Entry{
						Name: path.New(action.path).Basename(),
						Type: model.Entry_OBJECT,
					})

				case commit:
					_, err = kvIndex.Commit(repo.RepoId, repo.DefaultBranch, "test msg", "committer", nil)

				case revertTree:
					err = kvIndex.RevertPath(repo.RepoId, repo.DefaultBranch, action.path)

				case revertObj:
					err = kvIndex.RevertObject(repo.RepoId, repo.DefaultBranch, action.path)

				default:
					t.Fatal(xerrors.Errorf("unknown command"))
				}
			}
			if err != nil {
				if xerrors.Is(err, tc.ExpectedError) {
					return
				}
				t.Fatal(err)
			}
			if tc.ExpectedError != nil {
				t.Fatalf("expected to get error but did not get any")
			}
			for _, entryPath := range tc.ExpectExisting {
				_, err := kvIndex.ReadEntry(repo.RepoId, repo.DefaultBranch, entryPath)
				if err != nil {
					if xerrors.Is(err, db.ErrNotFound) {
						t.Fatalf("files added before commit should be available after revert")
					} else {
						t.Fatal(err)
					}
				}
			}
			for _, entryPath := range tc.ExpectMissing {
				_, err := kvIndex.ReadEntry(repo.RepoId, repo.DefaultBranch, entryPath)
				if !xerrors.Is(err, db.ErrNotFound) {
					t.Fatalf("files added after commit should be removed after revert")
				}
			}
		})
	}
}
