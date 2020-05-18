package index_test

import (
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/testutil"
)

const testBranch = "testBranch"

type Command int

const (
	write Command = iota
	commit
	revertTree
	revertObj
	deleteEntry
)

var (
	pool        *dockertest.Pool
	databaseUri string
)

func TestMain(m *testing.M) {
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	var closer func()
	databaseUri, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

func TestKVIndex_GetCommit(t *testing.T) {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)

	commit, err := kvIndex.Commit(repo.Id, repo.DefaultBranch, "test msg", "committer", nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("get commit", func(t *testing.T) {
		got, err := kvIndex.GetCommit(repo.Id, commit.Address)
		if err != nil {
			t.Fatal(err)
		}
		//compare commitrer
		if commit.Committer != got.Committer {
			t.Errorf("got wrong committer. got:%s, expected:%s", got.Committer, commit.Committer)
		}
		//compare message
		if commit.Message != got.Message {
			t.Errorf("got wrong message. got:%s, expected:%s", got.Message, commit.Message)
		}
	})

	t.Run("get non existing commit - expect error", func(t *testing.T) {
		_, err := kvIndex.GetCommit(repo.Id, "a564356445bdef")
		if !errors.Is(err, db.ErrNotFound) {
			t.Errorf("expected to get not found error for non existing commit")
		}
	})

}

func TestKVIndex_RevertCommit(t *testing.T) {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)

	firstEntry := &model.Entry{
		Name:      "bar",
		EntryType: model.EntryTypeObject,
	}
	err := kvIndex.WriteEntry(repo.Id, repo.DefaultBranch, "", firstEntry)
	if err != nil {
		t.Fatal(err)
	}
	// test read entry not exist when read-uncommitted is false
	_, err = kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, "/bar", false)
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("missing data from requested commit")
	}
	commit, err := kvIndex.Commit(repo.Id, repo.DefaultBranch, "test msg", "committer", nil)
	if err != nil {
		t.Fatal(err)
	}
	commitId := ident.Hash(commit)
	_, err = kvIndex.CreateBranch(repo.Id, testBranch, commitId)
	if err != nil {
		t.Fatal(err)
	}
	secondEntry := &model.Entry{
		Name:      "foo",
		EntryType: model.EntryTypeObject,
	}
	// commit second entry to default branch
	err = kvIndex.WriteEntry(repo.Id, repo.DefaultBranch, "", secondEntry)
	if err != nil {
		t.Fatal(err)
	}
	_, err = kvIndex.Commit(repo.Id, repo.DefaultBranch, "test msg", "committer", nil)
	if err != nil {
		t.Fatal(err)
	}
	//commit second entry to test branch
	err = kvIndex.WriteEntry(repo.Id, testBranch, "", secondEntry)
	if err != nil {
		t.Fatal(err)
	}

	_, err = kvIndex.Commit(repo.Id, testBranch, "test msg", "committer", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = kvIndex.RevertCommit(repo.Id, repo.DefaultBranch, commitId)
	if err != nil {
		t.Fatal(err)
	}

	// test entry1 exists
	te, err := kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, "bar", true)
	if err != nil {
		t.Fatal(err)
	}
	if te.Name != firstEntry.Name {
		t.Fatalf("missing data from requested commit")
	}
	// test secondEntry does not exist
	_, err = kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, "foo", true)
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("missing data from requested commit")
	}

	// test secondEntry exists on test branch
	_, err = kvIndex.ReadEntryObject(repo.Id, testBranch, "foo", true)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
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
			mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
			kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)

			var err error
			for _, action := range tc.Actions {
				_, err = runCommand(kvIndex, repo, action.command, action.path)
				if err != nil {
					if errors.Is(err, tc.ExpectedError) {
						return
					}
					t.Fatal(err)
				}
			}
			if tc.ExpectedError != nil {
				t.Fatalf("expected to get error but did not get any")
			}
			for _, entryPath := range tc.ExpectExisting {
				_, err := kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, entryPath, true)
				if err != nil {
					if errors.Is(err, db.ErrNotFound) {
						t.Fatalf("files added before commit should be available after revert")
					} else {
						t.Fatal(err)
					}
				}
			}
			for _, entryPath := range tc.ExpectMissing {
				_, err := kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, entryPath, true)
				if !errors.Is(err, db.ErrNotFound) {
					t.Fatalf("files added after commit should be removed after revert")
				}
			}
		})
	}
}

func TestKVIndex_DiffWorkspace(t *testing.T) {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)

	err := kvIndex.WriteEntry(repo.Id, repo.DefaultBranch, "foo/bar", &model.Entry{
		Name:         "bar",
		Address:      "123456789",
		CreationDate: time.Now(),
		EntryType:    model.EntryTypeObject,
	})
	if err != nil {
		t.Fatal(err)
	}
	diff, err := kvIndex.DiffWorkspace(repo.Id, repo.DefaultBranch)
	if err != nil {
		t.Fatal(err)
	}
	if len(diff) == 0 {
		t.Errorf("expected diff size to be 1, got 0")
	}
}

func TestKVIndex_DeleteObject(t *testing.T) {
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
			"add and delete",
			[]Action{
				{write, "a/foo"},
				{deleteEntry, "a/foo"},
			},

			nil,
			[]string{"a/foo"},
			nil,
		},
		{
			"delete non existing",
			[]Action{
				{write, "a/bar"},
				{deleteEntry, "a/foo"},
			},

			[]string{"a/bar"},
			[]string{"a/foo"},
			db.ErrNotFound,
		},
		{
			"rewrite deleted",
			[]Action{
				{write, "a/foo"},
				{deleteEntry, "a/foo"},
				{write, "a/foo"},
			},

			[]string{"a/foo"},
			nil,
			nil,
		},
		{
			"included",
			[]Action{
				{write, "a/foo/bar"},
				{write, "a/foo"},
				{write, "a/foo/bar/one"},
				{deleteEntry, "a/foo"},
			},

			[]string{"a/foo/bar", "a/foo/bar/one"},
			[]string{"a/foo"},
			nil,
		},
		{
			"remove from workspace",
			[]Action{
				{write, "a/foo"},
				{write, "a/foo/bar"},
				{deleteEntry, "a/foo"},
				{commit, ""},
			},
			[]string{"a/foo/bar"},
			[]string{"a/foo"},
			nil,
		},
		{
			"remove from workspace and from merkle",
			[]Action{
				{write, "a/foo"},
				{commit, ""},
				{write, "a/foo"},
				{write, "a/foo/bar"},
				{deleteEntry, "a/foo"},
				{commit, ""},
			},
			[]string{"a/foo/bar"},
			[]string{"a/foo"},
			nil,
		},
		{
			"remove from twice from merkle before partial commit",
			[]Action{
				{write, "a/foo"},
				{commit, ""},
				{write, "a/foo"},
				{write, "a/foo/bar"},
				{deleteEntry, "a/foo"},
				{deleteEntry, "a/foo"},
				{commit, ""},
			},
			[]string{"a/foo/bar"},
			[]string{"a/foo"},
			db.ErrNotFound,
		},
	}

	for _, tc := range testData {
		t.Run(tc.Name, func(t *testing.T) {
			mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
			kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)
			var err error
			for _, action := range tc.Actions {
				_, err = runCommand(kvIndex, repo, action.command, action.path)
				if err != nil {
					if errors.Is(err, tc.ExpectedError) {
						return
					}
					t.Fatal(err)
				}
			}
			if tc.ExpectedError != nil {
				t.Fatalf("expected to get error but did not get any")
			}
			for _, entryPath := range tc.ExpectExisting {
				_, err := kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, entryPath, true)
				if err != nil {
					if errors.Is(err, db.ErrNotFound) {
						t.Fatalf("files added before commit should be available after revert")
					} else {
						t.Fatal(err)
					}
				}
			}
			for _, entryPath := range tc.ExpectMissing {
				_, err := kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, entryPath, true)
				if !errors.Is(err, db.ErrNotFound) {
					t.Fatalf("files added after commit should be removed after revert")
				}
			}
		})
	}
}

func runCommand(kvIndex index.Index, repo *model.Repo, command Command, actionPath string) (retVal string, err error) {
	switch command {
	case write:
		err = kvIndex.WriteEntry(repo.Id, repo.DefaultBranch, actionPath, &model.Entry{
			Name:         path.New(actionPath, model.EntryTypeObject).BaseName(),
			Address:      "123456789",
			CreationDate: time.Now(),
			EntryType:    model.EntryTypeObject,
		})

	case commit:
		var commit *model.Commit
		commit, err = kvIndex.Commit(repo.Id, repo.DefaultBranch, "test msg", "committer", nil)
		if err == nil && commit != nil {
			retVal = commit.Address
		}

	case revertTree:
		err = kvIndex.RevertPath(repo.Id, repo.DefaultBranch, actionPath)

	case revertObj:
		err = kvIndex.RevertObject(repo.Id, repo.DefaultBranch, actionPath)

	case deleteEntry:
		err = kvIndex.DeleteObject(repo.Id, repo.DefaultBranch, actionPath)

	default:
		err = errors.New("unknown command")
	}
	return
}
