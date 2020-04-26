package index_test

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"

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
		if !xerrors.Is(err, db.ErrNotFound) {
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
	te, err := kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, "bar")
	if err != nil {
		t.Fatal(err)
	}
	if te.Name != firstEntry.Name {
		t.Fatalf("missing data from requested commit")
	}
	// test secondEntry does not exist
	_, err = kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, "foo")
	if !xerrors.Is(err, db.ErrNotFound) {
		t.Fatalf("missing data from requested commit")
	}

	// test secondEntry exists on test branch
	_, err = kvIndex.ReadEntryObject(repo.Id, testBranch, "foo")
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
			mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
			kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)

			var err error
			for _, action := range tc.Actions {
				err = runCommand(kvIndex, repo, action.command, action.path)
				if err != nil {
					if xerrors.Is(err, tc.ExpectedError) {
						return
					}
					t.Fatal(err)
				}
			}
			if tc.ExpectedError != nil {
				t.Fatalf("expected to get error but did not get any")
			}
			for _, entryPath := range tc.ExpectExisting {
				_, err := kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, entryPath)
				if err != nil {
					if xerrors.Is(err, db.ErrNotFound) {
						t.Fatalf("files added before commit should be available after revert")
					} else {
						t.Fatal(err)
					}
				}
			}
			for _, entryPath := range tc.ExpectMissing {
				_, err := kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, entryPath)
				if !xerrors.Is(err, db.ErrNotFound) {
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

func TestKVIndex_ListObjectsByPrefix(t *testing.T) {
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

	entries, _, err := kvIndex.ListObjectsByPrefix(repo.Id, repo.DefaultBranch, "/", "", -1, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Errorf("expected entries size to be 1, got 0")
	}

}

func TestKVIndex_DeleteObject(t *testing.T) {
	type Action struct {
		command Command
		path    string
	}
	testData := []struct {
		Name               string
		partialCommitRatio float32
		Actions            []Action
		ExpectExisting     []string
		ExpectMissing      []string
		ExpectedError      error
	}{
		{
			"add and delete",
			1,
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
			1,
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
			1,
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
			1,
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
			0,
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
			0,
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
			0,
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
				err = runCommand(kvIndex, repo, action.command, action.path)
				if err != nil {
					if xerrors.Is(err, tc.ExpectedError) {
						return
					}
					t.Fatal(err)
				}
			}
			if tc.ExpectedError != nil {
				t.Fatalf("expected to get error but did not get any")
			}
			for _, entryPath := range tc.ExpectExisting {
				_, err := kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, entryPath)
				if err != nil {
					if xerrors.Is(err, db.ErrNotFound) {
						t.Fatalf("files added before commit should be available after revert")
					} else {
						t.Fatal(err)
					}
				}
			}
			for _, entryPath := range tc.ExpectMissing {
				_, err := kvIndex.ReadEntryObject(repo.Id, repo.DefaultBranch, entryPath)
				if !xerrors.Is(err, db.ErrNotFound) {
					t.Fatalf("files added after commit should be removed after revert")
				}
			}
		})
	}
}

func TestSizeConsistency(t *testing.T) {
	type Object struct {
		name string
		path string
		size int64
	}
	type Tree struct {
		name       string
		wantedSize int64
	}
	testData := []struct {
		name           string
		objectList     []Object
		wantedTrees    []Tree
		wantedRootSize int64
	}{
		{
			name: "simple case",
			objectList: []Object{
				{"file1", "a/file1", 100},
				{"file2", "a/file2", 100},
				{"file3", "a/file3", 100},
				{"file4", "a/file4", 100},
				{"file5", "a/file5", 100},
			},
			wantedTrees: []Tree{
				{"a/", 500},
			},
			wantedRootSize: 500,
		},
		{
			name: "two separate trees",
			objectList: []Object{
				{"file1", "a/file1", 100},
				{"file2", "a/file2", 100},
				{"file3", "b/file3", 100},
				{"file4", "b/file4", 100},
				{"file5", "b/file5", 100},
			},
			wantedTrees: []Tree{
				{"a/", 200},
				{"b/", 300},
			},
			wantedRootSize: 500,
		},
		{
			name: "two sub trees",
			objectList: []Object{
				{"file1", "parent/a/file1", 100},
				{"file2", "parent/a/file2", 100},
				{"file3", "parent/b/file3", 100},
				{"file4", "parent/b/file4", 100},
				{"file5", "parent/b/file5", 100},
			},
			wantedTrees: []Tree{
				{"parent/a/", 200},
				{"parent/b/", 300},
				{"parent/", 500},
			},
			wantedRootSize: 500,
		},
		{
			name: "deep sub trees",
			objectList: []Object{
				{"file1", "a/file1", 100},
				{"file2", "a/b/file2", 100},
				{"file3", "a/b/c/file3", 100},
				{"file4", "a/b/c/d/file4", 100},
				{"file5", "a/b/c/d/e/file5", 100},
			},
			wantedTrees: []Tree{
				{"a/", 500},
				{"a/b/", 400},
				{"a/b/c/", 300},
				{"a/b/c/d/", 200},
				{"a/b/c/d/e/", 100},
			},
			wantedRootSize: 500,
		},
	}

	for _, tc := range testData {
		t.Run(tc.name, func(t *testing.T) {
			mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
			kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)

			for _, object := range tc.objectList {
				err := kvIndex.WriteEntry(repo.Id, repo.DefaultBranch, object.path, &model.Entry{
					Name:      object.name,
					Size:      object.size,
					EntryType: model.EntryTypeObject,
				})
				if err != nil {
					t.Fatal(err)
				}
			}

			//force partial commit
			_, err := kvIndex.Commit(repo.Id, repo.DefaultBranch, "message", "committer", nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, tree := range tc.wantedTrees {
				entry, err := kvIndex.ReadEntryTree(repo.Id, repo.DefaultBranch, tree.name)
				if err != nil {
					t.Fatal(err)
				}

				// test entry size
				if entry.Size != tree.wantedSize {
					t.Errorf("did not get the expected size for directory %s want: %d, got: %d", tree.name, tree.wantedSize, entry.Size)
				}
			}

			rootObject, err := kvIndex.ReadRootObject(repo.Id, repo.DefaultBranch)
			if err != nil {
				t.Fatal(err)
			}
			if rootObject.Size != tc.wantedRootSize {
				t.Errorf("did not get the expected size for root want: %d, got: %d", tc.wantedRootSize, rootObject.Size)
			}
		})
	}
}

func TestTimeStampConsistency(t *testing.T) {
	type timedObject struct {
		path    string
		name    string
		seconds time.Duration
	}
	type expectedTree struct {
		path    string
		seconds time.Duration
	}
	testData := []struct {
		name           string
		timedObjects   []timedObject
		deleteObjects  []timedObject
		expectedTrees  []expectedTree
		expectedRootTS time.Duration
	}{
		{
			timedObjects:   []timedObject{{"a/foo", "foo", 10}, {"a/bar", "bar", 20}},
			expectedTrees:  []expectedTree{{"a/", 20}},
			expectedRootTS: 20,
		},
		{
			timedObjects:   []timedObject{{"a/wow", "wow", 5}, {"a/c/bar", "bar", 10}, {"a/b/bar", "bar", 20}},
			expectedTrees:  []expectedTree{{"a/", 20}, {"a/c/", 10}, {"a/b/", 20}},
			expectedRootTS: 20,
		},
		{
			name:           "delete file",
			timedObjects:   []timedObject{{"a/wow", "wow", 5}, {"a/c/bar", "bar", 10}, {"a/c/foo", "foo", 15}, {"a/b/bar", "bar", 20}},
			deleteObjects:  []timedObject{{"a/c/bar", "bar", 25}},
			expectedTrees:  []expectedTree{{"a/", 25}, {"a/c/", 25}, {"a/b/", 20}},
			expectedRootTS: 25,
		},
	}

	for _, tc := range testData {
		t.Run(tc.name, func(t *testing.T) {
			mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
			kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)

			now := time.Now()
			currentTime := now
			mockTime := func() time.Time {
				return currentTime
			}
			kvIndex = index.NewDBIndex(mdb, index.WithTimeGenerator(mockTime))
			for _, obj := range tc.timedObjects {
				ts := now.Add(obj.seconds * time.Second)
				err := kvIndex.WriteEntry(repo.Id, repo.DefaultBranch, obj.path, &model.Entry{
					Name:         obj.name,
					Address:      "12345678",
					EntryType:    model.EntryTypeObject,
					CreationDate: ts,
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, obj := range tc.deleteObjects {
				currentTime = now.Add(obj.seconds * time.Second)
				err := kvIndex.DeleteObject(repo.Id, repo.DefaultBranch, obj.path)
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, tree := range tc.expectedTrees {
				entry, err := kvIndex.ReadEntryTree(repo.Id, repo.DefaultBranch, tree.path)
				if err != nil {
					t.Fatal(err)
				}
				expectedTS := now.Add(tree.seconds * time.Second)
				if entry.CreationDate.Unix() != expectedTS.Unix() {
					t.Errorf("unexpected times stamp for tree, expected: %v , got: %v", expectedTS, entry.CreationDate)
				}
			}

			rootObject, err := kvIndex.ReadRootObject(repo.Id, repo.DefaultBranch)
			if err != nil {
				t.Fatal(err)
			}
			expectedTS := now.Add(tc.expectedRootTS * time.Second)
			if rootObject.CreationDate.Unix() != expectedTS.Unix() {
				t.Errorf("unexpected times stamp for tree, expected: %v , got: %v", expectedTS, rootObject.CreationDate)
			}

		})
	}
}

func runCommand(kvIndex index.Index, repo *model.Repo, command Command, actionPath string) error {
	var err error
	switch command {
	case write:
		err = kvIndex.WriteEntry(repo.Id, repo.DefaultBranch, actionPath, &model.Entry{
			Name:         path.New(actionPath, model.EntryTypeObject).BaseName(),
			Address:      "123456789",
			CreationDate: time.Now(),
			EntryType:    model.EntryTypeObject,
		})

	case commit:
		_, err = kvIndex.Commit(repo.Id, repo.DefaultBranch, "test msg", "committer", nil)

	case revertTree:
		err = kvIndex.RevertPath(repo.Id, repo.DefaultBranch, actionPath)

	case revertObj:
		err = kvIndex.RevertObject(repo.Id, repo.DefaultBranch, actionPath)

	case deleteEntry:
		err = kvIndex.DeleteObject(repo.Id, repo.DefaultBranch, actionPath)

	default:
		err = xerrors.Errorf("unknown command")
	}
	return err
}
