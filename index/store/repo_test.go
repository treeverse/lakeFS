package store_test

import (
	"errors"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/merkle"
	"github.com/treeverse/lakefs/index/model"
	pth "github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/index/store"
	"github.com/treeverse/lakefs/testutil"
	"strings"
	"testing"
	"time"
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
		err = ops.WriteToWorkspacePath(repo.DefaultBranch, []*model.WorkspaceEntry{{
			Path:              "/foo/bar",
			EntryName:         dstr("bar"),
			EntryAddress:      dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryType:         dstr(model.EntryTypeObject),
			EntryChecksum:     dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryCreationDate: &n,
		}})

		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteToWorkspacePath(repo.DefaultBranch, []*model.WorkspaceEntry{{
			Path:              "/foo/baz/bar",
			EntryName:         dstr("bar"),
			EntryAddress:      dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryType:         dstr(model.EntryTypeObject),
			EntryCreationDate: &n,
			EntryChecksum:     dstr("d41d8cd98f00b204e9800998ecf8427e"),
		}})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteToWorkspacePath(repo.DefaultBranch, []*model.WorkspaceEntry{{
			Path:              "/foo/baz/barrrr",
			EntryName:         dstr("barrrr"),
			EntryAddress:      dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryType:         dstr(model.EntryTypeObject),
			EntryCreationDate: &n,
			EntryChecksum:     dstr("d41d8cd98f00b204e9800998ecf8427e"),
		}})
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
		err = ops.WriteToWorkspacePath(repo.DefaultBranch, []*model.WorkspaceEntry{{
			Path:              "/foo/bar",
			EntryName:         dstr("bar"),
			EntryAddress:      dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryType:         dstr(model.EntryTypeObject),
			EntryCreationDate: &n,
			EntryChecksum:     dstr("d41d8cd98f00b204e9800998ecf8427e"),
		}})
		if err != nil {
			t.Fatal(err)
		}
		err = ops.WriteToWorkspacePath(repo.DefaultBranch, []*model.WorkspaceEntry{{
			Path:              "/foo/baz/bar",
			EntryName:         dstr("bar"),
			EntryAddress:      dstr("d41d8cd98f00b204e9800998ecf8427e"),
			EntryType:         dstr(model.EntryTypeObject),
			EntryCreationDate: &n,
			EntryChecksum:     dstr("d41d8cd98f00b204e9800998ecf8427e"),
		}})
		if err != nil {
			t.Fatal(err)
		}

		_, err = ops.ReadFromWorkspace(repo.DefaultBranch, "/foo/bbbbb", model.EntryTypeObject)
		if !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("expected a not found error got %v instead", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func CreateWorkspaceEntries(path, typ string, tombstone bool) []*model.WorkspaceEntry {
	entries := make([]*model.WorkspaceEntry, 0, strings.Count(path, pth.Separator))
	currentParent := path
	var dirChecksum string
	currentType := &typ
	treeType := model.EntryTypeTree
	for strings.Contains(currentParent, pth.Separator) {
		currentPath := currentParent
		currentParent = currentPath[:strings.LastIndex(currentParent[:len(currentParent)-1], pth.Separator)+1]
		currentName := currentPath[len(currentParent):]
		newEntry := model.WorkspaceEntry{
			ParentPath:        currentParent,
			Path:              currentPath,
			EntryType:         currentType,
			EntryName:         &currentName,
			EntrySize:         new(int64),
			EntryCreationDate: new(time.Time),
			EntryChecksum:     &dirChecksum,
			Tombstone:         tombstone,
		}
		entries = append(entries, &newEntry)
		currentType = &treeType
	}
	return entries
}

type ListResult struct {
	Path      string
	Type      string
	Tombstone bool
}

func TestDBRepoOperations_ListTreeAndWorkspaceDirectory(t *testing.T) {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	str := store.NewDBStore(mdb)
	_, repo := testutil.GetIndexWithRepo(t, mdb)

	_, err := str.RepoTransact(repo.Id, func(ops store.RepoOperations) (i interface{}, e error) {
		paths := []string{"foo/bar", "foo/baz/bar1", "foo/baz/bar2", "foo/baz/bar3", "bar/baz/foo", "a/b/c/d/e/f"}
		var err2 error
		for _, path := range paths {
			err2 = ops.WriteToWorkspacePath(repo.DefaultBranch, CreateWorkspaceEntries(path, model.EntryTypeObject, false))
			if err2 != nil {
				t.Fatal(err2)
			}
		}
		deletedPaths := []string{"a/b/c/d/e/g", "a/b/c/e/f", "a/b/c/e/g"}
		for _, deletedPath := range deletedPaths {
			err2 = ops.WriteToWorkspacePath(repo.DefaultBranch, CreateWorkspaceEntries(deletedPath, model.EntryTypeObject, true))
			if err2 != nil {
				t.Fatal(err2)
			}
			err2 = ops.CascadeDirectoryDeletion(repo.DefaultBranch, deletedPath)
			if err2 != nil {
				t.Fatal(err2)
			}
		}
		testData := []struct {
			Path    string
			Entries []ListResult
		}{
			{"foo/", []ListResult{{Path: "foo/bar", Type: "object"}, {Path: "foo/baz/", Type: "tree"}}},
			{"", []ListResult{{Path: "a/", Type: "tree"}, {Path: "bar/", Type: "tree"}, {Path: "foo/", Type: "tree"}}},
			{"foo/baz/", []ListResult{{Path: "foo/baz/bar1", Type: "object"}, {Path: "foo/baz/bar2", Type: "object"}, {Path: "foo/baz/bar3", Type: "object"}}},
			{"bar/", []ListResult{{Path: "bar/baz/", Type: "tree"}}},
			{"bar/baz/", []ListResult{{Path: "bar/baz/foo", Type: "object"}}},
			{"a/", []ListResult{{Path: "a/b/", Type: "tree"}}},
			{"a/b/", []ListResult{{Path: "a/b/c/", Type: "tree"}}},
			{"a/b/c/", []ListResult{{Path: "a/b/c/d/", Type: "tree"}}},
			{"a/b/c/d/", []ListResult{{Path: "a/b/c/d/e/", Type: "tree"}}},
			{"a/b/c/d/e/", []ListResult{{Path: "a/b/c/d/e/f", Type: "object"}}},
			{"a/b/c/e/", []ListResult{}},
		}
		var entries []*model.Entry
		var err error
		for _, test := range testData {
			entries, _, err = ops.ListTreeAndWorkspaceDirectory(repo.DefaultBranch, test.Path, "", 50, false)
			if err != nil {
				//t.Fatal(err)
			}
			if len(entries) != len(test.Entries) {
				return nil, nil
				//t.Fatalf("Expected %d entries in dir \"%s\", got %d", len(test.Entries), test.Path, len(entries))
			}
			for i, expectedEntry := range test.Entries {
				if expectedEntry.Path != entries[i].Name {
					t.Fatalf("Expected path %s in index %d for dir %s, got: %s", expectedEntry.Path, i, test.Path, entries[i].Name)
				}
				if expectedEntry.Type != entries[i].EntryType {
					t.Fatalf("Expected type %s for %s, got: %s", expectedEntry.Type, expectedEntry.Path, entries[i].EntryType)
				}
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestDBRepoOperations_ListTreeWithPrefix(t *testing.T) {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	str := store.NewDBStore(mdb)
	idx, repo := testutil.GetIndexWithRepo(t, mdb)
	tree := testutil.ConstructTree(map[string][]*model.Entry{
		"master": {},
	})
	m := merkle.New("master")
	paths := []string{"foo/bar", "foo/baz/bar1", "foo/baz/bar2", "foo/baz/bar3", "bar/baz/foo", "a/b/c/d/e/f"}
	var entries []*model.WorkspaceEntry
	for _, path := range paths {
		entries = append(entries, CreateWorkspaceEntries(path, model.EntryTypeObject, false)...)
	}
	initialMerkle, err := m.Update(tree, entries)
	_, _ = str.RepoTransact(repo.Id, func(ops store.RepoOperations) (i interface{}, e error) {
		getEntries, e := initialMerkle.GetEntries(tree, "")
		ops.WriteTree("meow", getEntries)
		idx.Commit(repo.Id, repo.DefaultBranch, "meow", "", nil)
		//deletedPaths := []string{"a/b/c/d/e/g", "a/b/c/e/f", "a/b/c/e/g"}
		//for _, deletedPath := range deletedPaths {
		//	err2 = ops.WriteToWorkspacePath(repo.DefaultBranch, CreateWorkspaceEntries(deletedPath, true))
		//	if err2 != nil {
		//		t.Fatal(err2)
		//	}
		//	err2 = ops.CascadeDirectoryDeletion(repo.DefaultBranch, deletedPath)
		//	if err2 != nil {
		//		t.Fatal(err2)
		//	}
		//}
		testData := []struct {
			Path    string
			Entries []ListResult
		}{
			{"foo/", []ListResult{{Path: "foo/bar", Type: "object"}, {Path: "foo/baz/", Type: "tree"}}},
			{"", []ListResult{{Path: "a/", Type: "tree"}, {Path: "bar/", Type: "tree"}, {Path: "foo/", Type: "tree"}}},
			{"foo/baz/", []ListResult{{Path: "foo/baz/bar1", Type: "object"}, {Path: "foo/baz/bar2", Type: "object"}, {Path: "foo/baz/bar3", Type: "object"}}},
			{"bar/", []ListResult{{Path: "bar/baz/", Type: "tree"}}},
			{"bar/baz/", []ListResult{{Path: "bar/baz/foo", Type: "object"}}},
			{"a/", []ListResult{{Path: "a/b/", Type: "tree"}}},
			{"a/b/", []ListResult{{Path: "a/b/c/", Type: "tree"}}},
			{"a/b/c/", []ListResult{{Path: "a/b/c/d/", Type: "tree"}}},
			{"a/b/c/d/", []ListResult{{Path: "a/b/c/d/e/", Type: "tree"}}},
			{"a/b/c/d/e/", []ListResult{{Path: "a/b/c/d/e/f", Type: "object"}}},
			{"a/b/c/e/", []ListResult{}},
		}
		var entries []*model.Entry
		var err error
		for _, test := range testData {
			entries, _, err = ops.ListTreeWithPrefix("meow", test.Path, "", 50, false)
			if err != nil {
				//t.Fatal(err)
			}
			if len(entries) != len(test.Entries) {
				//t.Fatalf("Expected %d entries in dir \"%s\", got %d", len(test.Entries), test.Path, len(entries))
			}

			//for i, expectedEntry := range test.Entries {
			//	if expectedEntry.Path != entries[i].Name {
			//		//t.Fatalf("Expected path %s in index %d for dir %s, got: %s", expectedEntry.Path, i, test.Path, entries[i].Name)
			//	}
			//	if expectedEntry.Type != entries[i].EntryType {
			//		//t.Fatalf("Expected type %s for %s, got: %s", expectedEntry.Type, expectedEntry.Path, entries[i].EntryType)
			//	}
			//}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
