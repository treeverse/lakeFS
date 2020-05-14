package index_test

import (
	"testing"

	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/testutil"
)

func TestKVIndex_ListObjectsByPrefix(t *testing.T) {

	type Action struct {
		command Command
		path    string
	}
	testTable := []struct {
		Name       string
		Actions    []Action
		ListPath   string
		ListAfter  string
		ListAmount int
		Expected   []model.Entry
	}{
		{
			Name: "one file from workspace",
			Actions: []Action{{
				command: write,
				path:    "a/foo",
			}},
			ListPath: "a/",
			Expected: []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
		},
		{
			Name: "one file from merkle",
			Actions: []Action{
				{
					write,
					"a/foo",
				},
				{
					commit,
					"",
				},
			},
			ListPath: "a/",
			Expected: []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
		},
		{
			Name: "file from merkle folder from workspace",
			Actions: []Action{
				{
					write,
					"a/foo",
				},
				{
					commit,
					"",
				},
				{
					write,
					"a/b/bar",
				},
			},
			ListPath: "a/",
			Expected: []model.Entry{{Name: "a/b/", EntryType: model.EntryTypeTree}, {Name: "a/foo", EntryType: model.EntryTypeObject}},
		},
		{
			Name: "deep delete",
			Actions: []Action{
				{
					write,
					"a/foo",
				},
				{
					write,
					"a/b/c/bar",
				},
				{
					commit,
					"",
				},
				{
					deleteEntry,
					"a/b/c/bar",
				},
			},
			ListPath: "a/",
			Expected: []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
		},
		{
			Name: "override and delete",
			Actions: []Action{
				{
					write,
					"a/foo",
				},
				{
					write,
					"a/b/c/bar",
				},
				{
					commit,
					"",
				},
				{
					write,
					"a/b/c/bar",
				},
				{
					deleteEntry,
					"a/b/c/bar",
				},
			},
			ListPath: "a/",
			Expected: []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
		},
		{
			Name: "two layer delete",
			Actions: []Action{
				{
					write,
					"a/foo",
				},
				{
					write,
					"a/b/c/bar",
				},
				{
					write,
					"a/b/bar",
				},
				{
					commit,
					"",
				},
				{
					deleteEntry,
					"a/b/c/bar",
				},
				{
					deleteEntry,
					"a/b/bar",
				},
			},
			ListPath: "a/",
			Expected: []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
		},
		{
			Name: "deep delete and add",
			Actions: []Action{
				{
					write,
					"a/foo",
				},
				{
					write,
					"a/b/c/bar",
				},
				{
					commit,
					"",
				},
				{
					deleteEntry,
					"a/b/c/bar",
				},
				{
					write,
					"a/b/c/foo",
				},
			},
			ListPath: "a/",
			Expected: []model.Entry{{Name: "a/b/", EntryType: model.EntryTypeTree}, {Name: "a/foo", EntryType: model.EntryTypeObject}},
		},
		{
			Name: "file from merkle folder from workspace with after",
			Actions: []Action{
				{
					write,
					"a/foo",
				},
				{
					commit,
					"",
				},
				{
					write,
					"a/b/bar",
				},
			},
			ListPath:  "a/",
			ListAfter: "a/b/",
			Expected:  []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
		},
		{
			Name: "delete one get one - delete committed",
			Actions: []Action{
				{
					write,
					"a/bar",
				},
				{
					commit,
					"",
				},
				{
					deleteEntry,
					"a/bar",
				},
				{
					write,
					"a/foo",
				},
			},
			ListPath:   "a/",
			ListAfter:  "a/b/",
			ListAmount: 1,
			Expected:   []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
		},
		{
			Name: "delete one get one both committed",
			Actions: []Action{
				{
					write,
					"a/bar",
				},
				{
					write,
					"a/foo",
				},
				{
					commit,
					"",
				},
				{
					deleteEntry,
					"a/bar",
				},
			},
			ListPath:   "a/",
			ListAfter:  "a/b/",
			ListAmount: 1,
			Expected:   []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
		},
	}

	for _, tc := range testTable {

		t.Run(tc.Name, func(t *testing.T) {
			mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
			kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)
			var err error
			for _, action := range tc.Actions {
				err = runCommand(kvIndex, repo, action.command, action.path)
				if err != nil {
					t.Fatal(err)
				}
			}
			amount := tc.ListAmount
			if amount == 0 { //TODO: remove this workaround once 0 and -1 works
				amount = -1
			}
			entries, _, err := kvIndex.ListObjectsByPrefix(repo.Id, repo.DefaultBranch, tc.ListPath, tc.ListAfter, amount, false)

			//compare entries
			if len(entries) != len(tc.Expected) {
				t.Fatalf("expected: %d entries, got: %d", len(tc.Expected), len(entries))
			}

			for n, entry := range entries {
				expected := tc.Expected[n]
				if expected.Name != entry.Name || expected.EntryType != entry.EntryType {
					t.Errorf("file mismatch expected name:%s got:%s, expected type:%s got:%s", expected.Name, entry.Name, expected.EntryType, entry.EntryType)
				}
			}

		})
	}
}
