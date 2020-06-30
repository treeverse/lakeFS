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
		Name                 string
		Actions              []Action
		ListPath             string
		ListAfter            string
		ListAmount           int
		Expected             []model.Entry
		ExpectedHasMore      bool
		Descend              bool
		UseLatestCommitAsRef bool
	}{
		{
			Name: "one file from workspace",
			Actions: []Action{{
				command: write,
				path:    "a/foo",
			}},
			ListPath:   "a/",
			Expected:   []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
			ListAmount: -1,
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
			ListPath:   "a/",
			Expected:   []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
			ListAmount: -1,
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
			ListPath:   "a/",
			Expected:   []model.Entry{{Name: "a/b/", EntryType: model.EntryTypeTree}, {Name: "a/foo", EntryType: model.EntryTypeObject}},
			ListAmount: -1,
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
			ListPath:   "a/",
			Expected:   []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
			ListAmount: -1,
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
			ListPath:   "a/",
			Expected:   []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
			ListAmount: -1,
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
			ListPath:   "a/",
			Expected:   []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
			ListAmount: -1,
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
			ListPath:   "a/",
			Expected:   []model.Entry{{Name: "a/b/", EntryType: model.EntryTypeTree}, {Name: "a/foo", EntryType: model.EntryTypeObject}},
			ListAmount: -1,
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
			ListPath:   "a/",
			ListAfter:  "a/b/",
			Expected:   []model.Entry{{Name: "a/foo", EntryType: model.EntryTypeObject}},
			ListAmount: -1,
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
		{
			Name: "zero amount from workspace",
			Actions: []Action{{
				command: write,
				path:    "a/foo",
			}},
			ListPath:        "a/",
			Expected:        []model.Entry{},
			ListAmount:      0,
			ExpectedHasMore: true,
		},
		{
			Name: "zero amount from merkle",
			Actions: []Action{
				{
					command: write,
					path:    "a/foo",
				},
				{
					commit,
					"",
				},
			},
			ListPath:        "a/",
			Expected:        []model.Entry{},
			ListAmount:      0,
			ExpectedHasMore: true,
		},
		{
			Name: "zero amount from both",
			Actions: []Action{
				{
					command: write,
					path:    "a/b/foo",
				},
				{
					commit,
					"",
				},
				{
					command: write,
					path:    "a/c/bar",
				},
			},
			ListPath:        "a/",
			Expected:        []model.Entry{},
			ListAmount:      0,
			ExpectedHasMore: true,
		},
		{
			Name: "zero amount when path is empty",
			Actions: []Action{
				{
					command: write,
					path:    "a/foo",
				},
				{
					commit,
					"",
				},
				{
					command: write,
					path:    "a/bar",
				},
			},
			ListPath:        "b/",
			Expected:        []model.Entry{},
			ListAmount:      0,
			ExpectedHasMore: false,
		},
		{
			Name: "delete and paging",
			Actions: []Action{
				{
					command: write,
					path:    "a/foo",
				},
				{
					command: write,
					path:    "a/bar1",
				},
				{
					command: write,
					path:    "a/bar3",
				},
				{
					command: deleteEntry,
					path:    "a/bar1",
				},
				{
					commit,
					"",
				},
				{
					command: write,
					path:    "a/b/bar2",
				},
				{
					command: deleteEntry,
					path:    "a/bar3",
				},
			},
			ListPath:        "a/",
			Expected:        []model.Entry{{Name: "a/b/", EntryType: model.EntryTypeTree}, {Name: "a/foo", EntryType: model.EntryTypeObject}},
			ListAmount:      2,
			ExpectedHasMore: false,
		},
		{
			Name:    "simple descend",
			Descend: true,
			Actions: []Action{
				{
					command: write,
					path:    "a/foo",
				},
				{
					command: write,
					path:    "c/loo",
				},
				{
					command: write,
					path:    "a/b/foo",
				},
				{
					commit,
					"",
				},
			},
			ListPath:        "a/",
			Expected:        []model.Entry{{Name: "a/b/foo"}, {Name: "a/foo"}},
			ListAmount:      3,
			ExpectedHasMore: false,
		},
		{
			Name:    "descend workspace",
			Descend: true,
			Actions: []Action{
				{
					command: write,
					path:    "a/foo",
				},
				{
					command: write,
					path:    "c/loo",
				},
				{
					command: write,
					path:    "a/b/foo",
				},
			},
			ListPath:        "a/",
			Expected:        []model.Entry{{Name: "a/b/foo"}, {Name: "a/foo"}},
			ListAmount:      3,
			ExpectedHasMore: false,
		},
		{
			Name:    "descend with paging",
			Descend: true,
			Actions: []Action{
				{
					command: write,
					path:    "a/foo",
				},
				{
					command: write,
					path:    "c/loo",
				},
				{
					command: write,
					path:    "a/b/foo",
				},
				{
					command: write,
					path:    "a/goo",
				},
			},
			ListPath:        "a/",
			Expected:        []model.Entry{{Name: "a/b/foo"}, {Name: "a/foo"}},
			ListAmount:      2,
			ExpectedHasMore: true,
		},
		{
			Name: "tombstone comes in later batch",
			Actions: []Action{
				{
					command: write,
					path:    "a/loo",
				},
				{
					command: write,
					path:    "b/shoo",
				},
				{
					command: commit,
				},
				{
					command: write,
					path:    "a/foo",
				},
				{
					command: deleteEntry,
					path:    "a/loo",
				},
				{
					command: deleteEntry,
					path:    "b/shoo",
				},
			},
			Descend:    true,
			ListPath:   "",
			Expected:   []model.Entry{{Name: "a/foo"}},
			ListAmount: 1,
		},
		{
			Name: "tombstone comes before file itself",
			Actions: []Action{
				{
					command: write,
					path:    "b/loo",
				},
				{
					command: write,
					path:    "a/moo",
				},
				{
					command: write,
					path:    "a/foo",
				},
				{
					command: write,
					path:    "a/zoo",
				},
				{
					command: commit,
				},
				{
					command: deleteEntry,
					path:    "b/loo",
				},
				{
					command: deleteEntry,
					path:    "a/zoo",
				},
			},
			Descend:    true,
			ListPath:   "",
			Expected:   []model.Entry{{Name: "a/foo"}, {Name: "a/moo"}},
			ListAmount: 2,
		},
		{
			Name: "tombstone comes before file itself with after",
			Actions: []Action{
				{
					command: write,
					path:    "a/1",
				},
				{
					command: write,
					path:    "a/2",
				},
				{
					command: write,
					path:    "a/3",
				},
				{
					command: commit,
				},
				{
					command: deleteEntry,
					path:    "a/3",
				},
			},
			Descend:    true,
			ListPath:   "",
			ListAfter:  "a/1",
			Expected:   []model.Entry{{Name: "a/2"}},
			ListAmount: 1,
		},
		{
			Name: "tombstone comes before file itself - amount bigger",
			Actions: []Action{
				{
					command: write,
					path:    "b/loo",
				},
				{
					command: write,
					path:    "a/moo",
				},
				{
					command: write,
					path:    "a/foo",
				},
				{
					command: write,
					path:    "a/zoo",
				},
				{
					command: commit,
				},
				{
					command: deleteEntry,
					path:    "b/loo",
				},
				{
					command: deleteEntry,
					path:    "a/zoo",
				},
			},
			Descend:    true,
			ListPath:   "",
			Expected:   []model.Entry{{Name: "a/foo"}, {Name: "a/moo"}},
			ListAmount: 3,
		},
		{
			Name: "use ref instead of branch",
			Actions: []Action{
				{
					command: write,
					path:    "a/moo",
				},
				{
					command: write,
					path:    "a/foo",
				},
				{
					command: write,
					path:    "a/zoo",
				},
				{
					command: commit,
				},
				{
					command: deleteEntry,
					path:    "a/moo",
				},
			},
			Descend:              true,
			ListPath:             "",
			Expected:             []model.Entry{{Name: "a/foo"}, {Name: "a/moo"}},
			ListAmount:           2,
			UseLatestCommitAsRef: true,
			ExpectedHasMore:      true,
		},
		{
			Name: "use ref instead of branch with after",
			Actions: []Action{
				{
					command: write,
					path:    "a/1",
				},
				{
					command: write,
					path:    "a/2",
				},
				{
					command: write,
					path:    "a/3",
				},
				{
					command: write,
					path:    "a/4",
				},
				{
					command: commit,
				},
				{
					command: deleteEntry,
					path:    "a/2",
				},
			},
			Descend:              true,
			ListPath:             "",
			ListAfter:            "a/3",
			Expected:             []model.Entry{{Name: "a/4"}},
			ListAmount:           2,
			UseLatestCommitAsRef: true,
		},
	}

	for _, tc := range testTable {

		t.Run(tc.Name, func(t *testing.T) {
			mdb, _ := testutil.GetDB(t, databaseUri)
			kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)
			var err error
			var retVal, commitId string
			for _, action := range tc.Actions {
				retVal, err = runCommand(kvIndex, repo, action.command, action.path)
				if action.command == commit {
					commitId = retVal
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			ref := repo.DefaultBranch
			if tc.UseLatestCommitAsRef {
				ref = commitId
			}
			entries, hasMore, err := kvIndex.ListObjectsByPrefix(repo.Id, ref, tc.ListPath, tc.ListAfter, tc.ListAmount, tc.Descend, true)

			//compare entries
			if len(entries) != len(tc.Expected) {
				t.Fatalf("expected: %d entries, got: %d", len(tc.Expected), len(entries))
			}

			for n, entry := range entries {
				expected := tc.Expected[n]
				if expected.EntryType == "" {
					expected.EntryType = model.EntryTypeObject
				}
				if expected.Name != entry.Name || expected.EntryType != entry.EntryType {
					t.Errorf("file mismatch expected name:%s got:%s, expected type:%s got:%s", expected.Name, entry.Name, expected.EntryType, entry.EntryType)
				}
			}
			if tc.ExpectedHasMore != hasMore {
				t.Fatalf("expected hasMore value: %v, got: %v", tc.ExpectedHasMore, hasMore)
			}
		})
	}
}
