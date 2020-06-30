package index_test

import (
	"testing"

	"github.com/treeverse/lakefs/index/merkle"

	"github.com/treeverse/lakefs/testutil"
)

func TestKVIndex_Diff(t *testing.T) {

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
		Expected   []merkle.Difference
	}{
		{
			Name: "add one file ",
			Actions: []Action{{
				command: write,
				path:    "a/foo",
			}},
			ListPath: "a/",
			Expected: []merkle.Difference{{Direction: merkle.DifferenceDirectionLeft, Type: merkle.DifferenceTypeAdded, Path: "a/", PathType: "tree"}},
		},
		{
			Name: "no changes",
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
			Expected: []merkle.Difference{},
		},
		{
			Name: "erase file",
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
					command: deleteEntry,
					path:    "a/foo",
				}},
			ListPath: "a/",
			Expected: []merkle.Difference{{Direction: merkle.DifferenceDirectionLeft, Type: merkle.DifferenceTypeRemoved, Path: "a/", PathType: "tree"}},
		},
		{
			Name: "move file to different folder",
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
					deleteEntry,
					"a/b/foo",
				},
				{
					write,
					"a/c/foo",
				},
			},
			ListPath: "a/",
			Expected: []merkle.Difference{
				{Direction: merkle.DifferenceDirectionLeft, Type: merkle.DifferenceTypeRemoved, Path: "a/b/", PathType: "tree"},
				{Direction: merkle.DifferenceDirectionLeft, Type: merkle.DifferenceTypeAdded, Path: "a/c/", PathType: "tree"},
			},
		},
		{
			Name: "remove folder with folders and files",
			Actions: []Action{
				{
					command: write,
					path:    "a/b/foo",
				}, {
					command: write,
					path:    "a/b/bar",
				}, {
					command: write,
					path:    "a/c/foo",
				},
				{
					command: write,
					path:    "a/c/bar",
				},
				{
					command: write,
					path:    "a/foo",
				},
				{
					commit,
					"",
				},
				{
					command: deleteEntry,
					path:    "a/b/foo",
				}, {
					command: deleteEntry,
					path:    "a/b/bar",
				}, {
					command: deleteEntry,
					path:    "a/c/foo",
				},
				{
					command: deleteEntry,
					path:    "a/c/bar",
				},
				{
					command: deleteEntry,
					path:    "a/foo",
				},
			},
			ListPath: "a/",
			Expected: []merkle.Difference{
				{Direction: merkle.DifferenceDirectionLeft, Type: merkle.DifferenceTypeRemoved, Path: "a/", PathType: "tree"},
			},
		},
		{
			Name: "remove folder with folders and files",
			Actions: []Action{
				{
					command: write,
					path:    "a/b/foo",
				}, {
					command: write,
					path:    "a/b/bar",
				}, {
					command: write,
					path:    "a/c/foo",
				},
				{
					command: write,
					path:    "a/c/bar",
				},
				{
					command: write,
					path:    "a/foo",
				},
				{
					command: write,
					path:    "a/b/c/foo",
				},
				{
					commit,
					"",
				},
				{
					command: write,
					path:    "a/b/c/foo",
				},
			},
			ListPath: "a/",
			Expected: []merkle.Difference{},
		},
	}

	for _, tc := range testTable {

		t.Run(tc.Name, func(t *testing.T) {
			mdb, _ := testutil.GetDB(t, databaseUri)
			kvIndex, repo := testutil.GetIndexWithRepo(t, mdb)
			var err error
			for _, action := range tc.Actions {
				_, err = runCommand(kvIndex, repo, action.command, action.path)
				if err != nil {
					t.Fatal(err)
				}
			}

			diffs, err := kvIndex.DiffWorkspace(repo.Id, repo.DefaultBranch)
			if err != nil {
				t.Fatal(err)
			}
			if len(diffs) != len(tc.Expected) {
				t.Errorf("expected diff size to be %d, got %d", len(tc.Expected), len(diffs))
			}

			for n, diff := range diffs {
				expectedDiff := tc.Expected[n]
				if diff.Path != expectedDiff.Path {
					t.Fatalf("got wrong path: expected:%s, got:%s", expectedDiff.Path, diff.Path)
				}
				if diff.Direction != expectedDiff.Direction {
					t.Fatalf("got wrong direction: expected:%v, got:%v for path:%s", expectedDiff.Direction, diff.Direction, diff.Path)
				}
				if diff.Type != expectedDiff.Type {
					t.Fatalf("got wrong type: expected:%v, got:%v for path:%s", expectedDiff.Type, diff.Type, diff.Path)
				}

			}

		})
	}
}
