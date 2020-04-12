package merkle_test

import (
	"strings"
	"testing"

	"github.com/treeverse/lakefs/testutil"

	"golang.org/x/xerrors"

	"github.com/treeverse/lakefs/index/merkle"

	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/index/model"
)

var (
	ErrUnexpected = xerrors.New("unexpected error")
)

func TestDiff(t *testing.T) {

	cases := []struct {
		Name        string
		Reader      *testutil.MockTree
		ExpectedErr error
		Expected    []string
	}{
		{
			Name: "add_file",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
				},
				"right":  {},
				"common": {},
			}),

			Expected: []string{
				"<+O file1",
			},
		},

		{
			Name: "add_file_db_error",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					{Name: "dir1", Address: "dir1/1", EntryType: model.EntryTypeTree},
				},
				"right": {},
				"common": {
					{Name: "dir1", Address: "dir1", EntryType: model.EntryTypeTree},
				},
			}),
			ExpectedErr: db.ErrNotFound,
		},

		{
			Name: "common_db_error",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					{Name: "dir1", Address: "dir1/1", EntryType: model.EntryTypeTree},
				},
				"right": {
					{Name: "dir1", Address: "dir1/2", EntryType: model.EntryTypeTree},
				},
				"dir1/1": {
					{Name: "file2", Address: "file2", EntryType: model.EntryTypeObject},
				},
				"common": {
					{Name: "dir1", Address: "dir1", EntryType: model.EntryTypeTree},
				},
			}),
			ExpectedErr: db.ErrNotFound,
		},

		{
			Name: "common_unknown_error",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					{Name: "dir1", Address: "dir1/1", EntryType: model.EntryTypeTree},
				},
				"right": {
					{Name: "dir1", Address: "dir1/2", EntryType: model.EntryTypeTree},
				},
				"dir1/1": {
					{Name: "file2", Address: "file2", EntryType: model.EntryTypeObject},
				},
				"common": {
					{Name: "dir1", Address: "dir1", EntryType: model.EntryTypeTree},
				},
			}).WithReadError(ErrUnexpected),
			ExpectedErr: ErrUnexpected,
		},

		{
			Name: "remove_file",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {},
				"right": {
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
				},
				"common": {
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
				},
			}),
			Expected: []string{
				"<-O file1",
			},
		},

		{
			Name: "no_diff",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					{Name: "dir1", Address: "dir1", EntryType: model.EntryTypeTree},
				},
				"right": {
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					{Name: "dir1", Address: "dir1", EntryType: model.EntryTypeTree},
				},
				"common": {
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					{Name: "dir1", Address: "dir1", EntryType: model.EntryTypeTree},
				},
				"dir1": {
					{Name: "file2", Address: "file2", EntryType: model.EntryTypeObject},
				},
			}),
			Expected: []string{},
		},

		{
			Name: "no_diff_on_right",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "file0", Address: "file0", EntryType: model.EntryTypeObject},
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					{Name: "dir1", Address: "dir1", EntryType: model.EntryTypeTree},
				},
				"right": {
					{Name: "file0", Address: "file0_changed", EntryType: model.EntryTypeObject},
					{Name: "file3", Address: "file3", EntryType: model.EntryTypeObject},
					{Name: "dir1", Address: "dir1", EntryType: model.EntryTypeTree},
					{Name: "dir2", Address: "dir2", EntryType: model.EntryTypeTree},
				},
				"common": {
					{Name: "file0", Address: "file0", EntryType: model.EntryTypeObject},
					{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					{Name: "dir1", Address: "dir1", EntryType: model.EntryTypeTree},
				},
				"dir1": {
					{Name: "file2", Address: "file2", EntryType: model.EntryTypeObject},
				},
				"dir2": {
					{Name: "file3", Address: "file3", EntryType: model.EntryTypeObject},
				},
			}),
			Expected: []string{},
		},

		{
			Name: "add_directory",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "events/", Address: "events/1", EntryType: model.EntryTypeTree},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", EntryType: model.EntryTypeObject},
				},
				"right":  {},
				"common": {},
			}),
			Expected: []string{
				"<+D events/",
			},
		},

		{
			Name: "left_modified_right_deleted",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "events/", Address: "events/2", EntryType: model.EntryTypeTree},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", EntryType: model.EntryTypeObject},
				},
				"events/2": {
					{Name: "file_one", Address: "file_one_2", EntryType: model.EntryTypeObject},
				},
				"right": {},
				"common": {
					{Name: "events/", Address: "events/1", EntryType: model.EntryTypeTree},
				},
			}),
			Expected: []string{
				"*~O events/file_one",
			},
		},

		{
			Name: "right_modified_left_deleted",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"right": {
					{Name: "events/", Address: "events/2", EntryType: model.EntryTypeTree},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", EntryType: model.EntryTypeObject},
				},
				"events/2": {
					{Name: "file_one", Address: "file_one_2", EntryType: model.EntryTypeObject},
				},
				"left": {},
				"common": {
					{Name: "events/", Address: "events/1", EntryType: model.EntryTypeTree},
				},
			}),
			Expected: []string{
				"*-O events/file_one",
			},
		},

		{
			Name: "remove_directory",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {},
				"events/1": {
					{Name: "file_one", Address: "file_one", EntryType: model.EntryTypeObject},
				},
				"right": {
					{Name: "events/", Address: "events/1", EntryType: model.EntryTypeTree},
				},
				"common": {
					{Name: "events/", Address: "events/1", EntryType: model.EntryTypeTree},
				},
			}),
			Expected: []string{
				"<-D events/",
			},
		},

		{
			Name: "changes_in_directory",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "events/", Address: "events/1", EntryType: model.EntryTypeTree},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", EntryType: model.EntryTypeObject},
				},
				"right": {
					{Name: "events/", Address: "events/2", EntryType: model.EntryTypeTree},
				},
				"events/2": {
					{Name: "file_two", Address: "file_two", EntryType: model.EntryTypeObject},
				},
				"common": {},
			}),
			Expected: []string{
				"<+O events/file_one",
			},
		},

		{
			Name: "changes_in_directory_and_add_file",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "events/", Address: "events/1", EntryType: model.EntryTypeTree},
					{Name: "file_three", Address: "file_three", EntryType: model.EntryTypeObject},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", EntryType: model.EntryTypeObject},
				},
				"right": {
					{Name: "events/", Address: "events/2", EntryType: model.EntryTypeTree},
				},
				"events/2": {
					{Name: "file_two", Address: "file_two", EntryType: model.EntryTypeObject},
				},
				"common": {},
			}),
			Expected: []string{
				"<+O events/file_one",
				"<+O file_three",
			},
		},

		{
			Name: "changes_in_directory_and_remove_file",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "events/", Address: "events/1", EntryType: model.EntryTypeTree},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", EntryType: model.EntryTypeObject},
				},
				"right": {
					{Name: "events/", Address: "events/2", EntryType: model.EntryTypeTree},
					{Name: "file_three", Address: "file_three", EntryType: model.EntryTypeObject},
				},
				"events/2": {
					{Name: "file_two", Address: "file_two", EntryType: model.EntryTypeObject},
				},
				"common": {
					{Name: "file_three", Address: "file_three", EntryType: model.EntryTypeObject},
				},
			}),
			Expected: []string{
				"<+O events/file_one",
				"<-O file_three",
			},
		},

		{
			Name: "changes_in_directory_and_overwrite_file",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "events/", Address: "events/1", EntryType: model.EntryTypeTree},
					{Name: "file_three", Address: "file_three_other", EntryType: model.EntryTypeObject},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", EntryType: model.EntryTypeObject},
				},
				"right": {
					{Name: "events/", Address: "events/2", EntryType: model.EntryTypeTree},
					{Name: "file_three", Address: "file_three", EntryType: model.EntryTypeObject},
				},
				"events/2": {
					{Name: "file_two", Address: "file_two", EntryType: model.EntryTypeObject},
				},
				"common": {
					{Name: "file_three", Address: "file_three", EntryType: model.EntryTypeObject},
				},
			}),
			Expected: []string{
				"<+O events/file_one",
				"<~O file_three",
			},
		},

		{
			Name: "changes_in_directory_and_conflict_file",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "events/", Address: "events/1", EntryType: model.EntryTypeTree},
					{Name: "file_three", Address: "file_three_conflict", EntryType: model.EntryTypeObject},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", EntryType: model.EntryTypeObject},
				},
				"right": {
					{Name: "events/", Address: "events/2", EntryType: model.EntryTypeTree},
					{Name: "file_three", Address: "file_three_other", EntryType: model.EntryTypeObject},
				},
				"events/2": {
					{Name: "file_two", Address: "file_two", EntryType: model.EntryTypeObject},
				},
				"common": {
					{Name: "file_three", Address: "file_three", EntryType: model.EntryTypeObject},
				},
			}),
			Expected: []string{
				"<+O events/file_one",
				"*~O file_three",
			},
		},
	}

	for _, testCase := range cases {

		t.Run(testCase.Name, func(t *testing.T) {
			results, err := merkle.Diff(testCase.Reader,
				merkle.New("left"), merkle.New("right"), merkle.New("common"))

			// validate errors
			if testCase.ExpectedErr == nil && err != nil {
				t.Fatalf("got unexpected error: %s", err)
			} else if testCase.ExpectedErr != nil && !xerrors.Is(err, testCase.ExpectedErr) {
				t.Fatalf("expected error of type: %s, got %s instead", testCase.ExpectedErr, err)
			} else if testCase.ExpectedErr != nil {
				return
			}

			// compare diffs
			if len(results) != len(testCase.Expected) {
				t.Fatalf("diff results: %d records, expected %d", len(results), len(testCase.Expected))
			}
			for di, difference := range results {
				if !strings.EqualFold(difference.String(), testCase.Expected[di]) {
					t.Fatalf("diff index %d - expected diff '%s', got '%s'", di, testCase.Expected[di], difference.String())
				}
			}
		})
	}
}
