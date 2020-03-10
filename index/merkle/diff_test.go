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
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
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
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					{Name: "dir1", Address: "dir1/1", Type: model.Entry_TREE},
				},
				"right": {},
				"common": {
					{Name: "dir1", Address: "dir1", Type: model.Entry_TREE},
				},
			}),
			ExpectedErr: db.ErrNotFound,
		},

		{
			Name: "common_db_error",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					{Name: "dir1", Address: "dir1/1", Type: model.Entry_TREE},
				},
				"right": {
					{Name: "dir1", Address: "dir1/2", Type: model.Entry_TREE},
				},
				"dir1/1": {
					{Name: "file2", Address: "file2", Type: model.Entry_OBJECT},
				},
				"common": {
					{Name: "dir1", Address: "dir1", Type: model.Entry_TREE},
				},
			}),
			ExpectedErr: db.ErrNotFound,
		},

		{
			Name: "common_unknown_error",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					{Name: "dir1", Address: "dir1/1", Type: model.Entry_TREE},
				},
				"right": {
					{Name: "dir1", Address: "dir1/2", Type: model.Entry_TREE},
				},
				"dir1/1": {
					{Name: "file2", Address: "file2", Type: model.Entry_OBJECT},
				},
				"common": {
					{Name: "dir1", Address: "dir1", Type: model.Entry_TREE},
				},
			}).WithReadError(ErrUnexpected),
			ExpectedErr: ErrUnexpected,
		},

		{
			Name: "remove_file",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {},
				"right": {
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
				},
				"common": {
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
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
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					{Name: "dir1", Address: "dir1", Type: model.Entry_TREE},
				},
				"right": {
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					{Name: "dir1", Address: "dir1", Type: model.Entry_TREE},
				},
				"common": {
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					{Name: "dir1", Address: "dir1", Type: model.Entry_TREE},
				},
				"dir1": {
					{Name: "file2", Address: "file2", Type: model.Entry_OBJECT},
				},
			}),
			Expected: []string{},
		},

		{
			Name: "no_diff_on_right",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "file0", Address: "file0", Type: model.Entry_OBJECT},
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					{Name: "dir1", Address: "dir1", Type: model.Entry_TREE},
				},
				"right": {
					{Name: "file0", Address: "file0_changed", Type: model.Entry_OBJECT},
					{Name: "file3", Address: "file3", Type: model.Entry_OBJECT},
					{Name: "dir1", Address: "dir1", Type: model.Entry_TREE},
					{Name: "dir2", Address: "dir2", Type: model.Entry_TREE},
				},
				"common": {
					{Name: "file0", Address: "file0", Type: model.Entry_OBJECT},
					{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					{Name: "dir1", Address: "dir1", Type: model.Entry_TREE},
				},
				"dir1": {
					{Name: "file2", Address: "file2", Type: model.Entry_OBJECT},
				},
				"dir2": {
					{Name: "file3", Address: "file3", Type: model.Entry_OBJECT},
				},
			}),
			Expected: []string{},
		},

		{
			Name: "add_directory",
			Reader: testutil.ConstructTree(map[string][]*model.Entry{
				"left": {
					{Name: "events/", Address: "events/1", Type: model.Entry_TREE},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", Type: model.Entry_OBJECT},
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
					{Name: "events/", Address: "events/2", Type: model.Entry_TREE},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", Type: model.Entry_OBJECT},
				},
				"events/2": {
					{Name: "file_one", Address: "file_one_2", Type: model.Entry_OBJECT},
				},
				"right": {},
				"common": {
					{Name: "events/", Address: "events/1", Type: model.Entry_TREE},
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
					{Name: "events/", Address: "events/2", Type: model.Entry_TREE},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", Type: model.Entry_OBJECT},
				},
				"events/2": {
					{Name: "file_one", Address: "file_one_2", Type: model.Entry_OBJECT},
				},
				"left": {},
				"common": {
					{Name: "events/", Address: "events/1", Type: model.Entry_TREE},
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
					{Name: "file_one", Address: "file_one", Type: model.Entry_OBJECT},
				},
				"right": {
					{Name: "events/", Address: "events/1", Type: model.Entry_TREE},
				},
				"common": {
					{Name: "events/", Address: "events/1", Type: model.Entry_TREE},
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
					{Name: "events/", Address: "events/1", Type: model.Entry_TREE},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", Type: model.Entry_OBJECT},
				},
				"right": {
					{Name: "events/", Address: "events/2", Type: model.Entry_TREE},
				},
				"events/2": {
					{Name: "file_two", Address: "file_two", Type: model.Entry_OBJECT},
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
					{Name: "events/", Address: "events/1", Type: model.Entry_TREE},
					{Name: "file_three", Address: "file_three", Type: model.Entry_OBJECT},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", Type: model.Entry_OBJECT},
				},
				"right": {
					{Name: "events/", Address: "events/2", Type: model.Entry_TREE},
				},
				"events/2": {
					{Name: "file_two", Address: "file_two", Type: model.Entry_OBJECT},
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
					{Name: "events/", Address: "events/1", Type: model.Entry_TREE},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", Type: model.Entry_OBJECT},
				},
				"right": {
					{Name: "events/", Address: "events/2", Type: model.Entry_TREE},
					{Name: "file_three", Address: "file_three", Type: model.Entry_OBJECT},
				},
				"events/2": {
					{Name: "file_two", Address: "file_two", Type: model.Entry_OBJECT},
				},
				"common": {
					{Name: "file_three", Address: "file_three", Type: model.Entry_OBJECT},
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
					{Name: "events/", Address: "events/1", Type: model.Entry_TREE},
					{Name: "file_three", Address: "file_three_other", Type: model.Entry_OBJECT},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", Type: model.Entry_OBJECT},
				},
				"right": {
					{Name: "events/", Address: "events/2", Type: model.Entry_TREE},
					{Name: "file_three", Address: "file_three", Type: model.Entry_OBJECT},
				},
				"events/2": {
					{Name: "file_two", Address: "file_two", Type: model.Entry_OBJECT},
				},
				"common": {
					{Name: "file_three", Address: "file_three", Type: model.Entry_OBJECT},
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
					{Name: "events/", Address: "events/1", Type: model.Entry_TREE},
					{Name: "file_three", Address: "file_three_conflict", Type: model.Entry_OBJECT},
				},
				"events/1": {
					{Name: "file_one", Address: "file_one", Type: model.Entry_OBJECT},
				},
				"right": {
					{Name: "events/", Address: "events/2", Type: model.Entry_TREE},
					{Name: "file_three", Address: "file_three_other", Type: model.Entry_OBJECT},
				},
				"events/2": {
					{Name: "file_two", Address: "file_two", Type: model.Entry_OBJECT},
				},
				"common": {
					{Name: "file_three", Address: "file_three", Type: model.Entry_OBJECT},
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
