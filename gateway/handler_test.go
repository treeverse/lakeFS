package gateway_test

import (
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/gateway"
)

func TestSplitFirst(t *testing.T) {
	cases := []struct {
		Name          string
		InputPath     string
		InputParts    int
		ResultSuccess bool
		ResultSlice   []string
	}{
		{
			Name:          "repo_only",
			InputPath:     "/foo",
			InputParts:    1,
			ResultSuccess: true,
			ResultSlice:   []string{"foo"},
		},
		{
			Name:          "repo_branch_1",
			InputPath:     "/foo/bar",
			InputParts:    1,
			ResultSuccess: true,
			ResultSlice:   []string{"foo/bar"},
		},
		{
			Name:          "repo_branch_split",
			InputPath:     "/foo/bar",
			InputParts:    2,
			ResultSuccess: true,
			ResultSlice:   []string{"foo", "bar"},
		},
		{
			Name:          "repo_branch_path",
			InputPath:     "/foo/bar/a/b/c",
			InputParts:    3,
			ResultSuccess: true,
			ResultSlice:   []string{"foo", "bar", "a/b/c"},
		},
		{
			Name:          "all_path",
			InputPath:     "/foo/bar/a/b/c",
			InputParts:    1,
			ResultSuccess: true,
			ResultSlice:   []string{"foo/bar/a/b/c"},
		},
		{
			Name:          "empty match",
			InputPath:     "/",
			InputParts:    1,
			ResultSuccess: false,
			ResultSlice:   []string{""},
		},
		{
			Name:          "not enough parts",
			InputPath:     "/foo/bar",
			InputParts:    3,
			ResultSuccess: false,
			ResultSlice:   []string{""},
		},
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			gotSlice, gotSuccess := gateway.SplitFirst(cas.InputPath, cas.InputParts)
			if gotSuccess != cas.ResultSuccess {
				t.Fatalf("expected success = %v for split '%s', got %v", cas.ResultSuccess, cas.InputPath, gotSuccess)
			}
			if cas.ResultSuccess && !reflect.DeepEqual(gotSlice, cas.ResultSlice) {
				t.Fatalf("expected parts = %+v for split '%s', got %+v", cas.ResultSlice, cas.InputPath, gotSlice)
			}
		})
	}
}
