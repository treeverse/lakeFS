package path_test

import (
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/pkg/actions/lua/path"
)

func TestIsHidden(t *testing.T) {
	tbl := []struct {
		Input    string
		Expected bool
	}{
		{
			Input:    "foo/bar/baz",
			Expected: false,
		},
		{
			Input:    "foo/bar/",
			Expected: false,
		},
		{
			Input:    "",
			Expected: false,
		},
		{
			Input:    "/",
			Expected: false,
		},
		{
			Input:    "a/_b/c",
			Expected: true,
		},
		{
			Input:    "/_b/c",
			Expected: true,
		},
		{
			Input:    "/b_/c",
			Expected: false,
		},
		{
			Input:    "b/_c",
			Expected: true,
		},
		{
			Input:    "b/_c/",
			Expected: true,
		},
		{
			Input:    "/b/_c/",
			Expected: true,
		},
		{
			Input:    "/b/_c/a",
			Expected: true,
		},
		{
			Input:    "/b/_c/_a",
			Expected: true,
		},
	}

	for _, cas := range tbl {
		t.Run(cas.Input, func(t *testing.T) {
			got := path.IsHidden(cas.Input, "/", "_")
			if got != cas.Expected {
				t.Errorf("%s: expected hidden = %v but got %v", cas.Input, cas.Expected, got)
			}
		})
	}
}

func TestJoin(t *testing.T) {
	tbl := []struct {
		Input    []string
		Expected string
	}{
		{
			Input:    []string{"foo/bar/baz"},
			Expected: "foo/bar/baz",
		},

		{
			Input:    []string{"", "bar", "baz"},
			Expected: "/bar/baz",
		},
		{
			Input:    []string{"foo/", "bar", "baz"},
			Expected: "foo/bar/baz",
		},
		{
			Input:    []string{"a", "b", "c"},
			Expected: "a/b/c",
		},
		{
			Input:    []string{"a", "b", "c/"},
			Expected: "a/b/c/",
		},
	}

	for i, cas := range tbl {
		t.Run(fmt.Sprintf("join_%d", i), func(t *testing.T) {
			got := path.Join(path.SEPARATOR, cas.Input...)
			if got != cas.Expected {
				t.Errorf("Expected %s got %s", cas.Expected, got)
			}
		})
	}
}

func TestParse(t *testing.T) {
	tbl := []struct {
		Input            string
		ExpectedBasename string
		ExpectedParent   string
	}{
		{
			Input:            "foo/bar/baz",
			ExpectedBasename: "baz",
			ExpectedParent:   "foo/bar/",
		},
		{
			Input:            "bar",
			ExpectedBasename: "bar",
			ExpectedParent:   "",
		},
		{
			Input:            "/bar",
			ExpectedBasename: "bar",
			ExpectedParent:   "/",
		},
		{
			Input:            "foo/bar/",
			ExpectedParent:   "foo/",
			ExpectedBasename: "bar",
		},
		{
			Input:            "",
			ExpectedBasename: "",
			ExpectedParent:   "",
		},
	}

	for _, cas := range tbl {
		t.Run(cas.Input, func(t *testing.T) {
			got := path.Parse(cas.Input, path.SEPARATOR)
			if got["base_name"] != cas.ExpectedBasename {
				t.Errorf("base_name: expected '%s' got '%s'", cas.ExpectedBasename, got["base_name"])
			}
			if got["parent"] != cas.ExpectedParent {
				t.Errorf("parent: expected '%s' got '%s'", cas.ExpectedParent, got["parent"])
			}
		})
	}
}
