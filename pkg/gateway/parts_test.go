package gateway_test

import (
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/pkg/gateway"
)

func TestSplitParts(t *testing.T) {
	bareDomain := "lakefs.example.com"
	cases := []struct {
		Name           string
		URLPath        string
		Host           string
		ResultSuccess  bool
		ExpectedResult []string
	}{
		{
			Name:           "repo_only_virtual_style",
			URLPath:        "/",
			Host:           "foo.lakefs.example.com",
			ExpectedResult: []string{"foo", "", ""},
		},
		{
			Name:           "repo_only_virtual_style_1",
			URLPath:        "",
			Host:           "foo.lakefs.example.com",
			ExpectedResult: []string{"foo", "", ""},
		},
		{
			Name:           "repo_only_path_style",
			URLPath:        "/foo",
			Host:           "lakefs.example.com",
			ExpectedResult: []string{"foo", "", ""},
		},
		{
			Name:           "repo_only_path_style_1",
			URLPath:        "/foo/",
			Host:           "lakefs.example.com",
			ExpectedResult: []string{"foo", "", ""},
		},
		{
			Name:           "repo_only_path_style_2",
			URLPath:        "foo/",
			Host:           "lakefs.example.com",
			ExpectedResult: []string{"foo", "", ""},
		},
		{
			Name:           "repo_branch_virtual_style",
			URLPath:        "/bar",
			Host:           "foo.lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", ""},
		},
		{
			Name:           "repo_branch_virtual_style_1",
			URLPath:        "/bar/",
			Host:           "foo.lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", ""},
		},
		{
			Name:           "repo_branch_virtual_style_2",
			URLPath:        "bar/",
			Host:           "foo.lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", ""},
		},
		{
			Name:           "repo_branch_virtual_style_3",
			URLPath:        "bar",
			Host:           "foo.lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", ""},
		},
		{
			Name:           "repo_branch_path_virtual_style",
			URLPath:        "bar/a/b/c",
			Host:           "foo.lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", "a/b/c"},
		},
		{
			Name:           "repo_branch_path_virtual_style_1",
			URLPath:        "/bar/a/b/c",
			Host:           "foo.lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", "a/b/c"},
		},
		{
			Name:           "repo_branch_path_virtual_style_2",
			URLPath:        "bar/a/b/c/",
			Host:           "foo.lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", "a/b/c/"},
		}, {
			Name:           "repo_branch_path_virtual_style_3",
			URLPath:        "/bar/a/b/c/",
			Host:           "foo.lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", "a/b/c/"},
		},
		{
			Name:           "repo_branch_path_path_style",
			URLPath:        "foo/bar/a/b/c",
			Host:           "lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", "a/b/c"},
		},
		{
			Name:           "repo_branch_path_path_style_1",
			URLPath:        "/foo/bar/a/b/c",
			Host:           "lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", "a/b/c"},
		},
		{
			Name:           "repo_branch_path_path_style_2",
			URLPath:        "foo/bar/a/b/c/",
			Host:           "lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", "a/b/c/"},
		}, {
			Name:           "repo_branch_path_path_style_3",
			URLPath:        "/foo/bar/a/b/c/",
			Host:           "lakefs.example.com",
			ExpectedResult: []string{"foo", "bar", "a/b/c/"},
		},
		{
			Name:           "all_empty",
			URLPath:        "",
			Host:           "lakefs.example.com",
			ExpectedResult: []string{"", "", ""},
		},
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			gotRepo, gotRef, gotPath := gateway.Parts(cas.Host, cas.URLPath, bareDomain)
			actualResult := []string{gotRepo, gotRef, gotPath}
			if !reflect.DeepEqual(cas.ExpectedResult, actualResult) {
				t.Fatalf("expected parts = %+v for split '%s', got %+v", cas.ExpectedResult, cas.URLPath, actualResult)
			}
		})
	}
}
