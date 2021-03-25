package block_test

import (
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/treeverse/lakefs/pkg/block"
)

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestPath_SplitParts_Objects(t *testing.T) {
	testData := []struct {
		Path  string
		Parts []string
	}{
		{"/foo/bar", []string{"foo/", "bar"}},
		{"foo/bar/", []string{"foo/", "bar/", ""}},
		{"/foo///bar", []string{"foo/", "/", "/", "bar"}},
		{"/foo///bar/", []string{"foo/", "/", "/", "bar/", ""}},
		{"/foo///bar////", []string{"foo/", "/", "/", "bar/", "/", "/", "/", ""}},
		{"////foo", []string{"/", "/", "/", "foo"}},
		{"//", []string{"/", ""}},
		{"/", []string{""}},
		{"", []string{""}},
		{"/hello/world/another/level", []string{"hello/", "world/", "another/", "level"}},
		{"/hello/world/another/level/", []string{"hello/", "world/", "another/", "level/", ""}},
	}
	for i, test := range testData {
		p := block.NewPath(test.Path, block.EntryTypeObject)
		if !equalStrings(p.Split(), test.Parts) {
			t.Fatalf("expected (%d): %s, got %s for path: %s", i, spew.Sdump(test.Parts), spew.Sdump(p.Split()), test.Path)
		}
	}
}

func TestPath_SplitParts_Trees(t *testing.T) {
	testData := []struct {
		Path  string
		Parts []string
	}{
		{"//", []string{"/"}},
		{"/", []string{""}},
		{"", []string{""}},
		{"/foo/bar", []string{"foo/", "bar"}},
		{"foo/bar/", []string{"foo/", "bar/"}},
		{"/hello/world/another/level", []string{"hello/", "world/", "another/", "level"}},
		{"/hello/world/another/level/", []string{"hello/", "world/", "another/", "level/"}},
	}
	for i, test := range testData {
		p := block.NewPath(test.Path, block.EntryTypeTree)
		if !equalStrings(p.Split(), test.Parts) {
			t.Fatalf("expected (%d): %s, got %s for path: %s", i, spew.Sdump(test.Parts), spew.Sdump(p.Split()), test.Path)
		}
	}
}

func TestPath_String(t *testing.T) {
	var nilPath *block.Path
	testData := []struct {
		Path   *block.Path
		String string
	}{
		{block.NewPath("hello/world/another/level", block.EntryTypeObject), "hello/world/another/level"},
		{block.NewPath("/hello/world/another/level", block.EntryTypeObject), "hello/world/another/level"},
		{block.NewPath("/hello/world/another/level/", block.EntryTypeTree), "hello/world/another/level/"},
		{nilPath, ""},
	}
	for i, test := range testData {
		if !strings.EqualFold(test.Path.String(), test.String) {
			t.Fatalf("expected (%d): \"%s\", got \"%s\" for path: \"%s\"", i, test.String, test.Path.String(), test.Path)
		}
	}
}

func TestJoin(t *testing.T) {
	testData := []struct {
		parts    []string
		expected string
	}{
		{[]string{"foo/bar", "baz"}, "foo/bar/baz"},
		{[]string{"foo/bar/", "baz"}, "foo/bar/baz"},
		{[]string{"foo/bar", "", "baz"}, "foo/bar//baz"},
		{[]string{"foo//bar", "baz"}, "foo//bar/baz"},
		{[]string{"foo/bar", ""}, "foo/bar/"},
		{[]string{"foo/bar/", ""}, "foo/bar/"},
	}
	for i, test := range testData {
		got := block.JoinPathParts(test.parts)
		if !strings.EqualFold(got, test.expected) {
			t.Fatalf("expected (%d): '%s', got '%s' for %v", i, test.expected, got, test.parts)
		}
	}
}

func TestPath_BaseName(t *testing.T) {
	testData := []struct {
		Path      string
		BaseName  string
		EntryType string
	}{
		{"/foo", "foo", block.EntryTypeObject},
		{"/foo/bar", "bar", block.EntryTypeObject},
		{"", "", block.EntryTypeTree},
		{"/", "", block.EntryTypeTree},
		{"foo/bar", "bar", block.EntryTypeObject},
		{"foo/bar/", "", block.EntryTypeObject},
		{"foo/bar", "bar", block.EntryTypeTree},
		{"foo/bar/", "bar/", block.EntryTypeTree},
	}
	for _, test := range testData {
		p := block.NewPath(test.Path, test.EntryType)
		if p.BaseName() != test.BaseName {
			t.Fatalf("expected BaseName to return %s, got %s for input: %s", test.BaseName, p.BaseName(), test.Path)
		}
	}
}

func TestPath_ParentPath(t *testing.T) {
	testData := []struct {
		Path       string
		ParentPath string
		EntryType  string
	}{
		{"/", "", block.EntryTypeTree},
		{"foo", "", block.EntryTypeObject},
		{"/foo", "", block.EntryTypeObject},
		{"foo/", "", block.EntryTypeTree},
		{"foo/", "foo/", block.EntryTypeObject},
		{"/foo/bar", "foo/", block.EntryTypeObject},
		{"foo/bar", "foo/", block.EntryTypeObject},
		{"/foo/bar/", "foo/", block.EntryTypeTree},
		{"foo/bar/", "foo/bar/", block.EntryTypeObject},
		{"/foo/bar/baz", "foo/bar/", block.EntryTypeObject},
		{"foo/bar/baz", "foo/bar/", block.EntryTypeObject},
		{"/foo/bar/baz/", "foo/bar/", block.EntryTypeTree},
		{"/foo/bar/baz", "foo/bar/", block.EntryTypeTree},
	}
	for _, test := range testData {
		p := block.NewPath(test.Path, test.EntryType)
		if p.ParentPath() != test.ParentPath {
			t.Fatalf("expected ParentPath to return %s, got %s for input: %s", test.ParentPath, p.ParentPath(), test.Path)
		}
	}
}
