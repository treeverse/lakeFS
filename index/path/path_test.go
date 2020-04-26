package path_test

import (
	"fmt"
	"github.com/treeverse/lakefs/index/model"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/index/path"
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

func reprstrings(inp []string) string {
	parts := make([]string, len(inp))
	for i, part := range inp {
		parts[i] = fmt.Sprintf("\"%s\"", part)
	}
	return fmt.Sprintf("[%s]", strings.Join(parts, ", "))

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
		p := path.New(test.Path, model.EntryTypeObject)
		if !equalStrings(p.SplitParts(), test.Parts) {
			t.Fatalf("expected (%d): %s, got %s for path: %s", i, reprstrings(test.Parts), reprstrings(p.SplitParts()), test.Path)
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
		p := path.New(test.Path, model.EntryTypeTree)
		if !equalStrings(p.SplitParts(), test.Parts) {
			t.Fatalf("expected (%d): %s, got %s for path: %s", i, reprstrings(test.Parts), reprstrings(p.SplitParts()), test.Path)
		}
	}
}

func TestPath_String(t *testing.T) {
	var nilpath *path.Path
	testData := []struct {
		Path   *path.Path
		String string
	}{
		{path.New("hello/world/another/level", model.EntryTypeObject), "hello/world/another/level"},
		{path.New("/hello/world/another/level", model.EntryTypeObject), "hello/world/another/level"},
		{path.New("/hello/world/another/level/", model.EntryTypeTree), "hello/world/another/level/"},
		{nilpath, ""},
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
		got := path.Join(test.parts)
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
		{"/foo", "foo", model.EntryTypeObject},
		{"/foo/bar", "bar", model.EntryTypeObject},
		{"", "", model.EntryTypeTree},
		{"/", "", model.EntryTypeTree},
		{"foo/bar", "bar", model.EntryTypeObject},
		{"foo/bar/", "", model.EntryTypeObject},
		{"foo/bar", "bar", model.EntryTypeTree},
		{"foo/bar/", "bar/", model.EntryTypeTree},
	}
	for _, test := range testData {
		p := path.New(test.Path, test.EntryType)
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
		{"/", "", model.EntryTypeTree},
		{"foo", "", model.EntryTypeObject},
		{"/foo", "", model.EntryTypeObject},
		{"foo/", "", model.EntryTypeTree},
		{"foo/", "foo/", model.EntryTypeObject},
		{"/foo/bar", "foo/", model.EntryTypeObject},
		{"foo/bar", "foo/", model.EntryTypeObject},
		{"/foo/bar/", "foo/", model.EntryTypeTree},
		{"foo/bar/", "foo/bar/", model.EntryTypeObject},
		{"/foo/bar/baz", "foo/bar/", model.EntryTypeObject},
		{"foo/bar/baz", "foo/bar/", model.EntryTypeObject},
		{"/foo/bar/baz/", "foo/bar/", model.EntryTypeTree},
		{"/foo/bar/baz", "foo/bar/", model.EntryTypeTree},
	}
	for _, test := range testData {
		p := path.New(test.Path, test.EntryType)
		if p.ParentPath() != test.ParentPath {
			t.Fatalf("expected ParentPath to return %s, got %s for input: %s", test.ParentPath, p.ParentPath(), test.Path)
		}
	}
}
