package path_test

import (
	"fmt"
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

func TestPath_SplitParts(t *testing.T) {
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
		p := path.New(test.Path)
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
		{path.New("hello/world/another/level"), "hello/world/another/level"},
		{path.New("/hello/world/another/level"), "hello/world/another/level"},
		{path.New("/hello/world/another/level/"), "hello/world/another/level/"},
		{nilpath, ""},
	}
	for i, test := range testData {
		if !strings.EqualFold(test.Path.String(), test.String) {
			t.Fatalf("expected (%d): \"%s\", got \"%s\" for path: \"%s\"", i, test.String, test.Path.String(), test.Path)
		}
	}
}

func TestPath_HasParent(t *testing.T) {
	testData := []struct {
		Path      string
		HasParent bool
	}{
		{"/foo", false},
		{"/foo/bar", true},
		{"", false},
		{"/", false},
		{"foo/bar", true},
		{"foo/bar/", true},
	}
	for _, test := range testData {
		p := path.New(test.Path)
		if !p.HasParent() == test.HasParent {
			t.Fatalf("expected HasParent to return %v, got %v for input: %s", test.HasParent, p.HasParent(), test.Path)
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

func TestPath_Dir(t *testing.T) {
	testData := []struct {
		Path     string
		expected string
	}{
		{"foo/bar", "foo/"},
		{"foo/bar/", "foo/"},
		{"foo/", ""},
		{"foo", ""},
		{"", ""},
	}
	for i, test := range testData {
		pth := path.New(test.Path)
		got := pth.Dir().String()
		if !strings.EqualFold(got, test.expected) {
			t.Fatalf("expected (%d): '%s', got '%s' for %v", i, test.expected, got, test.Path)
		}
	}
}
