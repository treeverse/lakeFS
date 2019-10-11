package path_test

import (
	"fmt"
	"strings"
	"testing"
	"versio-index/index/path"
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
		{"/foo/bar", []string{"foo", "bar"}},
		{"/foo///bar", []string{"foo", "bar"}},
		{"/foo///bar/", []string{"foo", "bar"}},
		{"/foo///bar////", []string{"foo", "bar"}},
		{"////foo", []string{"foo"}},
		{"//", []string{}},
		{"/", []string{}},
		{"", []string{}},
		{"/hello/world/another/level", []string{"hello", "world", "another", "level"}},
		{"/hello/world/another/level/", []string{"hello", "world", "another", "level"}},
	}
	for _, test := range testData {
		p := path.New(test.Path)
		if !equalStrings(p.SplitParts(), test.Parts) {
			t.Fatalf("expected: %s, got %s for path: %s", reprstrings(test.Parts), reprstrings(p.SplitParts()), test.Path)
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

func TestPath_Pop(t *testing.T) {
	testData := []struct {
		Path      string
		Remainder *path.Path
		Popped    string
	}{
		{Path: "/foo/bar", Remainder: path.New("/foo"), Popped: "bar"},
		{Path: "/foo/bar/baz", Remainder: path.New("/foo/bar"), Popped: "baz"},
		{Path: "/foo/bar/baz/", Remainder: path.New("/foo/bar"), Popped: "baz"},
		{Path: "/foo", Remainder: nil, Popped: "foo"},
	}

	for _, test := range testData {
		p := path.New(test.Path)
		p, popped := p.Pop()
		if !p.Equals(test.Remainder) {
			t.Fatalf("expected remainder: '%s', got '%s' (while popping '%s') for path '%s'", test.Remainder, p, popped, test.Path)
		}
		if !strings.EqualFold(test.Popped, popped) {
			t.Fatalf("expected to pop: '%s', got '%s' for path '%s'", test.Popped, popped, test.Path)
		}
	}
}
