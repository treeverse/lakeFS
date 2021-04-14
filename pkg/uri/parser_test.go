package uri_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/pkg/uri"
)

func strp(v string) *string {
	return &v
}

func TestParse(t *testing.T) {
	cases := []struct {
		Input    string
		Err      error
		Expected *uri.URI
	}{
		{
			Input: "lakefs://foo/bar/baz",
			Expected: &uri.URI{
				Repository: "foo",
				Ref:        "bar",
				Path:       strp("baz"),
			},
		},
		{
			Input: "lakefs://foo",
			Expected: &uri.URI{
				Repository: "foo",
			},
		},
		{
			Input: "lakefs://foo/bar/baz/path",
			Expected: &uri.URI{
				Repository: "foo",
				Ref:        "bar",
				Path:       strp("baz/path"),
			},
		},
		{
			Input: "lakefs://foo/bar/baz/path@withappendix.foo",
			Expected: &uri.URI{
				Repository: "foo",
				Ref:        "bar",
				Path:       strp("baz/path@withappendix.foo"),
			},
		},
		{
			Input: "lakefs://fo-o/bar/baz/path@withappendix.foo",
			Expected: &uri.URI{
				Repository: "fo-o",
				Ref:        "bar",
				Path:       strp("baz/path@withappendix.foo"),
			},
		},
		{
			Input: "lakefs://foo",
			Expected: &uri.URI{
				Repository: "foo",
			},
		},
		{
			Input: "lakefs://foo/bar/",
			Expected: &uri.URI{
				Repository: "foo",
				Ref:        "bar",
				Path:       strp(""),
			},
		},
		{
			Input: "lakefs://foo/bar//",
			Expected: &uri.URI{
				Repository: "foo",
				Ref:        "bar",
				Path:       strp("/"),
			},
		},
		{
			Input: "lakefs://foo/bar",
			Expected: &uri.URI{
				Repository: "foo",
				Ref:        "bar",
			},
		},
		{
			Input: "lakefs://foo@bar",
			Err:   uri.ErrMalformedURI,
		},
		{
			Input: "lakefs://foo@bar/baz",
			Err:   uri.ErrMalformedURI,
		},
		{
			Input: "lakefssss://foo/bar/baz",
			Err:   uri.ErrMalformedURI,
		},
		{
			Input: "lakefs:/foo/bar/baz",
			Err:   uri.ErrMalformedURI,
		},
		{
			Input: "lakefs//foo/bar/baz",
			Err:   uri.ErrMalformedURI,
		},
	}

	for i, test := range cases {
		u, err := uri.Parse(test.Input)
		if test.Err != nil {
			if !errors.Is(err, test.Err) {
				t.Fatalf("case (%d) - expected error %v for input %s, got error: %v", i, test.Err, test.Input, err)
			}
			continue
		}
		if !uri.Equals(u, test.Expected) {
			t.Fatalf("case (%d) - expected uri '%s' for input '%s', got uri: '%v'", i, test.Expected, test.Input, u)
		}
	}
}

func TestURI_String(t *testing.T) {
	cases := []struct {
		Input    *uri.URI
		Expected string
	}{
		{&uri.URI{
			Repository: "foo",
			Ref:        "bar",
			Path:       strp("baz/file.csv"),
		}, "lakefs://foo/bar/baz/file.csv"},
		{&uri.URI{
			Repository: "foo",
			Ref:        "bar",
			Path:       strp(""),
		}, "lakefs://foo/bar/"},
		{&uri.URI{
			Repository: "foo",
			Ref:        "bar",
		}, "lakefs://foo/bar"},
		{&uri.URI{
			Repository: "foo",
		}, "lakefs://foo"},
	}

	for i, test := range cases {
		if !strings.EqualFold(test.Input.String(), test.Expected) {
			t.Fatalf("case (%d) - expected '%s', got '%s'", i, test.Expected, test.Input.String())
		}
	}
}

func TestIsValid(t *testing.T) {
	cases := []struct {
		Input    string
		Expected bool
	}{
		{"lakefs://foo/bar/baz", true},
		{"lekefs://foo/bar/baz", false},
	}

	for i, test := range cases {
		if uri.IsValid(test.Input) != test.Expected {
			t.Fatalf("case (%d) - expected %v, got %v", i, test.Expected, uri.IsValid(test.Input))
		}
	}
}

func TestMust(t *testing.T) {
	// should not panic
	u := uri.Must(uri.Parse("lakefs://foo/bar/baz"))
	if !uri.Equals(u, &uri.URI{
		Repository: "foo",
		Ref:        "bar",
		Path:       strp("baz"),
	}) {
		t.Fatalf("expected a parsed URI according to input, instead got %s", u.String())
	}
	recovered := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				recovered = true
			}
		}()
		uri.Must(uri.Parse("lakefsssss://foo/bar"))
	}()

	if !recovered {
		t.Fatalf("expected parsing to cause a panic, it didnt")
	}
}
