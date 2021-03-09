package uri_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/uri"
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
		{"lakefs://foo@bar/baz", nil, &uri.URI{
			Protocol:   "lakefs",
			Repository: "foo",
			Ref:        "bar",
			Path:       strp("baz"),
		}},
		{"lakefs://foo@bar/baz/path", nil, &uri.URI{
			Protocol:   "lakefs",
			Repository: "foo",
			Ref:        "bar",
			Path:       strp("baz/path"),
		}},
		{"lakefs://foo@bar/baz/path@withappendix.foo", nil, &uri.URI{
			Protocol:   "lakefs",
			Repository: "foo",
			Ref:        "bar",
			Path:       strp("baz/path@withappendix.foo"),
		}},
		{"lakefs://fo/o@bar/baz/path@withappendix.foo", nil, &uri.URI{
			Protocol:   "lakefs",
			Repository: "fo/o",
			Ref:        "bar",
			Path:       strp("baz/path@withappendix.foo"),
		}},
		{"lakefs://foo", nil, &uri.URI{
			Protocol:   "lakefs",
			Repository: "foo",
		}},
		{"lakefs://foo@bar/", nil, &uri.URI{
			Protocol:   "lakefs",
			Repository: "foo",
			Ref:        "bar",
			Path:       strp(""),
		}},
		{"lakefs://foo@bar", nil, &uri.URI{
			Protocol:   "lakefs",
			Repository: "foo",
			Ref:        "bar",
		}},
		{"lakefssss://foo@bar/baz", uri.ErrMalformedURI, nil},
		{"lakefs:/foo@bar/baz", uri.ErrMalformedURI, nil},
		{"lakefs//foo@bar/baz", uri.ErrMalformedURI, nil},
	}

	for i, test := range cases {
		u, err := uri.Parse(test.Input)
		if test.Err != nil {
			if !errors.Is(err, test.Err) {
				t.Fatalf("case (%d) - expected error %v for input %s, got error: %v", i, test.Err, test.Input, err)
			} else {
				continue
			}
		}
		if !uri.Equals(u, test.Expected) {
			t.Fatalf("case (%d) - expected uri %s for input %s, got uri: %v", i, test.Expected, test.Input, u)
		}
	}
}

func TestURI_String(t *testing.T) {
	cases := []struct {
		Input    *uri.URI
		Expected string
	}{
		{&uri.URI{
			Protocol:   "lakefs",
			Repository: "foo",
			Ref:        "bar",
			Path:       strp("baz/file.csv"),
		}, "lakefs://foo@bar/baz/file.csv"},
		{&uri.URI{
			Protocol:   "lakefs",
			Repository: "foo",
			Ref:        "bar",
			Path:       strp(""),
		}, "lakefs://foo@bar/"},
		{&uri.URI{
			Protocol:   "lakefs",
			Repository: "foo",
			Ref:        "bar",
		}, "lakefs://foo@bar"},
		{&uri.URI{
			Protocol:   "lakefs",
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
		{"lakefs://foo@bar/baz", true},
		{"lekefs://foo@bar/baz", false},
	}

	for i, test := range cases {
		if uri.IsValid(test.Input) != test.Expected {
			t.Fatalf("case (%d) - expected %v, got %v", i, test.Expected, uri.IsValid(test.Input))
		}
	}
}

func TestMust(t *testing.T) {
	// should not panic
	u := uri.Must(uri.Parse("lakefs://foo@bar/baz"))
	if !uri.Equals(u, &uri.URI{
		Protocol:   "lakefs",
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
		uri.Must(uri.Parse("lakefsssss://foo@bar"))
	}()

	if !recovered {
		t.Fatalf("expected parsing to cause a panic, it didnt")
	}
}
