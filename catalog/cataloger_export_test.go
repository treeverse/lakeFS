package catalog

import (
	"context"
	"errors"
	syntax "regexp/syntax"
	"testing"

	"github.com/go-test/deep"
)

const (
	prefix        = "prefix1"
	defaultBranch = "main"
	anotherBranch = "lost-not-found"
)

func TestDisjunctRegexps(t *testing.T) {
	cases := []struct {
		name    string
		input   []string
		errCode syntax.ErrorCode
		output  string
	}{
		{name: "empty", output: ""},
		{name: "one", input: []string{"^foo$"}, output: "(?:^foo$)"},
		{name: "two", input: []string{"^foo", "bar$"}, output: "(?:^foo)|(?:bar$)"},
		{name: "many", input: []string{"a", "b", "c", "z"}, output: "(?:a)|(?:b)|(?:c)|(?:z)"},
		{name: "error on first", input: []string{"("}, errCode: syntax.ErrMissingParen},
		{name: "error on second", input: []string{"a", "[z", "!"}, errCode: syntax.ErrMissingBracket},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := DisjunctRegexps(c.input)
			if err != nil {
				var regexpErr *syntax.Error
				ok := errors.As(err, &regexpErr)
				if !ok {
					t.Errorf("unexpected non-regexp error type %T: %s", err, err)
				} else if regexpErr.Code != c.errCode {
					t.Errorf("expected regexp code \"%s\" but got \"%s\"",
						c.errCode, regexpErr.Code)
				}
				return
			}
			if c.errCode != "" {
				t.Errorf("expected error \"%s\" but succeeded", c.errCode)
			}
			if got.String() != c.output {
				t.Errorf("expected %s, got %s", c.output, got)
			}
		})
	}
}

func TestDeconstructDisjunctionRoundtrip(t *testing.T) {
	cases := []struct {
		name  string
		input []string
	}{
		{name: "empty"},
		{name: "one", input: []string{"^foo$"}},
		{name: "two", input: []string{"^foo", "bar$"}},
		{name: "many", input: []string{"a", "b", "c", "z"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			regexp, err := DisjunctRegexps(c.input)
			if err != nil {
				t.Fatalf("%s: %s", c.input, err)
			}
			got, err := DeconstructDisjunction(regexp)
			if err != nil {
				t.Errorf("deconstruct %s for %v failed: %s", regexp, c.input, err)
			}
			if diffs := deep.Equal(c.input, got); diffs != nil {
				t.Errorf("round-trip %v -> %s: %s", c.input, regexp, diffs)
			}
		})
	}

}

func TestExport_GetConfiguration(t *testing.T) {
	const (
		branchID        = 17
		anotherBranchID = 29
	)
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, prefix, defaultBranch)

	cfg := ExportConfiguration{
		Path:                   "/path/to/export",
		StatusPath:             "/path/to/status",
		LastKeysInPrefixRegexp: "*&@!#$",
	}

	if err := c.PutExportConfiguration(repo, defaultBranch, &cfg); err != nil {
		t.Fatal(err)
	}

	t.Run("unconfigured branch", func(t *testing.T) {
		gotCfg, err := c.GetExportConfigurationForBranch(repo, anotherBranch)
		if !errors.Is(err, ErrBranchNotFound) {
			t.Errorf("get configuration for unconfigured branch failed: expected ErrBranchNotFound but got %s (and %+v)", err, gotCfg)
		}
	})

	t.Run("configured branch", func(t *testing.T) {
		gotCfg, err := c.GetExportConfigurationForBranch(repo, defaultBranch)
		if err != nil {
			t.Errorf("get configuration for configured branch failed: %s", err)
		}
		if diffs := deep.Equal(cfg, gotCfg); diffs != nil {
			t.Errorf("got other configuration than expected: %s", diffs)
		}
	})
}
