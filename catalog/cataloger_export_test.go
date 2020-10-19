package catalog

import (
	"context"
	"errors"
	"regexp/syntax"
	"testing"

	"github.com/go-test/deep"
	"github.com/lib/pq"
)

const (
	prefix        = "prefix1"
	defaultBranch = "main"
	anotherBranch = "lost-not-found"
)

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
		LastKeysInPrefixRegexp: pq.StringArray{"xyz+y"},
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

	t.Run("reconfigured branch", func(t *testing.T) {
		newCfg := ExportConfiguration{
			Path:                   "/better/to/export",
			StatusPath:             "/better/for/status",
			LastKeysInPrefixRegexp: pq.StringArray{"abc", "def", "xyz"},
		}
		if err := c.PutExportConfiguration(repo, defaultBranch, &newCfg); err != nil {
			t.Fatalf("update configuration with %+v: %s", newCfg, err)
		}
		gotCfg, err := c.GetExportConfigurationForBranch(repo, defaultBranch)
		if err != nil {
			t.Errorf("get updated configuration for configured branch failed: %s", err)
		}
		if diffs := deep.Equal(newCfg, gotCfg); diffs != nil {
			t.Errorf("got other configuration than expected: %s", diffs)
		}
	})

	t.Run("invalid regexp", func(t *testing.T) {
		badCfg := ExportConfiguration{
			Path:                   "/better/to/export",
			StatusPath:             "/better/for/status",
			LastKeysInPrefixRegexp: pq.StringArray{"(unclosed"},
		}
		err := c.PutExportConfiguration(repo, defaultBranch, &badCfg)
		var regexpErr *syntax.Error
		if !errors.As(err, &regexpErr) {
			t.Fatalf("update configuration with bad %+v did not give a regexp error: %s", badCfg, err)
		}
		if regexpErr.Code != syntax.ErrMissingParen {
			t.Errorf("expected configuration update with bad %+v to give missing paren, but got %s", badCfg, regexpErr.Code)
		}
	})
}
