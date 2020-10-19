package catalog

import (
	"context"
	"errors"
	"regexp/syntax"
	"sort"
	"testing"

	"github.com/go-test/deep"
	"github.com/lib/pq"
)

const (
	prefix        = "prefix1"
	defaultBranch = "main"
	anotherBranch = "lost-not-found"
)

// configForBranchSlice adapts a slice to satisfy sort.Interface
type configForBranchSlice []ExportConfigurationForBranch

func (s configForBranchSlice) Len() int { return len(s) }

func (s configForBranchSlice) Less(i int, j int) bool {
	if s[i].Repository < s[j].Repository {
		return true
	}
	if s[i].Repository > s[j].Repository {
		return false
	}
	return s[i].Branch < s[j].Branch
}

func (s configForBranchSlice) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
}

func TestExportConfiguration(t *testing.T) {
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

	t.Run("GetExportConfigurations", func(t *testing.T) {
		moreBranch := "secondary"
		if _, err := c.CreateBranch(ctx, repo, moreBranch, defaultBranch); err != nil {
			t.Fatalf("create secondary branch: %s", err)
		}
		moreCfg := ExportConfiguration{
			Path:       "/more/to/export",
			StatusPath: "/more/for/status",
		}
		expected := []ExportConfigurationForBranch{
			{
				Repository:             repo,
				Branch:                 defaultBranch,
				Path:                   cfg.Path,
				StatusPath:             cfg.StatusPath,
				LastKeysInPrefixRegexp: cfg.LastKeysInPrefixRegexp,
			}, {
				Repository:             repo,
				Branch:                 moreBranch,
				Path:                   moreCfg.Path,
				StatusPath:             moreCfg.StatusPath,
				LastKeysInPrefixRegexp: moreCfg.LastKeysInPrefixRegexp,
			},
		}

		if err := c.PutExportConfiguration(repo, defaultBranch, &cfg); err != nil {
			t.Fatalf("add configuration with %+v failed: %s", cfg, err)
		}
		if err := c.PutExportConfiguration(repo, moreBranch, &moreCfg); err != nil {
			t.Fatalf("add configuration with %+v failed: %s", moreCfg, err)
		}
		got, err := c.GetExportConfigurations()
		if err != nil {
			t.Fatal(err)
		}
		sort.Sort(configForBranchSlice(expected))
		sort.Sort(configForBranchSlice(got))
		if diffs := deep.Equal(expected, got); diffs != nil {
			t.Errorf("did not read expected configurations: %s", diffs)
		}
	})
}
