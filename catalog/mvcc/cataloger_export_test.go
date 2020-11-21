package mvcc

import (
	"context"
	"errors"
	"fmt"
	"regexp/syntax"
	"sort"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const (
	prefix        = "prefix1"
	defaultBranch = "main"
	anotherBranch = "lost-not-found"
)

// configForBranchSlice adapts a slice to satisfy sort.Interface
type configForBranchSlice []catalog.ExportConfigurationForBranch

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
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, prefix, defaultBranch)

	cfg := catalog.ExportConfiguration{
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
		newCfg := catalog.ExportConfiguration{
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

	t.Run("continuous", func(t *testing.T) {
		newCfg := catalog.ExportConfiguration{
			Path:                   "/better/to/export",
			StatusPath:             "/better/for/status",
			LastKeysInPrefixRegexp: pq.StringArray{"abc", "def", "xyz"},
			IsContinuous:           true,
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
		badCfg := catalog.ExportConfiguration{
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
		moreCfg := catalog.ExportConfiguration{
			Path:       "/more/to/export",
			StatusPath: "/more/for/status",
		}
		expected := []catalog.ExportConfigurationForBranch{
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

func TestExportState(t *testing.T) {
	const (
		ref1 = "this commit"
		ref2 = "that commit"
	)
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, prefix, defaultBranch)

	cases := []struct {
		name         string
		startRef     string // start with this ref (and state) if set, otherwise start with no row
		startState   catalog.CatalogBranchExportStatus
		startMessage *string
		setRef       string
		expectState  catalog.CatalogBranchExportStatus
		expectErr    func(t *testing.T, err error)
	}{
		{
			name:        "clean",
			setRef:      ref2,
			expectState: catalog.ExportStatusInProgress,
			expectErr: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("unexpected error %s", err)
				}
			},
		}, {
			name:        "reset",
			startRef:    ref1,
			setRef:      ref2,
			expectState: catalog.ExportStatusInProgress,
			expectErr: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("unexpected error %s", err)
				}
			},
		}, {
			name:        "previousSucceeded",
			startRef:    ref1,
			startState:  catalog.ExportStatusSuccess,
			setRef:      ref2,
			expectState: catalog.ExportStatusInProgress,
			expectErr: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("unexpected error %s", err)
				}
			},
		}, {
			name:         "previousFailed",
			startRef:     ref1,
			startState:   catalog.ExportStatusFailed,
			startMessage: swag.String("humpty dumpty had a great fall"),
			setRef:       ref2,
			expectState:  catalog.ExportStatusInProgress,
			expectErr: func(t *testing.T, err error) {
				if !errors.Is(err, ErrExportFailed) {
					t.Errorf("expected ErrExportFailed but got %s", err)
				}
			},
		},
	}
	for _, tt := range cases {
		pool, err := pgxpool.Connect(ctx, c.DbConnURI)
		if err != nil {
			t.Fatalf(err.Error())
		}
		err = db.Ping(ctx, pool)
		if err != nil {
			t.Fatalf(err.Error())
		}
		d := db.NewPgxDatabase(pool)
		t.Run(tt.name, func(t *testing.T) {
			_, err = d.Transact(func(tx db.Tx) (interface{}, error) {
				// Clean up any existing state
				if err := c.ExportStateDelete(tx, repo, defaultBranch); err != nil && !errors.Is(err, ErrEntryNotFound) {
					return nil, fmt.Errorf("setup (delete): %w", err)
				}

				if tt.startRef != "" {
					// This also ends up testing ExportStateMarkStart in the same way
					// each time.
					if _, _, err := c.ExportStateMarkStart(tx, repo, defaultBranch, tt.startRef); err != nil {
						return nil, fmt.Errorf("setup (mark previous): %w", err)
					}
					// Test ExportMarkEnd if previous state is configured.
					if tt.startState != "" && tt.startState != catalog.ExportStatusInProgress {
						err := c.ExportStateMarkEnd(tx, repo, defaultBranch, tt.startRef, tt.startState, tt.startMessage)
						if err != nil {
							return nil, fmt.Errorf("setup (set previous): %w", err)
						}
					}
				}

				gotRef, gotState, err := c.ExportStateMarkStart(tx, repo, defaultBranch, tt.setRef)
				if tt.expectErr != nil {
					tt.expectErr(t, err)
				}
				if gotRef != tt.startRef {
					t.Errorf("expected to old ref %s but got %s", tt.startRef, gotRef)
				}
				if tt.startState != "" && gotState != tt.startState {
					t.Errorf("expected previous state %s but got %s", tt.startState, gotState)
				}
				return nil, nil
			})
			if err != nil {
				t.Errorf(err.Error())
			}
		})
	}
}
