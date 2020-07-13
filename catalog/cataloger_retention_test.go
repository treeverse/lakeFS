package catalog

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/retention"
)

func makeHours(hours int) *retention.TimePeriodHours {
	ret := retention.TimePeriodHours(hours)
	return &ret
}

func readFrom(ch chan ExpireResult) []ExpireResult {
	ret := make([]ExpireResult, 0, 10)
	for er := range ch {
		ret = append(ret, er)
	}
	return ret
}

// less compares two ExpireResults, to help sort with sort.Slice.
func less(a, b *ExpireResult) bool {
	if a.Repository < b.Repository {
		return true
	}
	if a.Repository > b.Repository {
		return false
	}
	if a.Branch < b.Branch {
		return true
	}
	if a.Branch > b.Branch {
		return false
	}
	if a.Path < b.Path {
		return true
	}
	if a.Path > b.Path {
		return false
	}
	return a.PhysicalPath < b.PhysicalPath
}

// sortResults sorts a slice of ExpireResults
func sortResults(results []ExpireResult) {
	sort.Slice(results, func(i, j int) bool { return less(&results[i], &results[j]) })
}

func TestCataloger_ScanExpired(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	testCatalogerBranch(t, ctx, c, repository, "slow", "master")
	testCatalogerBranch(t, ctx, c, repository, "fast", "slow")

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/master/history/1",
		CreationDate:    time.Now().Add(-20 * time.Hour),
		Checksum:        "1",
	}); err != nil {
		t.Fatal("Failed to create 0/historical on master", err)
	}
	masterHistorical20Hours := ExpireResult{
		Repository:   repository,
		Branch:       "master",
		Path:         "0/historical",
		PhysicalPath: "/master/history/1",
	}
	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/committed",
		PhysicalAddress: "/committed/1",
		CreationDate:    time.Now().Add(-19 * time.Hour),
		Checksum:        "2",
	}); err != nil {
		t.Fatal("Failed to create 0/committed on master", err)
	}
	masterCommitted19Hours := ExpireResult{
		Repository:   repository,
		Branch:       "master",
		Path:         "0/committed",
		PhysicalPath: "/committed/1",
	}
	if _, err := c.Commit(ctx, repository, "master", "first commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit first commit to master", err)
	}

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/master/history/2",
		CreationDate:    time.Now().Add(-19 * time.Hour),
		Checksum:        "2",
	}); err != nil {
		t.Fatal("Failed to update 0/historical on master", err)
	}
	masterHistorical19Hours := ExpireResult{
		Repository:   repository,
		Branch:       "master",
		Path:         "0/historical",
		PhysicalPath: "/master/history/2",
	}
	if _, err := c.Commit(ctx, repository, "master", "second commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit second commit to master", err)
	}

	if err := c.CreateEntry(ctx, repository, "slow", Entry{
		Path:            "0/committed",
		PhysicalAddress: "/committed/2",
		CreationDate:    time.Now().Add(-15 * time.Hour),
		Checksum:        "3",
	}); err != nil {
		t.Fatal("Failed to create 0/committed on slow", err)
	}
	slowCommitted15Hours := ExpireResult{
		Repository:   repository,
		Branch:       "slow",
		Path:         "0/committed",
		PhysicalPath: "/committed/2",
	}
	if _, err := c.Commit(ctx, repository, "slow", "first slow commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit to slow", err)
	}

	if err := c.CreateEntry(ctx, repository, "fast", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/history/2",
		CreationDate:    time.Now().Add(-15 * time.Hour),
		Checksum:        "3",
	}); err != nil {
		t.Fatal("Failed to update 0/historical on fast", err)
	}
	fastCommitted15Hours := ExpireResult{
		Repository:   repository,
		Branch:       "fast",
		Path:         "0/historical",
		PhysicalPath: "/history/2",
	}
	if _, err := c.Commit(ctx, repository, "fast", "first fast commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit first fast commit", err)
	}
	if err := c.CreateEntry(ctx, repository, "fast", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/history/3",
		CreationDate:    time.Now().Add(-5 * time.Hour),
		Checksum:        "4",
	}); err != nil {
		t.Fatal("Failed to update 0/historical again on fast", err)
	}
	fastCommitted5Hours := ExpireResult{
		Repository:   repository,
		Branch:       "fast",
		Path:         "0/historical",
		PhysicalPath: "/history/3",
	}
	if _, err := c.Commit(ctx, repository, "fast", "second fast commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit second fast commit", err)
	}

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/history/4",
		CreationDate:    time.Now().Add(-2 * time.Hour),
		Checksum:        "5",
	}); err != nil {
		t.Fatal("Failed to update 0/historical on master", err)
	}
	masterUncommitted2Hours := ExpireResult{
		Repository:   repository,
		Branch:       "master",
		Path:         "0/historical",
		PhysicalPath: "/history/4",
	}

	tests := []struct {
		name    string
		policy  *retention.Policy
		want    []ExpireResult
		wantErr bool
	}{
		{
			name: "expire nothing",
			policy: &retention.Policy{
				Rules: []retention.Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   retention.Expiration{All: makeHours(50)},
					},
				},
			},
			want: []ExpireResult{},
		}, {
			name: "expire all",
			policy: &retention.Policy{
				Rules: []retention.Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   retention.Expiration{All: makeHours(0)},
					},
				},
			},
			want: []ExpireResult{
				masterHistorical20Hours,
				masterHistorical19Hours,
				masterCommitted19Hours,
				slowCommitted15Hours,
				fastCommitted15Hours,
				fastCommitted5Hours,
				masterUncommitted2Hours,
			},
		}, {
			name: "expire uncommitted",
			policy: &retention.Policy{
				Rules: []retention.Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   retention.Expiration{Uncommitted: makeHours(0)},
					},
				},
			},
			want: []ExpireResult{masterUncommitted2Hours},
		}, {
			name: "expire all noncurrent",
			policy: &retention.Policy{
				Rules: []retention.Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   retention.Expiration{Noncurrent: makeHours(0)},
					},
				},
			},
			want: []ExpireResult{
				masterHistorical20Hours,
				fastCommitted15Hours,
			},
		}, {
			name: "expire old noncurrent",
			policy: &retention.Policy{
				Rules: []retention.Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   retention.Expiration{Noncurrent: makeHours(18)},
					},
				},
			},
			want: []ExpireResult{
				masterHistorical20Hours,
			},
		}, {
			name: "expire uncommitted and old noncurrent",
			policy: &retention.Policy{
				Rules: []retention.Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration: retention.Expiration{
							Noncurrent:  makeHours(18),
							Uncommitted: makeHours(0),
						},
					},
				},
			},
			want: []ExpireResult{
				masterHistorical20Hours,
				masterUncommitted2Hours,
			},
		}, {
			name: "expire by branch",
			policy: &retention.Policy{
				Rules: []retention.Rule{
					{
						Enabled:      true,
						FilterPrefix: "master/",
						Expiration: retention.Expiration{
							All: makeHours(0),
						},
					},
				},
			},
			want: []ExpireResult{
				masterHistorical20Hours,
				masterHistorical19Hours,
				masterCommitted19Hours,
				masterUncommitted2Hours,
			},
		}, {
			name: "expire noncurrent by branch",
			policy: &retention.Policy{
				Rules: []retention.Rule{
					{
						Enabled:      true,
						FilterPrefix: "master/",
						Expiration: retention.Expiration{
							Noncurrent: makeHours(0),
						},
					},
				},
			},
			want: []ExpireResult{
				masterHistorical20Hours,
			},
		}, {
			name: "expire by branch and path prefix",
			policy: &retention.Policy{
				Rules: []retention.Rule{
					{
						Enabled:      true,
						FilterPrefix: "master/0/",
						Expiration: retention.Expiration{
							All: makeHours(0),
						},
					},
				},
			},
			want: []ExpireResult{
				masterHistorical20Hours,
				masterHistorical19Hours,
				masterCommitted19Hours,
				masterUncommitted2Hours,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan ExpireResult)
			go func() {
				err := c.ScanExpired(ctx, repository, tt.policy, ch)
				if err != nil {
					t.Error("Scan for expired failed", err)
				}
			}()
			got := readFrom(ch)

			sortResults(tt.want)
			sortResults(got)

			if diffs := deep.Equal(tt.want, got); diffs != nil {
				t.Errorf("did not expire as expected, diffs %s", diffs)
				t.Errorf("expected %+v, got %+v", tt.want, got)
			}
		})
	}
}
