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
	return a.Path < b.Path
	// No need to compare physical addresses
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
		PhysicalAddress: "/history/1",
		CreationDate:    time.Now().Add(-20 * time.Hour),
		Checksum:        "1",
	}); err != nil {
		t.Fatal("Failed to create 0/historical on master", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/committed",
		PhysicalAddress: "/committed/1",
		CreationDate:    time.Now().Add(-20 * time.Hour),
		Checksum:        "2",
	}); err != nil {
		t.Fatal("Failed to create 0/committed on master", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "first commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit first commit to master", err)
	}

	if err := c.CreateEntry(ctx, repository, "slow", Entry{
		Path:            "0/committed",
		PhysicalAddress: "/committed/2",
		CreationDate:    time.Now().Add(-15 * time.Hour),
		Checksum:        "3",
	}); err != nil {
		t.Fatal("Failed to create 0/committed on slow", err)
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
						Expiration:   retention.Expiration{All: makeHours(0)},
					},
				},
			},
			wantErr: false,
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
