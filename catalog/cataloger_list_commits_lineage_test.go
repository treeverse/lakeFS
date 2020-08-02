package catalog

import (
	"context"
	"testing"

	"github.com/go-test/deep"
)

func TestCataloger_ListCommitsLineage(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	commits := setupListCommitsByBranchData(t, ctx, c, repository, "master")
	_ = commits

	testCatalogerBranch(t, ctx, c, repository, "br_1", "master")
	testCatalogerBranch(t, ctx, c, repository, "br_2", "br_1")
	master_commits, _, err := c.ListCommits(ctx, repository, "master", "", 100)
	br_1_commits, _, err := c.ListCommits(ctx, repository, "br_1", "", 100)
	diff := deep.Equal(master_commits, br_1_commits[1:])
	if diff != nil {
		t.Error("br_1 did not inherit commits correctly", diff)
	}
	br_2_commits, _, err := c.ListCommits(ctx, repository, "br_2", "", 100)
	if err != nil {
		t.Fatalf("ListCommits() error = %s", err)
	}
	if len(br_2_commits) != 6 {
		t.Fatalf("ListCommits() len = %d", len(br_2_commits))
	}
	diff = deep.Equal(br_1_commits, br_2_commits[1:])
	if diff != nil {
		t.Error("br_2 did not inherit commits correctly", diff)
	}

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "master-file",
		Checksum:        "ssss",
		PhysicalAddress: "xxxxxxx",
		Size:            10000,
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Write entry for list repository commits failed", err)
	}
	_, err = c.Commit(ctx, repository, "master", "commit master-file", "tester", nil)

	if err != nil {
		t.Fatalf("Commit for list repository commits failed '%s': %s", "master commit failed", err)
	}
	_, err = c.Merge(ctx, repository, "master", "br_1", "tester", "", nil)

	got, _, err := c.ListCommits(ctx, repository, "br_2", "", 100)
	diff = deep.Equal(got, br_2_commits)
	if diff != nil {
		t.Error("br_2 changed althouge not merged", diff)
	}
	master_commits, _, err = c.ListCommits(ctx, repository, "master", "", 100)
	got, _, err = c.ListCommits(ctx, repository, "br_1", "", 100)
	diff = deep.Equal(master_commits[0], got[1])
	if diff != nil {
		t.Error("br_1 did not inherit commits correctly", diff)
	}
}
