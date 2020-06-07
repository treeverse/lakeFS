package catalog

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_RevertEntries_Basics(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	const branch = "master"
	repo := testCatalogerRepo(t, ctx, c, "repo", branch)
	if err := c.CreateEntry(ctx, repo, "master", "/file1", "ffff", "/addr1", 111, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
	}
	if _, err := c.Commit(ctx, repo, branch, "commit file1", "tester", nil); err != nil {
		t.Fatal("commit for revert entry test:", err)
	}
	if err := c.CreateEntry(ctx, repo, "master", "/file2", "eeee", "/addr2", 222, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
	}

	type args struct {
		repo   string
		branch string
		prefix string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "entries",
			args: args{
				repo:   repo,
				branch: branch,
				prefix: "/file",
			},
			wantErr: false,
		},
		{
			name: "no entries",
			args: args{
				repo:   repo,
				branch: branch,
				prefix: "/unknown",
			},
			wantErr: false,
		},
		{
			name: "missing repo",
			args: args{
				repo:   "",
				branch: branch,
				prefix: "/file3",
			},
			wantErr: true,
		},
		{
			name: "missing branch",
			args: args{
				repo:   repo,
				branch: "",
				prefix: "/file3",
			},
			wantErr: true,
		},
		{
			name: "missing path",
			args: args{
				repo:   repo,
				branch: branch,
				prefix: "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.RevertEntries(ctx, tt.args.repo, tt.args.branch, tt.args.prefix); (err != nil) != tt.wantErr {
				t.Errorf("RevertEntries() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestCataloger_RevertEntries
// test data: 3 files committed on master and b1 (child of master)
// test: for each branch do create, replace and delete operations -> revert
func TestCataloger_RevertEntries(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// create master branch with 3 entries committed
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	for i := 0; i < 3; i++ {
		testutil.Must(t, c.CreateEntry(ctx, repo, "master", "/file"+strconv.Itoa(i), strings.Repeat("ff", i+1), "/addr"+strconv.Itoa(i), i+1, nil))
	}
	if _, err := c.Commit(ctx, repo, "master", "commit changes on master", "tester", nil); err != nil {
		t.Fatal("Commit for revert entry test:", err)
	}

	// create b1 branch with 3 entries committed
	_, err := c.CreateBranch(ctx, repo, "b1", "master")
	if err != nil {
		t.Fatal("CreateBranch for RevertEntries:", err)
	}
	for i := 3; i < 6; i++ {
		testutil.Must(t, c.CreateEntry(ctx, repo, "b1", "/file"+strconv.Itoa(i), strings.Repeat("ff", i+1), "/addr"+strconv.Itoa(i), i+1, nil))
	}
	if _, err := c.Commit(ctx, repo, "b1", "commit changes on b1", "tester", nil); err != nil {
		t.Fatal("Commit for revert entry test:", err)
	}
	testutil.Must(t, c.CreateEntry(ctx, repo, "master", "/file2", "eeee", "/addr2", 222, nil))

	// update file on both branches
	testutil.Must(t, c.CreateEntry(ctx, repo, "master", "/file0", "ee", "/addr0", 11, nil))
	testutil.Must(t, c.CreateEntry(ctx, repo, "b1", "/file3", "ee", "/addr3", 33, nil))

	// create new file on both branches
	testutil.Must(t, c.CreateEntry(ctx, repo, "master", "/file10", "eeee", "/addr10", 111, nil))
	testutil.Must(t, c.CreateEntry(ctx, repo, "b1", "/file13", "eeee", "/addr13", 333, nil))

	// delete file on both branches
	testutil.Must(t, c.DeleteEntry(ctx, repo, "master", "/file1"))
	testutil.Must(t, c.DeleteEntry(ctx, repo, "b1", "/file4"))

	t.Run("revert master", func(t *testing.T) {
		err = c.RevertEntries(ctx, repo, "master", "/file")
		if err != nil {
			t.Fatal("RevertEntries expected to succeed:", err)
		}
		entries, _, err := c.ListEntries(ctx, repo, "master", "", "", -1, false, true)
		testutil.Must(t, err)
		if len(entries) != 3 {
			t.Fatal("List entries of reverted master branch should return 3 items, got", len(entries))
		}
		for i := 0; i < 3; i++ {
			if entries[i].Size != int64(i+1) {
				t.Fatalf("RevertEntries got mismatch size on entry %d: %d, expected %d", i, entries[i].Size, i+1)
			}
		}
	})
	t.Run("revert b1", func(t *testing.T) {
		err = c.RevertEntries(ctx, repo, "b1", "/file")
		if err != nil {
			t.Fatal("RevertEntries expected to succeed:", err)
		}
		entries, _, err := c.ListEntries(ctx, repo, "b1", "", "", -1, false, true)
		testutil.Must(t, err)
		if len(entries) != 6 {
			t.Fatal("List entries of reverted b1 branch should return 3 items, got", len(entries))
		}
		for i := 0; i < 6; i++ {
			if entries[i].Size != int64(i+1) {
				t.Fatalf("RevertEntries got mismatch size on entry %d: %d, expected %d", i, entries[i].Size, i+1)
			}
		}
	})
}
