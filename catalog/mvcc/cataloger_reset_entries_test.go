package mvcc

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ResetEntries_Basics(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	const branch = "master"
	repository := testCatalogerRepo(t, ctx, c, "repository", branch)
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file1",
		Checksum:        "ffff",
		PhysicalAddress: "/addr1",
		Size:            111,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("create entry for reset entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, branch, "commit file1", "tester", nil); err != nil {
		t.Fatal("Commit for reset entry test:", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file2",
		Checksum:        "eeee",
		PhysicalAddress: "/addr2",
		Size:            222,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("create entry for reset entry test:", err)
	}

	type args struct {
		repository string
		branch     string
		prefix     string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "entries",
			args: args{
				repository: repository,
				branch:     branch,
				prefix:     "/file",
			},
			wantErr: false,
		},
		{
			name: "no entries",
			args: args{
				repository: repository,
				branch:     branch,
				prefix:     "/unknown",
			},
			wantErr: false,
		},
		{
			name: "missing repository",
			args: args{
				repository: "",
				branch:     branch,
				prefix:     "/file3",
			},
			wantErr: true,
		},
		{
			name: "missing branch",
			args: args{
				repository: repository,
				branch:     "",
				prefix:     "/file3",
			},
			wantErr: true,
		},
		{
			name: "missing path",
			args: args{
				repository: repository,
				branch:     branch,
				prefix:     "",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.ResetEntries(ctx, tt.args.repository, tt.args.branch, tt.args.prefix); (err != nil) != tt.wantErr {
				t.Errorf("ResetEntries() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestCataloger_ResetEntries
// test data: 3 files committed on master and b1 (child of master)
// test: for each branch do create, replace and delete operations -> reset
func TestCataloger_ResetEntries(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// create master branch with 3 entries committed
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	for i := 0; i < 3; i++ {
		testutil.Must(t, c.CreateEntry(ctx, repository, "master", catalog.Entry{
			Path:            "/file" + strconv.Itoa(i),
			Checksum:        strings.Repeat("ff", i+1),
			PhysicalAddress: "/addr" + strconv.Itoa(i),
			Size:            int64(i) + 1,
		}, catalog.CreateEntryParams{}))
	}
	if _, err := c.Commit(ctx, repository, "master", "commit changes on master", "tester", nil); err != nil {
		t.Fatal("Commit for reset entry test:", err)
	}

	// create b1 branch with 3 entries committed
	_, err := c.CreateBranch(ctx, repository, "b1", "master")
	testutil.MustDo(t, "Create branch b1 for ResetEntries", err)
	for i := 3; i < 6; i++ {
		testutil.Must(t, c.CreateEntry(ctx, repository, "b1", catalog.Entry{
			Path:            "/file" + strconv.Itoa(i),
			Checksum:        strings.Repeat("ff", i+1),
			PhysicalAddress: "/addr" + strconv.Itoa(i),
			Size:            int64(i) + 1,
		}, catalog.CreateEntryParams{}))
	}
	if _, err := c.Commit(ctx, repository, "b1", "commit changes on b1", "tester", nil); err != nil {
		t.Fatal("Commit for reset entry test:", err)
	}
	testutil.Must(t, c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file2",
		Checksum:        "eeee",
		PhysicalAddress: "/addr2",
		Size:            222,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}))

	// update file on both branches
	testutil.Must(t, c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file0",
		Checksum:        "ee",
		PhysicalAddress: "/addr0",
		Size:            11,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}))
	testutil.Must(t, c.CreateEntry(ctx, repository, "b1", catalog.Entry{
		Path:            "/file3",
		Checksum:        "ee",
		PhysicalAddress: "/addr3",
		Size:            33,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}))

	// create new file on both branches
	testutil.Must(t, c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file10",
		Checksum:        "eeee",
		PhysicalAddress: "/addr10",
		Size:            111,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}))
	testutil.Must(t, c.CreateEntry(ctx, repository, "b1", catalog.Entry{
		Path:            "/file13",
		Checksum:        "eeee",
		PhysicalAddress: "/addr13",
		Size:            333,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}))

	// delete file on both branches
	testutil.Must(t, c.DeleteEntry(ctx, repository, "master", "/file1"))
	testutil.Must(t, c.DeleteEntry(ctx, repository, "b1", "/file4"))

	t.Run("reset master", func(t *testing.T) {
		err := c.ResetEntries(ctx, repository, "master", "/file")
		if err != nil {
			t.Fatal("ResetEntries expected to succeed:", err)
		}
		entries, _, err := c.ListEntries(ctx, repository, catalog.MakeReference("master", catalog.UncommittedID), "", "", "", -1)
		testutil.Must(t, err)
		if len(entries) != 3 {
			t.Fatal("List entries of master branch after reset should return 3 items, got", len(entries))
		}
		for i := 0; i < 3; i++ {
			if entries[i].Size != int64(i+1) {
				t.Fatalf("ResetEntries got mismatch size on entry %d: %d, expected %d", i, entries[i].Size, i+1)
			}
		}
	})
	t.Run("reset b1", func(t *testing.T) {
		err := c.ResetEntries(ctx, repository, "b1", "/file")
		if err != nil {
			t.Fatal("ResetEntries expected to succeed:", err)
		}
		entries, _, err := c.ListEntries(ctx, repository, catalog.MakeReference("b1", catalog.UncommittedID), "", "", "", -1)
		testutil.Must(t, err)
		expectedEntriesLen := 6
		if len(entries) != expectedEntriesLen {
			t.Fatalf("List entries of rested b1 branch got %d items, expected %d", len(entries), expectedEntriesLen)
		}
		for i := 0; i < expectedEntriesLen; i++ {
			if entries[i].Size != int64(i+1) {
				t.Fatalf("ResetEntries got mismatch size on entry %d: %d, expected %d", i, entries[i].Size, i+1)
			}
		}
	})
}
