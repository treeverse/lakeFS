package mvcc

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

func TestCataloger_ResetEntry(t *testing.T) {
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
		path       string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "committed file",
			args: args{
				repository: repository,
				branch:     branch,
				path:       "/file1",
			},
			wantErr: true,
		},
		{
			name: "uncommitted file",
			args: args{
				repository: repository,
				branch:     branch,
				path:       "/file2",
			},
			wantErr: false,
		},
		{
			name: "file not found",
			args: args{
				repository: repository,
				branch:     branch,
				path:       "/fileX",
			},
			wantErr: true,
		},
		{
			name: "missing repository",
			args: args{
				repository: "",
				branch:     branch,
				path:       "/file3",
			},
			wantErr: true,
		},
		{
			name: "missing branch",
			args: args{
				repository: repository,
				branch:     "",
				path:       "/file3",
			},
			wantErr: true,
		},
		{
			name: "missing path",
			args: args{
				repository: repository,
				branch:     branch,
				path:       "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.ResetEntry(ctx, tt.args.repository, tt.args.branch, tt.args.path); (err != nil) != tt.wantErr {
				t.Errorf("ResetEntry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCataloger_ResetEntry_NewToNone(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file1",
		Checksum:        "ff",
		PhysicalAddress: "/addr1",
		Size:            1,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("create entry for reset entry test:", err)
	}
	if err := c.ResetEntry(ctx, repository, "master", "/file1"); err != nil {
		t.Fatal("ResetEntry should reset new uncommitted file:", err)
	}
	_, err := c.GetEntry(ctx, repository, MakeReference("master", UncommittedID), "/file1", catalog.GetEntryParams{})
	expectedErr := db.ErrNotFound
	if !errors.Is(err, expectedErr) {
		t.Fatalf("ResetEntry expecting the file to be gone with %s, got = %s", expectedErr, err)
	}
}

func TestCataloger_ResetEntry_NewToPrevious(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file1",
		Checksum:        "ff",
		PhysicalAddress: "/addr1",
		Size:            1,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("create entry for reset entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1", "tester", nil); err != nil {
		t.Fatal("Commit for reset entry test:", err)
	}
	const newChecksum = "eeee"
	const newPhysicalAddress = "/addrNew"
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file1",
		Checksum:        newChecksum,
		PhysicalAddress: newPhysicalAddress,
		Size:            2,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("create entry for reset entry test:", err)
	}
	ent, err := c.GetEntry(ctx, repository, MakeReference("master", UncommittedID), "/file1", catalog.GetEntryParams{})
	if err != nil {
		t.Fatal("ResetEntry expecting previous file to be found:", err)
	}
	if ent.Checksum != newChecksum {
		t.Errorf("ResetEntry should find previus entry with checksum %s, got %s", newChecksum, ent.Checksum)
	}
	if ent.PhysicalAddress != newPhysicalAddress {
		t.Errorf("ResetEntry should find previus entry with checksum %s, got %s", newPhysicalAddress, ent.Checksum)
	}
}

func TestCataloger_ResetEntry_Committed(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file1",
		Checksum:        "ff",
		PhysicalAddress: "/addr1",
		Size:            1,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("create entry for reset entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1", "tester", nil); err != nil {
		t.Fatal("Commit for reset entry test:", err)
	}
	err := c.ResetEntry(ctx, repository, "master", "/file1")
	expectedErr := db.ErrNotFound
	if !errors.Is(err, expectedErr) {
		t.Fatal("ResetEntry expected not to find file in case nothing to reset: ", err)
	}
}

func TestCataloger_ResetEntry_CommittedParentBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file1",
		Checksum:        "ff",
		PhysicalAddress: "/addr1",
		Size:            1,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("create entry for reset entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1", "tester", nil); err != nil {
		t.Fatal("Commit for reset entry test:", err)
	}
	_, err := c.CreateBranch(ctx, repository, "b1", "master")
	if err != nil {
		t.Fatal("create branch for reset entry test:", err)
	}
	err = c.ResetEntry(ctx, repository, "b1", "/file1")
	expectedErr := db.ErrNotFound
	if !errors.Is(err, expectedErr) {
		t.Fatal("ResetEntry expected not to find file in case nothing to reset:", err)
	}
}

func TestCataloger_ResetEntry_UncommittedDeleteSameBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file1",
		Checksum:        "ff",
		PhysicalAddress: "/addr1",
		Size:            1,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("create entry for reset entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1", "tester", nil); err != nil {
		t.Fatal("Commit for reset entry test:", err)
	}
	err := c.DeleteEntry(ctx, repository, "master", "/file1")
	if err != nil {
		t.Fatal("delete entry for reset entry test:", err)
	}
	err = c.ResetEntry(ctx, repository, "master", "/file1")
	if err != nil {
		t.Fatal("ResetEntry expected successful reset on delete entry:", err)
	}
	ent, err := c.GetEntry(ctx, repository, MakeReference("master", UncommittedID), "/file1", catalog.GetEntryParams{})
	if err != nil {
		t.Fatal("get entry for reset entry test:", err)
	}
	if ent.Path != "/file1" || ent.PhysicalAddress != "/addr1" {
		t.Fatalf("catalog.Entry should be reseted back to /file1 /addr1, got %+v", ent)
	}
}

func TestCataloger_ResetEntry_UncommittedDeleteParentBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file1",
		Checksum:        "ff",
		PhysicalAddress: "/addr1",
		Size:            1,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("create entry for reset entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1", "tester", nil); err != nil {
		t.Fatal("Commit for reset entry test:", err)
	}
	if _, err := c.CreateBranch(ctx, repository, "b1", "master"); err != nil {
		t.Fatal("create branch for reset entry test:", err)
	}
	err := c.DeleteEntry(ctx, repository, "b1", "/file1")
	if err != nil {
		t.Fatal("delete entry for reset entry test:", err)
	}
	err = c.ResetEntry(ctx, repository, "b1", "/file1")
	if err != nil {
		t.Fatal("ResetEntry expected successful reset on delete entry:", err)
	}
	ent, err := c.GetEntry(ctx, repository, MakeReference("b1", UncommittedID), "/file1", catalog.GetEntryParams{})
	if err != nil {
		t.Fatal("get entry for reset entry test:", err)
	}
	if ent.Path != "/file1" || ent.PhysicalAddress != "/addr1" {
		t.Fatalf("catalog.Entry should be reseted back to /file1 /addr1, got %+v", ent)
	}
}
