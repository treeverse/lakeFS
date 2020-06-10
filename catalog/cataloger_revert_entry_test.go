package catalog

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/db"
)

func TestCataloger_RevertEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	const branch = "master"
	repository := testCatalogerRepo(t, ctx, c, "repository", branch)
	if err := c.CreateEntry(ctx, repository, "master", "/file1", "ffff", "/addr1", 111, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, branch, "commit file1", "tester", nil); err != nil {
		t.Fatal("commit for revert entry test:", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", "/file2", "eeee", "/addr2", 222, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
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
			if err := c.RevertEntry(ctx, tt.args.repository, tt.args.branch, tt.args.path); (err != nil) != tt.wantErr {
				t.Errorf("RevertEntry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCataloger_RevertEntry_NewToNone(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", "/file1", "ff", "/addr1", 1, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
	}
	if err := c.RevertEntry(ctx, repository, "master", "/file1"); err != nil {
		t.Fatal("RevertEntry should revert new uncommitted file:", err)
	}
	_, err := c.GetEntry(ctx, repository, "master", "/file1", true)
	expectedErr := db.ErrNotFound
	if !errors.As(err, &expectedErr) {
		t.Fatalf("RevertEntry expecting the file to be gone with %s, got = %s", expectedErr, err)
	}
}

func TestCataloger_RevertEntry_NewToPrevious(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", "/file1", "ff", "/addr1", 1, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1", "tester", nil); err != nil {
		t.Fatal("commit for revert entry test:", err)
	}
	const newChecksum = "eeee"
	const newPhysicalAddress = "/addrNew"
	if err := c.CreateEntry(ctx, repository, "master", "/file1", newChecksum, newPhysicalAddress, 2, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
	}
	ent, err := c.GetEntry(ctx, repository, "master", "/file1", true)
	if err != nil {
		t.Fatal("RevertEntry expecting previous file to be found:", err)
	}
	if ent.Checksum != newChecksum {
		t.Errorf("RevertEntry should find previus entry with checksum %s, got %s", newChecksum, ent.Checksum)
	}
	if ent.PhysicalAddress != newPhysicalAddress {
		t.Errorf("RevertEntry should find previus entry with checksum %s, got %s", newPhysicalAddress, ent.Checksum)
	}
}

func TestCataloger_RevertEntry_Committed(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", "/file1", "ff", "/addr1", 1, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1", "tester", nil); err != nil {
		t.Fatal("commit for revert entry test:", err)
	}
	err := c.RevertEntry(ctx, repository, "master", "/file1")
	expectedErr := db.ErrNotFound
	if !errors.As(err, &expectedErr) {
		t.Fatal("RevertEntry expected not to find file in case nothing to revert: ", err)
	}
}

func TestCataloger_RevertEntry_CommittedParentBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", "/file1", "ff", "/addr1", 1, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1", "tester", nil); err != nil {
		t.Fatal("commit for revert entry test:", err)
	}
	_, err := c.CreateBranch(ctx, repository, "b1", "master")
	if err != nil {
		t.Fatal("create branch for revert entry test:", err)
	}
	err = c.RevertEntry(ctx, repository, "b1", "/file1")
	expectedErr := db.ErrNotFound
	if !errors.As(err, &expectedErr) {
		t.Fatal("RevertEntry expected not to find file in case nothing to revert:", err)
	}
}

func TestCataloger_RevertEntry_UncommittedDeleteSameBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", "/file1", "ff", "/addr1", 1, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1", "tester", nil); err != nil {
		t.Fatal("commit for revert entry test:", err)
	}
	err := c.DeleteEntry(ctx, repository, "master", "/file1")
	if err != nil {
		t.Fatal("delete entry for revert entry test:", err)
	}
	err = c.RevertEntry(ctx, repository, "master", "/file1")
	if err != nil {
		t.Fatal("RevertEntry expected successful revert on delete entry:", err)
	}
	ent, err := c.GetEntry(ctx, repository, "master", "/file1", true)
	if err != nil {
		t.Fatal("get entry for revert entry test:", err)
	}
	if ent.Path != "/file1" || ent.PhysicalAddress != "/addr1" {
		t.Fatalf("Entry should be reverted back to /file1 /addr1, got %+v", ent)
	}
}

func TestCataloger_RevertEntry_UncommittedDeleteParentBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", "/file1", "ff", "/addr1", 1, nil); err != nil {
		t.Fatal("create entry for revert entry test:", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1", "tester", nil); err != nil {
		t.Fatal("commit for revert entry test:", err)
	}
	if _, err := c.CreateBranch(ctx, repository, "b1", "master"); err != nil {
		t.Fatal("create branch for revert entry test:", err)
	}
	err := c.DeleteEntry(ctx, repository, "b1", "/file1")
	if err != nil {
		t.Fatal("delete entry for revert entry test:", err)
	}
	err = c.RevertEntry(ctx, repository, "b1", "/file1")
	if err != nil {
		t.Fatal("RevertEntry expected successful revert on delete entry:", err)
	}
	ent, err := c.GetEntry(ctx, repository, "b1", "/file1", true)
	if err != nil {
		t.Fatal("get entry for revert entry test:", err)
	}
	if ent.Path != "/file1" || ent.PhysicalAddress != "/addr1" {
		t.Fatalf("Entry should be reverted back to /file1 /addr1, got %+v", ent)
	}
}
