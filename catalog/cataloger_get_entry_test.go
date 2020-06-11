package catalog

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_GetEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := setupReadEntryData(t, ctx, c)

	type args struct {
		repository string
		branch     string
		commitID   CommitID
		path       string
	}
	tests := []struct {
		name    string
		args    args
		want    *Entry
		wantErr bool
	}{
		{
			name:    "uncommitted - uncommitted file",
			args:    args{repository: repository, branch: "master", path: "/file3", commitID: UncommittedID},
			want:    &Entry{Path: "/file3", PhysicalAddress: "/addr3", Size: 42, Checksum: "ffff"},
			wantErr: false,
		},
		{
			name:    "uncommitted - committed file",
			args:    args{repository: repository, branch: "master", path: "/file1", commitID: UncommittedID},
			want:    &Entry{Path: "/file1", PhysicalAddress: "/addr1", Size: 42, Checksum: "ff"},
			wantErr: false,
		},
		{
			name:    "committed - committed file",
			args:    args{repository: repository, branch: "master", path: "/file2", commitID: CommittedID},
			want:    &Entry{Path: "/file2", PhysicalAddress: "/addr2", Size: 24, Checksum: "ee"},
			wantErr: false,
		},
		{
			name:    "uncommitted - unknown file",
			args:    args{repository: repository, branch: "master", path: "/fileX", commitID: UncommittedID},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "committed - unknown file",
			args:    args{repository: repository, branch: "master", path: "/fileX", commitID: CommittedID},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "unknown repository",
			args:    args{repository: "repoX", branch: "master", path: "/file1", commitID: UncommittedID},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing repository",
			args:    args{repository: "", branch: "master", path: "/file1", commitID: UncommittedID},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing branch",
			args:    args{repository: repository, branch: "", path: "/file1", commitID: UncommittedID},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing path",
			args:    args{repository: repository, branch: "master", path: "", commitID: UncommittedID},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetEntry(ctx, tt.args.repository, tt.args.branch, tt.args.commitID, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if (got == nil && tt.want != nil) || (got != nil && tt.want == nil) {
				t.Errorf("GetEntry() got = %+v, want = %+v", got, tt.want)
				return
			}
			if tt.want == nil || got == nil {
				return
			}
			// compare just specific fields
			if tt.want.Path != got.Path {
				t.Errorf("GetEntry() got Path = %v, want = %v", got.Path, tt.want.Path)
			}
			if tt.want.PhysicalAddress != got.PhysicalAddress {
				t.Errorf("GetEntry() got PhysicalAddress = %v, want = %v", got.PhysicalAddress, tt.want.PhysicalAddress)
			}
			if tt.want.Size != got.Size {
				t.Errorf("GetEntry() got Size = %v, want = %v", got.Size, tt.want.Size)
			}
			if tt.want.Checksum != got.Checksum {
				t.Errorf("GetEntry() got Checksum = %v, want = %v", got.Checksum, tt.want.Checksum)
			}
		})
	}
}

func TestCataloger_GetEntry_ByCommitSameBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := setupReadEntryData(t, ctx, c)

	// create the following commits on /file11 - new, change, del, new
	const fileName = "/file11"
	commit := make([]CommitID, 4)
	for i := 0; i < 4; i++ {
		id := i + 1
		fileChecksum := "fff" + strconv.Itoa(id)
		fileAddr := "/addr/" + strconv.Itoa(id)
		fileSize := 42 + id
		if id == 3 {
			testutil.MustDo(t, "delete "+fileName,
				c.DeleteEntry(ctx, repository, "master", fileName))
		} else {
			testutil.MustDo(t, "create "+fileName,
				c.CreateEntry(ctx, repository, "master", fileName, fileChecksum, fileAddr, fileSize, nil))
		}
		commitMsg := "commit " + fileName + " for the " + strconv.Itoa(id+1) + " time"
		commitID, err := c.Commit(ctx, repository, "master", commitMsg, "tester", nil)
		testutil.MustDo(t, commitMsg, err)
		commit[i] = commitID
	}

	// verify the above
	for i := 0; i < 4; i++ {
		id := i + 1
		commitID := commit[i]
		ent, err := c.GetEntry(ctx, repository, "master", commitID, fileName)
		if id == 3 {
			// check file not found
			if !errors.As(err, &db.ErrNotFound) {
				t.Errorf("On commit %d the entry is not expected to be found, err=%s", commitID, err)
			}
		} else {
			// check file exists with the right address
			if err != nil {
				t.Fatalf("On commit %d the entry is expected to be found, err=%s", commitID, err)
			}
			expectedAddr := "/addr/" + strconv.Itoa(id)
			if ent.PhysicalAddress != expectedAddr {
				t.Errorf("On commit %d the physical address = %s, expected %s", commitID, ent.PhysicalAddress, expectedAddr)
			}
		}
	}
}

func TestCataloger_GetEntry_ByCommitParentBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := setupReadEntryData(t, ctx, c)

	// create the following commits on /file11 - new, change, <branch>, del, new
	const fileName = "/file12"
	commit := make([]CommitID, 4)
	branch := "master"
	for i := 0; i < 4; i++ {
		id := i + 1
		fileChecksum := "fff" + strconv.Itoa(id)
		fileAddr := "/addr/" + strconv.Itoa(id)
		fileSize := 42 + id
		if id == 3 {
			branch = "b1"
			_, err := c.CreateBranch(ctx, repository, branch, "master")
			testutil.MustDo(t, "create branch b1", err)
			testutil.MustDo(t, "delete "+fileName,
				c.DeleteEntry(ctx, repository, branch, fileName))
		} else {
			testutil.MustDo(t, "create "+fileName,
				c.CreateEntry(ctx, repository, branch, fileName, fileChecksum, fileAddr, fileSize, nil))
		}

		commitMsg := "commit " + fileName + " for the " + strconv.Itoa(id+1) + " time"
		commitID, err := c.Commit(ctx, repository, branch, commitMsg, "tester", nil)
		testutil.MustDo(t, commitMsg, err)
		commit[i] = commitID
	}

	// verify the above
	branch = "master"
	for i := 0; i < 4; i++ {
		id := i + 1
		commitID := commit[i]
		if id == 3 {
			branch = "b1"
		}
		ent, err := c.GetEntry(ctx, repository, branch, commitID, fileName)
		if id == 3 {
			// check file not found
			if !errors.As(err, &db.ErrNotFound) {
				t.Errorf("On commit %d the entry is not expected to be found, err=%s", commitID, err)
			}
		} else {
			// check file exists with the right address
			if err != nil {
				t.Fatalf("On commit %d the entry is expected to be found, err=%s", commitID, err)
			}
			expectedAddr := "/addr/" + strconv.Itoa(id)
			if ent.PhysicalAddress != expectedAddr {
				t.Errorf("On commit %d the physical address = %s, expected %s", commitID, ent.PhysicalAddress, expectedAddr)
			}
		}
	}
}

func setupReadEntryData(t *testing.T, ctx context.Context, c Cataloger) string {
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", "/file1", "ff", "/addr1", 42, nil); err != nil {
		t.Fatal("failed to create entry", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", "/file2", "ee", "/addr2", 24, nil); err != nil {
		t.Fatal("failed to create entry", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1 and 2", "tester", nil); err != nil {
		t.Fatal("failed to commit for get entry:", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", "/file3", "ffff", "/addr3", 42, nil); err != nil {
		t.Fatal("failed to create entry", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", "/file4", "eeee", "/addr4", 24, nil); err != nil {
		t.Fatal("failed to create entry", err)
	}
	return repository
}
