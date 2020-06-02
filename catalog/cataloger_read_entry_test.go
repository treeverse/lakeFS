package catalog

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ReadEntry(t *testing.T) {
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	c := NewCataloger(cdb)

	// produce test data
	if err := c.CreateRepo(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repo for testing", err)
	}
	if err := c.WriteEntry(ctx, "repo1", "master", "/file1", "ff", "/addr1", 42, true, nil); err != nil {
		t.Fatal("failed to write entry", err)
	}
	if err := c.WriteEntry(ctx, "repo1", "master", "/file2", "ee", "/addr2", 24, false, nil); err != nil {
		t.Fatal("failed to write entry", err)
	}

	isStagedTrue := true
	isStagedFalse := false
	type args struct {
		repo        string
		branch      string
		path        string
		readOptions EntryReadOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *Entry
		wantErr bool
	}{
		{
			name: "read staged - file staged",
			args: args{
				repo:        "repo1",
				branch:      "master",
				path:        "/file1",
				readOptions: EntryReadOptions{EntryState: EntryStateStaged},
			},
			want:    &Entry{Path: "/file1", PhysicalAddress: "/addr1", Size: 42, Checksum: "ff", IsStaged: &isStagedTrue},
			wantErr: false,
		},
		{
			name: "read unstaged - file staged",
			args: args{
				repo:        "repo1",
				branch:      "master",
				path:        "/file1",
				readOptions: EntryReadOptions{EntryState: EntryStateUnstaged},
			},
			want:    &Entry{Path: "/file1", PhysicalAddress: "/addr1", Size: 42, Checksum: "ff", IsStaged: &isStagedTrue},
			wantErr: false,
		},
		{
			name: "read unstaged - file unstaged",
			args: args{
				repo:        "repo1",
				branch:      "master",
				path:        "/file2",
				readOptions: EntryReadOptions{EntryState: EntryStateUnstaged},
			},
			want:    &Entry{Path: "/file2", PhysicalAddress: "/addr2", Size: 24, Checksum: "ee", IsStaged: &isStagedFalse},
			wantErr: false,
		},
		{
			name: "read staged - file unstaged",
			args: args{
				repo:        "repo1",
				branch:      "master",
				path:        "/file2",
				readOptions: EntryReadOptions{EntryState: EntryStateStaged},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "read unknown file",
			args: args{
				repo:        "repo1",
				branch:      "master",
				path:        "/file3",
				readOptions: EntryReadOptions{EntryState: EntryStateUnstaged},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "read unknown repo",
			args: args{
				repo:        "repoX",
				branch:      "master",
				path:        "/file1",
				readOptions: EntryReadOptions{EntryState: EntryStateUnstaged},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "read missing repo",
			args: args{
				repo:        "",
				branch:      "master",
				path:        "/file1",
				readOptions: EntryReadOptions{EntryState: EntryStateUnstaged},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "read missing branch",
			args: args{
				repo:        "repo1",
				branch:      "",
				path:        "/file1",
				readOptions: EntryReadOptions{EntryState: EntryStateUnstaged},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "read missing path",
			args: args{
				repo:        "repo1",
				branch:      "master",
				path:        "",
				readOptions: EntryReadOptions{EntryState: EntryStateUnstaged},
			},
			want:    nil,
			wantErr: true,
		},
		// TODO(barak): when we will have commit - add read committed data
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.ReadEntry(ctx, tt.args.repo, tt.args.branch, tt.args.path, tt.args.readOptions)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if (got == nil && tt.want != nil) || (got != nil && tt.want == nil) {
				t.Errorf("ReadEntry() got = %+v, want = %+v", got, tt.want)
				return
			}
			if tt.want == nil || got == nil {
				return
			}
			// compare just specific fields
			if tt.want.Path != got.Path {
				t.Errorf("ReadEntry() got Path = %v, want = %v", got.Path, tt.want.Path)
			}
			if tt.want.PhysicalAddress != got.PhysicalAddress {
				t.Errorf("ReadEntry() got PhysicalAddress = %v, want = %v", got.PhysicalAddress, tt.want.PhysicalAddress)
			}
			if tt.want.Size != got.Size {
				t.Errorf("ReadEntry() got Size = %v, want = %v", got.Size, tt.want.Size)
			}
			if tt.want.Checksum != got.Checksum {
				t.Errorf("ReadEntry() got Checksum = %v, want = %v", got.Checksum, tt.want.Checksum)
			}
			if tt.want.IsStaged == nil && got.IsStaged != nil {
				t.Error("ReadEntry() got IsStaged non nil, want nil")
			} else if tt.want.IsStaged != nil && got.IsStaged == nil {
				t.Error("ReadEntry() got IsStaged nil, want non nil")
			} else if tt.want.IsStaged != nil && got.IsStaged != nil && *tt.want.IsStaged != *got.IsStaged {
				t.Errorf("ReadEntry() got IsStaged %t, want %t", *tt.want.IsStaged, *got.IsStaged)
			}
		})
	}
}
