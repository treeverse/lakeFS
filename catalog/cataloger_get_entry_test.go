package catalog

import (
	"context"
	"testing"
)

func TestCataloger_GetEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := setupReadEntryData(t, ctx, c)

	type args struct {
		repository      string
		branch          string
		path            string
		readUncommitted bool
	}
	tests := []struct {
		name    string
		args    args
		want    *Entry
		wantErr bool
	}{
		{
			name:    "uncommitted - uncommitted file",
			args:    args{repository: repository, branch: "master", path: "/file3", readUncommitted: true},
			want:    &Entry{Path: "/file3", PhysicalAddress: "/addr3", Size: 42, Checksum: "ffff"},
			wantErr: false,
		},
		{
			name:    "uncommitted - committed file",
			args:    args{repository: repository, branch: "master", path: "/file1", readUncommitted: true},
			want:    &Entry{Path: "/file1", PhysicalAddress: "/addr1", Size: 42, Checksum: "ff"},
			wantErr: false,
		},
		{
			name:    "committed - committed file",
			args:    args{repository: repository, branch: "master", path: "/file2", readUncommitted: false},
			want:    &Entry{Path: "/file2", PhysicalAddress: "/addr2", Size: 24, Checksum: "ee"},
			wantErr: false,
		},
		{
			name:    "uncommitted - unknown file",
			args:    args{repository: repository, branch: "master", path: "/fileX", readUncommitted: true},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "committed - unknown file",
			args:    args{repository: repository, branch: "master", path: "/fileX", readUncommitted: false},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "unknown repository",
			args:    args{repository: "repoX", branch: "master", path: "/file1", readUncommitted: true},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing repository",
			args:    args{repository: "", branch: "master", path: "/file1", readUncommitted: true},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing branch",
			args:    args{repository: repository, branch: "", path: "/file1", readUncommitted: true},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing path",
			args:    args{repository: repository, branch: "master", path: "", readUncommitted: true},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetEntry(ctx, tt.args.repository, tt.args.branch, tt.args.path, tt.args.readUncommitted)
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
