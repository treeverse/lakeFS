package mvcc

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/catalog"
)

func TestCataloger_GetEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := setupReadEntryData(t, ctx, c)

	type args struct {
		repository string
		reference  string
		path       string
	}
	tests := []struct {
		name    string
		args    args
		want    *catalog.Entry
		wantErr bool
	}{
		{
			name:    "uncommitted - uncommitted file",
			args:    args{repository: repository, reference: "master", path: "/file3"},
			want:    &catalog.Entry{Path: "/file3", PhysicalAddress: "/addr3", Size: 42, Checksum: "ffff"},
			wantErr: false,
		},
		{
			name:    "uncommitted - committed file",
			args:    args{repository: repository, reference: "master", path: "/file1"},
			want:    &catalog.Entry{Path: "/file1", PhysicalAddress: "/addr1", Size: 42, Checksum: "ff"},
			wantErr: false,
		},
		{
			name:    "committed - committed file",
			args:    args{repository: repository, reference: "master:HEAD", path: "/file2"},
			want:    &catalog.Entry{Path: "/file2", PhysicalAddress: "/addr2", Size: 24, Checksum: "ee"},
			wantErr: false,
		},
		{
			name:    "uncommitted - unknown file",
			args:    args{repository: repository, reference: "master", path: "/fileX"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "committed - unknown file",
			args:    args{repository: repository, reference: "master:HEAD", path: "/fileX"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "unknown repository",
			args:    args{repository: "repoX", reference: "master", path: "/file1"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing repository",
			args:    args{repository: "", reference: "master", path: "/file1"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty reference",
			args:    args{repository: repository, reference: "", path: "/file1"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing path",
			args:    args{repository: repository, reference: "master:HEAD", path: ""},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetEntry(ctx, tt.args.repository, tt.args.reference, tt.args.path, catalog.GetEntryParams{})
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if (got == nil && tt.want != nil) || (got != nil && tt.want == nil) {
				t.Errorf("Get() got = %+v, want = %+v", got, tt.want)
				return
			}
			if tt.want == nil || got == nil {
				return
			}
			// compare just specific fields
			if tt.want.Path != got.Path {
				t.Errorf("Get() got Key = %v, want = %v", got.Path, tt.want.Path)
			}
			if tt.want.PhysicalAddress != got.PhysicalAddress {
				t.Errorf("Get() got PhysicalAddress = %v, want = %v", got.PhysicalAddress, tt.want.PhysicalAddress)
			}
			if tt.want.Size != got.Size {
				t.Errorf("Get() got Size = %v, want = %v", got.Size, tt.want.Size)
			}
			if tt.want.Checksum != got.Checksum {
				t.Errorf("Get() got Checksum = %v, want = %v", got.Checksum, tt.want.Checksum)
			}
		})
	}
}

func setupReadEntryData(t *testing.T, ctx context.Context, c catalog.Cataloger) string {
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file1",
		Checksum:        "ff",
		PhysicalAddress: "/addr1",
		Size:            42,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("failed to create entry", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file2",
		Checksum:        "ee",
		PhysicalAddress: "/addr2",
		Size:            24,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("failed to create entry", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "commit file1 and 2", "tester", nil); err != nil {
		t.Fatal("failed to commit for get entry:", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file3",
		Checksum:        "ffff",
		PhysicalAddress: "/addr3",
		Size:            42,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("failed to create entry", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file4",
		Checksum:        "eeee",
		PhysicalAddress: "/addr4",
		Size:            24,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("failed to create entry", err)
	}
	return repository
}
