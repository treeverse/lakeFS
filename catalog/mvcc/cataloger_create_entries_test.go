package mvcc

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_CreateEntries(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	// test data
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	testutil.MustDo(t, "create entry on master for testing",
		c.CreateEntry(ctx, repo, "master",
			catalog.Entry{Path: "/aaa/bbb/ddd", Checksum: "cc", PhysicalAddress: "xx", Size: 1},
			catalog.CreateEntryParams{}))
	_, err := c.CreateBranch(ctx, repo, "b1", "master")
	testutil.MustDo(t, "create branch b1 based on master", err)

	type args struct {
		repository string
		branch     string
		entries    []catalog.Entry
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "basic",
			args: args{
				repository: repo,
				branch:     "master",
				entries: []catalog.Entry{
					{
						Path:            "/aaa/bbb/ccc1",
						Checksum:        "1231",
						PhysicalAddress: "5671",
						Size:            100,
						Metadata:        catalog.Metadata{"k1": "v1"},
					},
					{
						Path:            "/aaa/bbb/ccc2",
						Checksum:        "1232",
						PhysicalAddress: "5672",
						Size:            200,
						Metadata:        catalog.Metadata{"k2": "v2"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "unknown repo",
			args: args{
				repository: "norepo",
				branch:     "master",
				entries: []catalog.Entry{
					{
						Path:            "/aaa/bbb/cccX",
						Checksum:        "1239",
						PhysicalAddress: "5679",
						Size:            999,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "unknown branch",
			args: args{
				repository: repo,
				branch:     "masterX",
				entries: []catalog.Entry{
					{
						Path:            "/aaa/bbb/cccX",
						Checksum:        "1239",
						PhysicalAddress: "5679",
						Size:            999,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "missing repo",
			args: args{
				repository: "",
				branch:     "master",
				entries: []catalog.Entry{
					{
						Path:            "/aaa/bbb/cccX",
						Checksum:        "1239",
						PhysicalAddress: "5679",
						Size:            999,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "missing branch",
			args: args{
				repository: repo,
				branch:     "",
				entries: []catalog.Entry{
					{
						Path:            "/aaa/bbb/cccX",
						Checksum:        "1239",
						PhysicalAddress: "5679",
						Size:            999,
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.CreateEntries(ctx, tt.args.repository, tt.args.branch, tt.args.entries)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CreateEntries() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			// check if case there was no error
			for i, entry := range tt.args.entries {
				ent, err := c.GetEntry(ctx, repo, tt.args.branch, entry.Path, catalog.GetEntryParams{})
				testutil.MustDo(t, "get entry for new created entry", err)
				if ent.Path != entry.Path {
					t.Errorf("Entry at pos %d: path '%s', expected '%s'", i, ent.Path, entry.Path)
				}
				if ent.PhysicalAddress != entry.PhysicalAddress {
					t.Errorf("Entry at pos %d: address '%s', expected '%s'", i, ent.PhysicalAddress, entry.PhysicalAddress)
				}
				if ent.Size != entry.Size {
					t.Errorf("Entry at pos %d: size '%d', expected '%d'", i, ent.Size, entry.Size)
				}
			}
		})
	}
}
