package catalog

import (
	"context"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_CreateEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	// test data
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	testutil.MustDo(t, "create entry on master for testing",
		c.CreateEntry(ctx, repo, "master", "/aaa/bbb/ddd", "cc", "xx", 1, nil))
	_, err := c.CreateBranch(ctx, repo, "b1", "master")
	testutil.MustDo(t, "create branch b1 based on master", err)

	type args struct {
		repository      string
		branch          string
		path            string
		checksum        string
		physicalAddress string
		size            int
		metadata        Metadata
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "new",
			args: args{
				repository:      repo,
				branch:          "master",
				path:            "/aaa/bbb/ccc",
				checksum:        "1234",
				physicalAddress: "5678",
				size:            100,
				metadata:        Metadata{"k": "v"},
			},
			wantErr: false,
		},
		{
			name: "new on branch",
			args: args{
				repository:      repo,
				branch:          "b1",
				path:            "/aaa/bbb/ccc",
				checksum:        "1234",
				physicalAddress: "5678",
				size:            100,
				metadata:        Metadata{"k": "v"},
			},
			wantErr: false,
		},
		{
			name: "existing",
			args: args{
				repository:      repo,
				branch:          "master",
				path:            "/aaa/bbb/ddd",
				checksum:        "1234",
				physicalAddress: "5678",
				size:            100,
				metadata:        Metadata{"kk": "vv"},
			},
			wantErr: false,
		},
		{
			name: "unknown repo",
			args: args{
				repository:      "norepo",
				branch:          "master",
				path:            "/aaa/bbb/ccc",
				checksum:        "1234",
				physicalAddress: "5678",
				size:            100,
				metadata:        Metadata{"k": "v"},
			},
			wantErr: true,
		},
		{
			name: "unknown branch",
			args: args{
				repository:      repo,
				branch:          "masterX",
				path:            "/aaa/bbb/ccc",
				checksum:        "1234",
				physicalAddress: "5678",
				size:            100,
				metadata:        Metadata{"k": "v"},
			},
			wantErr: true,
		},
		{
			name: "missing repo",
			args: args{
				repository:      "",
				branch:          "master",
				path:            "/aaa/bbb/ccc",
				checksum:        "1234",
				physicalAddress: "5678",
				size:            100,
				metadata:        Metadata{"k": "v"},
			},
			wantErr: true,
		},
		{
			name: "missing branch",
			args: args{
				repository:      repo,
				branch:          "",
				path:            "/aaa/bbb/ccc",
				checksum:        "1234",
				physicalAddress: "5678",
				size:            100,
				metadata:        Metadata{"k": "v"},
			},
			wantErr: true,
		},
		{
			name: "missing path",
			args: args{
				repository:      repo,
				branch:          "master",
				path:            "",
				checksum:        "1234",
				physicalAddress: "5678",
				size:            100,
				metadata:        Metadata{"k": "v"},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.CreateEntry(ctx, tt.args.repository, tt.args.branch, tt.args.path, tt.args.checksum,
				tt.args.physicalAddress, tt.args.size, tt.args.metadata)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			// in case there is no error - get the entry and compare
			ent, err := c.GetEntry(ctx, tt.args.repository, tt.args.branch, tt.args.path, true)
			testutil.MustDo(t, "get entry we just created", err)
			if ent.Size != int64(tt.args.size) {
				t.Fatalf("entry size %d, expected %d", ent.Size, tt.args.size)
			}
			if ent.PhysicalAddress != tt.args.physicalAddress {
				t.Fatalf("entry physical address %s, expected %s", ent.PhysicalAddress, tt.args.physicalAddress)
			}
			if ent.Checksum != tt.args.checksum {
				t.Fatalf("entry checksum %s, expected %s", ent.Checksum, tt.args.checksum)
			}
			if !reflect.DeepEqual(ent.Metadata, tt.args.metadata) {
				t.Fatalf("entry metadata %+v, expected %+v", ent.Metadata, tt.args.metadata)
			}
		})
	}
}
