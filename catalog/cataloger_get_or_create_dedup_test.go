package catalog

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_GetOrCreateDedup(t *testing.T) {
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	c := NewCataloger(cdb)

	// setup test data
	if err := c.CreateRepo(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repo for testing failed", err)
	}
	_, _ = c.GetOrCreateDedup(ctx, "repo1", "dede", "/file9")

	type args struct {
		repo            string
		dedupID         string
		physicalAddress string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "new", args: args{repo: "repo1", dedupID: "de", physicalAddress: "/file1"}, want: "/file1", wantErr: false},
		{name: "existing same name", args: args{repo: "repo1", dedupID: "dede", physicalAddress: "/file9"}, want: "/file9", wantErr: false},
		{name: "existing diff name", args: args{repo: "repo1", dedupID: "dede", physicalAddress: "/fileX"}, want: "/file9", wantErr: false},
		{name: "invalid id", args: args{repo: "repo1", dedupID: "jj", physicalAddress: "/file2"}, want: "/file2", wantErr: true},
		{name: "unknown repo", args: args{repo: "repo2", dedupID: "dedede", physicalAddress: "/file3"}, want: "", wantErr: true},
		{name: "invalid address", args: args{repo: "repo1", dedupID: "dededede", physicalAddress: ""}, want: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetOrCreateDedup(ctx, tt.args.repo, tt.args.dedupID, tt.args.physicalAddress)
			if (err != nil) != tt.wantErr {
				t.Errorf("cataloger.GetOrCreateDedup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("cataloger.GetOrCreateDedup() = %v, want %v", got, tt.want)
			}
		})
	}
}
