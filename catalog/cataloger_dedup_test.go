package catalog

import (
	"context"
	"testing"
)

func TestCataloger_Dedup(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// setup test data
	if err := c.CreateRepository(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repository for testing failed", err)
	}
	_, _ = c.Dedup(ctx, "repo1", "dede", "/file9")

	type args struct {
		repository      string
		dedupID         string
		physicalAddress string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "new", args: args{repository: "repo1", dedupID: "de", physicalAddress: "/file1"}, want: "/file1", wantErr: false},
		{name: "existing same name", args: args{repository: "repo1", dedupID: "dede", physicalAddress: "/file9"}, want: "/file9", wantErr: false},
		{name: "existing diff name", args: args{repository: "repo1", dedupID: "dede", physicalAddress: "/fileX"}, want: "/file9", wantErr: false},
		{name: "invalid id", args: args{repository: "repo1", dedupID: "jj", physicalAddress: "/file2"}, want: "/file2", wantErr: true},
		{name: "unknown repository", args: args{repository: "repo2", dedupID: "dedede", physicalAddress: "/file3"}, want: "", wantErr: true},
		{name: "invalid address", args: args{repository: "repo1", dedupID: "dededede", physicalAddress: ""}, want: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Dedup(ctx, tt.args.repository, tt.args.dedupID, tt.args.physicalAddress)
			if (err != nil) != tt.wantErr {
				t.Errorf("cataloger.Dedup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("cataloger.Dedup() = %v, want %v", got, tt.want)
			}
		})
	}
}
