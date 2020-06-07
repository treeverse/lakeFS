package catalog

import (
	"context"
	"reflect"
	"testing"
)

func TestCataloger_GetBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// setup test data
	if err := c.CreateRepo(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repo for testing failed", err)
	}

	type args struct {
		repo   string
		branch string
	}
	tests := []struct {
		name    string
		args    args
		want    *Branch
		wantErr bool
	}{
		{
			name:    "existing",
			args:    args{repo: "repo1", branch: "master"},
			want:    &Branch{RepositoryID: 1, ID: 1, Name: "master", NextCommit: 1},
			wantErr: false,
		},
		{
			name:    "just repo",
			args:    args{repo: "repo1", branch: "shujin"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "no repo",
			args:    args{repo: "repoX", branch: "master"},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetBranch(ctx, tt.args.repo, tt.args.branch)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBranch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetBranch() got = %v, want %v", got, tt.want)
			}
		})
	}
}
