package mvcc

import (
	"context"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_GetBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// setup test data
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	_, err := c.CreateBranch(ctx, repo, "branch1", "master")
	testutil.MustDo(t, "create branch1", err)
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "a/file", nil, "")
	_, err = c.Commit(ctx, repo, "master", "commit a file", "tester", nil)
	testutil.MustDo(t, "commit a file", err)

	type args struct {
		repository string
		branch     string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name:    "branch with commits",
			args:    args{repository: repo, branch: "master"},
			want:    "~KJ8Wd1Rs96b",
			wantErr: false,
		},
		{
			name:    "branch without commits",
			args:    args{repository: repo, branch: "branch1"},
			want:    "~2FgRfNLHmSmvS",
			wantErr: false,
		},
		{
			name:    "unknown repository",
			args:    args{repository: "repoX", branch: "master"},
			want:    "",
			wantErr: true,
		},
		{
			name:    "missing repository",
			args:    args{repository: "", branch: "master"},
			want:    "",
			wantErr: true,
		},
		{
			name:    "unknown branch",
			args:    args{repository: repo, branch: "nobranch"},
			want:    "",
			wantErr: true,
		},
		{
			name:    "missing branch",
			args:    args{repository: repo, branch: ""},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetBranchReference(ctx, tt.args.repository, tt.args.branch)
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
