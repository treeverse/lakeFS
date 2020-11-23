package mvcc

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ListRepositories(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// create test data
	for i := 3; i > 0; i-- {
		repoName := fmt.Sprintf("repo%d", i)
		storage := fmt.Sprintf("s3://bucket%d", i)
		_, err := c.CreateRepository(ctx, repoName, storage, "master")
		testutil.MustDo(t, "create repository "+repoName, err)
	}

	type args struct {
		limit int
		after string
	}
	tests := []struct {
		name     string
		args     args
		want     []string
		wantMore bool
		wantErr  bool
	}{
		{
			name:     "basic",
			args:     args{limit: -1, after: ""},
			want:     []string{"repo1", "repo2", "repo3"},
			wantMore: false,
			wantErr:  false,
		},
		{
			name:     "small amount",
			args:     args{limit: 1, after: ""},
			want:     []string{"repo1"},
			wantMore: true,
			wantErr:  false,
		},
		{
			name:     "the rest",
			args:     args{limit: 10, after: "repo2"},
			want:     []string{"repo3"},
			wantMore: false,
			wantErr:  false,
		},
		{
			name:     "nothing to be found",
			args:     args{limit: 0, after: "repoX"},
			want:     nil,
			wantMore: false,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListRepositories(ctx, tt.args.limit, tt.args.after)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListRepositories() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			var names []string
			for _, repository := range got {
				names = append(names, repository.Name)
			}
			if !reflect.DeepEqual(tt.want, names) {
				t.Errorf("ListRepositories() got repos = %v, want %v", names, tt.want)
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListRepositories() got more = %v, want %v", gotMore, tt.wantMore)
			}
		})
	}
}
