package catalog

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
)

func TestCataloger_ListCommits(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	setupListCommitsByBranchData(t, ctx, c, repository, "master")

	type args struct {
		repository   string
		branch       string
		fromCommitID int
		limit        int
	}
	tests := []struct {
		name     string
		args     args
		want     []*CommitLog
		wantMore bool
		wantErr  bool
	}{
		{
			name: "all",
			args: args{
				repository:   repository,
				branch:       "master",
				fromCommitID: 0,
				limit:        -1,
			},
			want: []*CommitLog{
				{Branch: "master", CommitID: 1, Committer: "tester", Message: "commit1", Metadata: Metadata{}},
				{Branch: "master", CommitID: 2, Committer: "tester", Message: "commit2", Metadata: Metadata{}},
				{Branch: "master", CommitID: 3, Committer: "tester", Message: "commit3", Metadata: Metadata{}},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "just 2",
			args: args{
				repository:   repository,
				branch:       "master",
				fromCommitID: 0,
				limit:        2,
			},
			want: []*CommitLog{
				{Branch: "master", CommitID: 1, Committer: "tester", Message: "commit1", Metadata: Metadata{}},
				{Branch: "master", CommitID: 2, Committer: "tester", Message: "commit2", Metadata: Metadata{}},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "last 1",
			args: args{
				repository:   repository,
				branch:       "master",
				fromCommitID: 2,
				limit:        1,
			},
			want: []*CommitLog{
				{Branch: "master", CommitID: 3, Committer: "tester", Message: "commit3", Metadata: Metadata{}},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "center",
			args: args{
				repository:   repository,
				branch:       "master",
				fromCommitID: 1,
				limit:        1,
			},
			want: []*CommitLog{
				{Branch: "master", CommitID: 2, Committer: "tester", Message: "commit2", Metadata: Metadata{}},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "unknown repository",
			args: args{
				repository:   "no_repo",
				branch:       "master",
				fromCommitID: 0,
				limit:        -1,
			},
			want:     nil,
			wantMore: false,
			wantErr:  true,
		},
		{
			name: "no repository",
			args: args{
				repository:   "",
				branch:       "master",
				fromCommitID: 0,
				limit:        -1,
			},
			want:     nil,
			wantMore: false,
			wantErr:  true,
		},
		{
			name: "no branch",
			args: args{
				repository:   repository,
				branch:       "",
				fromCommitID: 0,
				limit:        -1,
			},
			want:     nil,
			wantMore: false,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListCommits(ctx, tt.args.repository, tt.args.branch, tt.args.fromCommitID, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListCommits() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// hack - remove the timestamp in order to compare everything except the time
			// consider create entry will control creation time
			for i := range got {
				got[i].CreationDate = time.Time{}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListCommits() got = %s, want = %s", spew.Sdump(got), spew.Sdump(tt.want))
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListCommits() gotMore = %v, want = %v", gotMore, tt.wantMore)
			}
		})
	}
}

func setupListCommitsByBranchData(t *testing.T, ctx context.Context, c Cataloger, repository, branch string) {
	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("/file%d", i)
		fileAddr := fmt.Sprintf("/addr%d", i)
		if err := c.CreateEntry(ctx, repository, branch, fileName, strings.Repeat("ff", i), fileAddr, i+1, nil); err != nil {
			t.Fatal("Write entry for list repository commits failed", err)
		}
		message := "commit" + strconv.Itoa(i+1)
		_, err := c.Commit(ctx, repository, branch, message, "tester", nil)
		if err != nil {
			t.Fatalf("Commit for list repository commits failed '%s': %s", message, err)
		}
	}
}
