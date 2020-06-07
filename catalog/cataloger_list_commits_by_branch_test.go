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

func TestCataloger_ListCommitsByBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	setupListCommitsByBranchData(t, ctx, c, repo, "master")

	type args struct {
		repo         string
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
				repo:         repo,
				branch:       "master",
				fromCommitID: 0,
				limit:        -1,
			},
			want: []*CommitLog{
				{Branch: "master", CommitID: 1, Committer: "tester", Message: "commit1", Metadata: nil},
				{Branch: "master", CommitID: 2, Committer: "tester", Message: "commit2", Metadata: nil},
				{Branch: "master", CommitID: 3, Committer: "tester", Message: "commit3", Metadata: nil},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "just 2",
			args: args{
				repo:         repo,
				branch:       "master",
				fromCommitID: 0,
				limit:        2,
			},
			want: []*CommitLog{
				{Branch: "master", CommitID: 1, Committer: "tester", Message: "commit1", Metadata: nil},
				{Branch: "master", CommitID: 2, Committer: "tester", Message: "commit2", Metadata: nil},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "last 1",
			args: args{
				repo:         repo,
				branch:       "master",
				fromCommitID: 2,
				limit:        1,
			},
			want: []*CommitLog{
				{Branch: "master", CommitID: 3, Committer: "tester", Message: "commit3", Metadata: nil},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "center",
			args: args{
				repo:         repo,
				branch:       "master",
				fromCommitID: 1,
				limit:        1,
			},
			want: []*CommitLog{
				{Branch: "master", CommitID: 2, Committer: "tester", Message: "commit2", Metadata: nil},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "unknown repo",
			args: args{
				repo:         "no_repo",
				branch:       "master",
				fromCommitID: 0,
				limit:        -1,
			},
			want:     nil,
			wantMore: false,
			wantErr:  true,
		},
		{
			name: "no repo",
			args: args{
				repo:         "",
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
				repo:         repo,
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
			got, gotMore, err := c.ListCommitsByBranch(ctx, tt.args.repo, tt.args.branch, tt.args.fromCommitID, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListCommitsByBranch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// hack - remove the timestamp in order to compare everything except the time
			// consider write entry will control creation time
			for i := range got {
				got[i].CreationDate = time.Time{}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListCommitsByBranch() got = %s, want = %s", spew.Sdump(got), spew.Sdump(tt.want))
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListCommitsByBranch() gotMore = %v, want = %v", gotMore, tt.wantMore)
			}
		})
	}
}

func setupListCommitsByBranchData(t *testing.T, ctx context.Context, c Cataloger, repo string, branch string) {
	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("/file%d", i)
		fileAddr := fmt.Sprintf("/addr%d", i)
		if err := c.CreateEntry(ctx, repo, branch, fileName, strings.Repeat("ff", i), fileAddr, i+1, nil); err != nil {
			t.Fatal("Write entry for list repo commits failed", err)
		}
		message := "commit" + strconv.Itoa(i+1)
		_, err := c.Commit(ctx, repo, branch, message, "tester", nil)
		if err != nil {
			t.Fatalf("Commit for list repo commits failed '%s': %s", message, err)
		}
	}
}
