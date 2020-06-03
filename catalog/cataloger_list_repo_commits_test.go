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

func TestCataloger_ListRepoCommits(t *testing.T) {
	ctx := context.Background()
	c := setupCatalogerForTesting(t)
	repo := setupCatalogerRepo(t, ctx, c, "repo", "master")
	setupListRepoCommitsData(t, ctx, c, repo, "master")

	type args struct {
		repo         string
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
			args: args{repo: repo, fromCommitID: 0, limit: -1},
			want: []*CommitLog{
				{Branch: "master", CommitID: 1, Committer: "tester", Message: "commit1", Metadata: nil},
				{Branch: "master", CommitID: 2, Committer: "tester", Message: "commit2", Metadata: nil},
				{Branch: "master", CommitID: 3, Committer: "tester", Message: "commit3", Metadata: nil},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "just one",
			args: args{repo: repo, fromCommitID: 0, limit: 1},
			want: []*CommitLog{
				{Branch: "master", CommitID: 1, Committer: "tester", Message: "commit1", Metadata: nil},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "last one",
			args: args{repo: repo, fromCommitID: 2, limit: 1},
			want: []*CommitLog{
				{Branch: "master", CommitID: 3, Committer: "tester", Message: "commit3", Metadata: nil},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name:     "no commit like that",
			args:     args{repo: repo, fromCommitID: 665, limit: 1},
			want:     nil,
			wantMore: false,
			wantErr:  false,
		},
		{
			name:     "repo not exists",
			args:     args{repo: "no_repo", fromCommitID: 0, limit: -1},
			want:     nil,
			wantMore: false,
			wantErr:  true,
		},
		{
			name:     "missing repo",
			args:     args{repo: "", fromCommitID: 0, limit: -1},
			want:     nil,
			wantMore: false,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCommits, gotMore, err := c.ListRepoCommits(ctx, tt.args.repo, tt.args.fromCommitID, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListRepoCommits() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// hack - remove the timestamp in order to compare everything except the time
			// consider write entry will control creation time
			for i := range gotCommits {
				gotCommits[i].CreationDate = time.Time{}
			}
			if !reflect.DeepEqual(gotCommits, tt.want) {
				t.Errorf("ListRepoCommits() got = %s, want = %s", spew.Sdump(gotCommits), spew.Sdump(tt.want))
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListRepoCommits() gotMore = %v, wantMore = %v", gotMore, tt.wantMore)
			}
		})
	}
}

func setupListRepoCommitsData(t *testing.T, ctx context.Context, c Cataloger, repo string, branch string) {
	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("/file%d", i)
		fileAddr := fmt.Sprintf("/addr%d", i)
		if err := c.WriteEntry(ctx, repo, branch, fileName, strings.Repeat("ff", i), fileAddr, i+1, nil); err != nil {
			t.Fatal("Write entry for list repo commits failed", err)
		}
		message := "commit" + strconv.Itoa(i+1)
		_, err := c.Commit(ctx, repo, branch, message, "tester", nil)
		if err != nil {
			t.Fatalf("Commit for list repo commits failed '%s': %s", message, err)
		}
	}
}
