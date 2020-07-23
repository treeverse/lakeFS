package catalog

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/treeverse/lakefs/testutil"

	"github.com/davecgh/go-spew/spew"
)

func TestCataloger_ListCommits(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	initialCommitReference, err := c.GetBranchReference(ctx, repository, "master")
	testutil.MustDo(t, "get master branch reference", err)

	commits := setupListCommitsByBranchData(t, ctx, c, repository, "master")

	type args struct {
		repository    string
		branch        string
		fromReference string
		limit         int
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
				repository:    repository,
				branch:        "master",
				fromReference: "",
				limit:         -1,
			},
			want: []*CommitLog{
				{Reference: commits[2].Reference, Committer: "tester", Message: "commit3", Metadata: Metadata{}},
				{Reference: commits[1].Reference, Committer: "tester", Message: "commit2", Metadata: Metadata{}},
				{Reference: commits[0].Reference, Committer: "tester", Message: "commit1", Metadata: Metadata{}},
				{Reference: initialCommitReference, Committer: CatalogerCommitter, Message: createRepositoryCommitMessage, Metadata: Metadata{}},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "just 2",
			args: args{
				repository:    repository,
				branch:        "master",
				fromReference: "",
				limit:         2,
			},
			want: []*CommitLog{
				{Reference: commits[2].Reference, Committer: "tester", Message: "commit3", Metadata: Metadata{}},
				{Reference: commits[1].Reference, Committer: "tester", Message: "commit2", Metadata: Metadata{}},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "get last commit",
			args: args{
				repository:    repository,
				branch:        "master",
				fromReference: commits[0].Reference,
				limit:         1,
			},
			want: []*CommitLog{
				{Reference: initialCommitReference, Committer: CatalogerCommitter, Message: createRepositoryCommitMessage, Metadata: Metadata{}},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "center",
			args: args{
				repository:    repository,
				branch:        "master",
				fromReference: commits[2].Reference,
				limit:         1,
			},
			want: []*CommitLog{
				{Reference: commits[1].Reference, Committer: "tester", Message: "commit2", Metadata: Metadata{}},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "unknown repository",
			args: args{
				repository:    "no_repo",
				branch:        "master",
				fromReference: "",
				limit:         -1,
			},
			want:     nil,
			wantMore: false,
			wantErr:  true,
		},
		{
			name: "no repository",
			args: args{
				repository:    "",
				branch:        "master",
				fromReference: "",
				limit:         -1,
			},
			want:     nil,
			wantMore: false,
			wantErr:  true,
		},
		{
			name: "no branch",
			args: args{
				repository:    repository,
				branch:        "",
				fromReference: "",
				limit:         -1,
			},
			want:     nil,
			wantMore: false,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListCommits(ctx, tt.args.repository, tt.args.branch, tt.args.fromReference, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListCommits() error = %s, wantErr %t", err, tt.wantErr)
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

func setupListCommitsByBranchData(t *testing.T, ctx context.Context, c Cataloger, repository, branch string) []*CommitLog {
	var commits []*CommitLog
	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("/file%d", i)
		fileAddr := fmt.Sprintf("/addr%d", i)
		if err := c.CreateEntry(ctx, repository, branch, Entry{
			Path:            fileName,
			Checksum:        strings.Repeat("ff", i),
			PhysicalAddress: fileAddr,
			Size:            int64(i) + 1,
		}, CreateEntryParams{}); err != nil {
			t.Fatal("Write entry for list repository commits failed", err)
		}
		message := "commit" + strconv.Itoa(i+1)
		commitLog, err := c.Commit(ctx, repository, branch, message, "tester", nil)
		if err != nil {
			t.Fatalf("Commit for list repository commits failed '%s': %s", message, err)
		}
		commits = append(commits, commitLog)
	}
	return commits
}
