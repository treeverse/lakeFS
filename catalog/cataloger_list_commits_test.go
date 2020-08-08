package catalog

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/testutil"
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
				{Reference: commits[2].Reference, Committer: "tester", Message: "commit3", Metadata: Metadata{}, Parents: []string{"~KJ8Wd1Rs96a"}},
				{Reference: commits[1].Reference, Committer: "tester", Message: "commit2", Metadata: Metadata{}, Parents: []string{"~KJ8Wd1Rs96Z"}},
				{Reference: commits[0].Reference, Committer: "tester", Message: "commit1", Metadata: Metadata{}, Parents: []string{"~KJ8Wd1Rs96Y"}},
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
				{Reference: commits[2].Reference, Committer: "tester", Message: "commit3", Metadata: Metadata{}, Parents: []string{"~KJ8Wd1Rs96a"}},
				{Reference: commits[1].Reference, Committer: "tester", Message: "commit2", Metadata: Metadata{}, Parents: []string{"~KJ8Wd1Rs96Z"}},
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
				{Reference: commits[1].Reference, Committer: "tester", Message: "commit2", Metadata: Metadata{}, Parents: []string{"~KJ8Wd1Rs96Z"}},
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

			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Error("ListCommits", diff)
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListCommits() gotMore = %v, want = %v", gotMore, tt.wantMore)
			}
		})
	}
	testCatalogerBranch(t, ctx, c, repository, "br_1", "master")
	testCatalogerBranch(t, ctx, c, repository, "br_2", "br_1")
	master_commits, _, err := c.ListCommits(ctx, repository, "master", "", 100)
	br_1_commits, _, err := c.ListCommits(ctx, repository, "br_1", "", 100)
	if diff := deep.Equal(master_commits, br_1_commits[1:]); diff != nil {
		t.Error("br_1 did not inherit commits correctly", diff)
	}
	br_2_commits, _, err := c.ListCommits(ctx, repository, "br_2", "", 100)
	_ = br_2_commits
	if err != nil {
		t.Fatalf("ListCommits() error = %s", err)
	}
	if len(br_2_commits) != 6 {
		t.Fatalf("ListCommits() error = %s", err)
	}
	// hack - remove the timestamp in order to compare everything except the time
	// consider create entry will control creation time

	//if diff := deep.Equal(got, tt.want); diff != nil {
	//	t.Error("ListCommits", diff)
	//}

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "master-file",
		Checksum:        "ssss",
		PhysicalAddress: "xxxxxxx",
		Size:            10000,
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Write entry for list repository commits failed", err)
	}
	commitLog, err := c.Commit(ctx, repository, "master", "commit master", "tester", nil)
	_ = commitLog
	if err != nil {
		t.Fatalf("Commit for list repository commits failed '%s': %s", "master commit failed", err)
	}
	_, err = c.Merge(ctx, repository, "master", "br_1", "tester", "", nil)

	got, _, err := c.ListCommits(ctx, repository, "br_2", "", 100)
	_ = got
	got, _, err = c.ListCommits(ctx, repository, "br_1", "", 100)

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
		message := "commit" + strconv.Itoa(i+1) + " on branch " + branch
		commitLog, err := c.Commit(ctx, repository, branch, message, "tester", nil)
		if err != nil {
			t.Fatalf("Commit for list repository commits failed '%s': %s", message, err)
		}
		commits = append(commits, commitLog)
	}
	return commits
}

func TestCataloger_ListCommits_Lineage(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	_ = setupListCommitsByBranchData(t, ctx, c, repository, "master")

	testCatalogerBranch(t, ctx, c, repository, "br_1", "master")
	testCatalogerBranch(t, ctx, c, repository, "br_2", "br_1")
	masterCommits, _, err := c.ListCommits(ctx, repository, "master", "", 100)
	testutil.MustDo(t, "list master commits", err)

	br1Commits, _, err := c.ListCommits(ctx, repository, "br_1", "", 100)
	testutil.MustDo(t, "list br_1 commits", err)

	// get all commits without the first one
	if diff := deep.Equal(masterCommits, br1Commits[1:]); diff != nil {
		t.Error("br_1 did not inherit commits correctly", diff)
	}

	b2Commits, _, err := c.ListCommits(ctx, repository, "br_2", "", 100)
	testutil.MustDo(t, "list br_2 commits", err)

	if diff := deep.Equal(br1Commits, b2Commits[1:]); diff != nil {
		t.Error("br_2 did not inherit commits correctly", diff)
	}

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "master-file",
		Checksum:        "ssss",
		PhysicalAddress: "xxxxxxx",
		Size:            10000,
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Write entry for list repository commits failed", err)
	}
	_, err = c.Commit(ctx, repository, "master", "commit master-file", "tester", nil)
	if err != nil {
		t.Fatalf("Commit for list repository commits failed '%s': %s", "master commit failed", err)
	}
	_, err = c.Merge(ctx, repository, "master", "br_1", "tester", "", nil)
	testutil.MustDo(t, "merge master  into br_1", err)

	got, _, err := c.ListCommits(ctx, repository, "br_2", "", 100)
	testutil.MustDo(t, "list br_2 commits", err)
	if diff := deep.Equal(got, b2Commits); diff != nil {
		t.Error("br_2 changed although not merged", diff)
	}
	masterCommits, _, err = c.ListCommits(ctx, repository, "master", "", 100)
	testutil.MustDo(t, "list master commits", err)

	got, _, err = c.ListCommits(ctx, repository, "br_1", "", 100)
	testutil.MustDo(t, "list br_1 commits", err)
	if diff := deep.Equal(masterCommits[0], got[1]); diff != nil {
		t.Error("br_1 did not inherit commits correctly", diff)
	}
}

func TestCataloger_ListCommits_Lineage_from_son(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	_ = setupListCommitsByBranchData(t, ctx, c, repository, "master")

	testCatalogerBranch(t, ctx, c, repository, "br_1_1", "master")
	testCatalogerBranch(t, ctx, c, repository, "br_1_2", "br_1_1")
	masterCommits, _, err := c.ListCommits(ctx, repository, "master", "", 100)
	testutil.MustDo(t, "list master commits", err)

	br1Commits, _, err := c.ListCommits(ctx, repository, "br_1_1", "", 100)
	testutil.MustDo(t, "list br_1_1 commits", err)

	// get all commits without the first one
	if diff := deep.Equal(masterCommits, br1Commits[1:]); diff != nil {
		t.Error("br_1_1 did not inherit commits correctly", diff)
	}

	b2Commits, _, err := c.ListCommits(ctx, repository, "br_1_2", "", 100)
	testutil.MustDo(t, "list br_1_2 commits", err)

	if diff := deep.Equal(br1Commits, b2Commits[1:]); diff != nil {
		t.Error("br_1_2 did not inherit commits correctly", diff)
	}

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "master-file",
		Checksum:        "ssss",
		PhysicalAddress: "xxxxxxx",
		Size:            10000,
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Write entry for list repository commits failed", err)
	}
	_, err = c.Commit(ctx, repository, "master", "commit master-file on master", "tester", nil)
	if err != nil {
		t.Fatalf("Commit for list repository commits failed '%s': %s", "master commit failed", err)
	}
	_, err = c.Merge(ctx, repository, "master", "br_1_1", "tester", "merge master to br_1_1", nil)
	testutil.MustDo(t, "merge master  into br_1_1", err)

	got, _, err := c.ListCommits(ctx, repository, "br_1_2", "", 100)
	testutil.MustDo(t, "list br_1_2 commits", err)
	if diff := deep.Equal(got, b2Commits); diff != nil {
		t.Error("br_1_2 changed although not merged", diff)
	}
	masterCommits, _, err = c.ListCommits(ctx, repository, "master", "", 100)
	testutil.MustDo(t, "list master commits", err)

	br_1_1_base_list, _, err := c.ListCommits(ctx, repository, "br_1_1", "", 100)
	testutil.MustDo(t, "list br_1_1 commits", err)
	if diff := deep.Equal(masterCommits[0], br_1_1_base_list[1]); diff != nil {
		t.Error("br_1_1 did not inherit commits correctly", diff)
	}

	testCatalogerBranch(t, ctx, c, repository, "br_2_1", "master")
	testCatalogerBranch(t, ctx, c, repository, "br_2_2", "br_2_1")
	if err := c.CreateEntry(ctx, repository, "br_2_2", Entry{
		Path:            "master-file",
		Checksum:        "zzzzz",
		PhysicalAddress: "yyyyy",
		Size:            20000,
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Write entry to br_2_2 failed", err)
	}
	_, err = c.Commit(ctx, repository, "br_2_2", "commit master-file to br_2_2", "tester", nil)
	if err != nil {
		t.Fatalf("Commit for list repository commits failed '%s': %s", "br_2_2  commit failed", err)
	}
	br_2_2_list, _, err := c.ListCommits(ctx, repository, "br_2_2", "", 100)
	testutil.MustDo(t, "list br_2_2  commits", err)
	_ = br_2_2_list
	_, err = c.Merge(ctx, repository, "br_2_2", "br_2_1", "tester", "merge br_2_2 to br_2_1", nil)
	testutil.MustDo(t, "merge br_2_2  into br_2_1", err)
	br_2_1_list, _, err := c.ListCommits(ctx, repository, "br_2_1", "", 100)
	testutil.MustDo(t, "list br_2_1  commits", err)
	_ = br_2_1_list
	masterList, _, err := c.ListCommits(ctx, repository, "master", "", 100)
	testutil.MustDo(t, "list master commits", err)
	if diff := deep.Equal(masterCommits, masterList); diff != nil {
		t.Error("master commits changed befor merge", diff)
	}
	merge_2, err := c.Merge(ctx, repository, "br_2_1", "master", "tester", "merge br_2_1 to master", nil)
	testutil.MustDo(t, "merge br_2_1  into master", err)
	if merge_2.Differences[0].Type != DifferenceTypeChanged || merge_2.Differences[0].Path != "master-file" {
		t.Error("merge br_2_1 into master with unexpected results", merge_2.Differences[0])
	}
	masterList, _, err = c.ListCommits(ctx, repository, "master", "", 100)
	testutil.MustDo(t, "list master commits", err)
	if diff := deep.Equal(br_2_1_list, masterList[1:]); diff != nil {
		t.Error("master commits list mismatch with br_2_1_list", diff)
	}

	br_1_1_list, _, err := c.ListCommits(ctx, repository, "br_1_1", "", 100)
	testutil.MustDo(t, "list br_1_1 commits", err)
	if diff := deep.Equal(br_1_1_base_list, br_1_1_list); diff != nil {
		t.Error("br_1_1 commits changed before merge", diff)
	}
	_, err = c.Merge(ctx, repository, "master", "br_1_1", "tester", "merge master to br_1_1", nil)
	testutil.MustDo(t, "merge master  into br_1_1", err)
	br_1_1_list, _, err = c.ListCommits(ctx, repository, "br_1_1", "", 100)
	testutil.MustDo(t, "list br_1_1 commits", err)
	if diff := deep.Equal(masterList[:5], br_1_1_list[1:6]); diff != nil {
		t.Error("master 5 first different from br_1_1 [1:6]", diff)
	}
	if diff := deep.Equal(masterList[6:], br_1_1_list[9:]); diff != nil {
		t.Error("master 5 first different from br_1_1 [1:6]", diff)
	}
}
