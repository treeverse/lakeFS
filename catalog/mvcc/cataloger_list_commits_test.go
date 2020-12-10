package mvcc

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ListCommits(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	importInitialCommitReference, err := c.GetBranchReference(ctx, repository, catalog.DefaultImportBranchName)
	testutil.MustDo(t, "get import branch reference", err)

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
		want     []*catalog.CommitLog
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
			want: []*catalog.CommitLog{
				{Reference: commits[2].Reference, Committer: "tester", Message: "commit3 on branch master", Metadata: catalog.Metadata{}, Parents: []string{"~KJ8Wd1Rs96b"}},
				{Reference: commits[1].Reference, Committer: "tester", Message: "commit2 on branch master", Metadata: catalog.Metadata{}, Parents: []string{"~KJ8Wd1Rs96a"}},
				{Reference: commits[0].Reference, Committer: "tester", Message: "commit1 on branch master", Metadata: catalog.Metadata{}, Parents: []string{"~KJ8Wd1Rs96Z"}},
				{Reference: initialCommitReference, Committer: catalog.DefaultCommitter, Message: createRepositoryCommitMessage, Metadata: catalog.Metadata{}, Parents: []string{"~3BJEPkwHYMdLDpaBrwnpTPmfarYET3yz"}},
				{Reference: importInitialCommitReference, Committer: catalog.DefaultCommitter, Message: createRepositoryImportCommitMessage, Metadata: catalog.Metadata{}},
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
			want: []*catalog.CommitLog{
				{Reference: commits[2].Reference, Committer: "tester", Message: "commit3 on branch master", Metadata: catalog.Metadata{}, Parents: []string{"~KJ8Wd1Rs96b"}},
				{Reference: commits[1].Reference, Committer: "tester", Message: "commit2 on branch master", Metadata: catalog.Metadata{}, Parents: []string{"~KJ8Wd1Rs96a"}},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "get last commit",
			args: args{
				repository:    repository,
				branch:        "master",
				fromReference: initialCommitReference,
				limit:         1,
			},
			want: []*catalog.CommitLog{
				{Reference: importInitialCommitReference, Committer: catalog.DefaultCommitter, Message: createRepositoryImportCommitMessage, Metadata: catalog.Metadata{}},
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
			want: []*catalog.CommitLog{
				{Reference: commits[1].Reference, Committer: "tester", Message: "commit2 on branch master", Metadata: catalog.Metadata{}, Parents: []string{"~KJ8Wd1Rs96a"}},
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
	masterCommits, _, err := c.ListCommits(ctx, repository, "master", "", 100)
	testutil.Must(t, err)
	br1Commits, _, err := c.ListCommits(ctx, repository, "br_1", "", 100)
	testutil.Must(t, err)
	if diff := deep.Equal(masterCommits, br1Commits[1:]); diff != nil {
		t.Error("br_1 did not inherit commits correctly", diff)
	}
	br2Commits, _, err := c.ListCommits(ctx, repository, "br_2", "", 100)
	if err != nil {
		t.Fatalf("ListCommits() error = %s", err)
	}
	if len(br2Commits) != 7 {
		t.Fatalf("ListCommits() error = %s", err)
	}

	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "master-file",
		Checksum:        "ssss",
		PhysicalAddress: "xxxxxxx",
		Size:            10000,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("Write entry for list repository commits failed", err)
	}
	commitLog, err := c.Commit(ctx, repository, "master", "commit master", "tester", nil)
	_ = commitLog
	if err != nil {
		t.Fatalf("Commit for list repository commits failed '%s': %s", "master commit failed", err)
	}
	_, err = c.Merge(ctx, repository, "master", "br_1", "tester", "", nil)
	testutil.Must(t, err)
	_, _, err = c.ListCommits(ctx, repository, "br_2", "", 100)
	testutil.Must(t, err)
	_, _, err = c.ListCommits(ctx, repository, "br_1", "", 100)
	testutil.Must(t, err)
}

func setupListCommitsByBranchData(t *testing.T, ctx context.Context, c catalog.Cataloger, repository, branch string) []*catalog.CommitLog {
	var commits []*catalog.CommitLog
	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("/file%d", i)
		fileAddr := fmt.Sprintf("/addr%d", i)
		if err := c.CreateEntry(ctx, repository, branch, catalog.Entry{
			Path:            fileName,
			Checksum:        strings.Repeat("ff", i),
			PhysicalAddress: fileAddr,
			Size:            int64(i) + 1,
		}, catalog.CreateEntryParams{}); err != nil {
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

	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "master-file",
		Checksum:        "ssss",
		PhysicalAddress: "xxxxxxx",
		Size:            10000,
	}, catalog.CreateEntryParams{}); err != nil {
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

func TestCataloger_ListCommits_LineageFromChild(t *testing.T) {
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

	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "master-file",
		Checksum:        "ssss",
		PhysicalAddress: "xxxxxxx",
		Size:            10000,
	}, catalog.CreateEntryParams{}); err != nil {
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

	br11BaseList, _, err := c.ListCommits(ctx, repository, "br_1_1", "", 100)
	testutil.MustDo(t, "list br_1_1 commits", err)
	if diff := deep.Equal(masterCommits[0], br11BaseList[1]); diff != nil {
		t.Error("br_1_1 did not inherit commits correctly", diff)
	}

	testCatalogerBranch(t, ctx, c, repository, "br_2_1", "master")
	testCatalogerBranch(t, ctx, c, repository, "br_2_2", "br_2_1")
	if err := c.CreateEntry(ctx, repository, "br_2_2", catalog.Entry{
		Path:            "master-file",
		Checksum:        "zzzzz",
		PhysicalAddress: "yyyyy",
		Size:            20000,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("Write entry to br_2_2 failed", err)
	}
	_, err = c.Commit(ctx, repository, "br_2_2", "commit master-file to br_2_2", "tester", nil)
	if err != nil {
		t.Fatalf("Commit for list repository commits failed '%s': %s", "br_2_2  commit failed", err)
	}
	br22List, _, err := c.ListCommits(ctx, repository, "br_2_2", "", 100)
	testutil.MustDo(t, "list br_2_2  commits", err)
	_ = br22List
	_, err = c.Merge(ctx, repository, "br_2_2", "br_2_1", "tester", "merge br_2_2 to br_2_1", nil)
	testutil.MustDo(t, "merge br_2_2  into br_2_1", err)
	br21List, _, err := c.ListCommits(ctx, repository, "br_2_1", "", 100)
	testutil.MustDo(t, "list br_2_1  commits", err)
	_ = br21List
	masterList, _, err := c.ListCommits(ctx, repository, "master", "", 100)
	testutil.MustDo(t, "list master commits", err)
	if diff := deep.Equal(masterCommits, masterList); diff != nil {
		t.Error("master commits changed before merge", diff)
	}
	merge2, err := c.Merge(ctx, repository, "br_2_1", "master", "tester", "merge br_2_1 to master", nil)
	testutil.MustDo(t, "merge br_2_1  into master", err)
	commitLog, err := c.GetCommit(ctx, repository, merge2.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(merge2.Summary, map[catalog.DifferenceType]int{
		catalog.DifferenceTypeChanged: 1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//
	//if differences[0].Type != catalog.DifferenceTypeChanged || differences[0].Path != "master-file" {
	//	t.Error("merge br_2_1 into master with unexpected results", differences[0])
	//}

	masterList, _, err = c.ListCommits(ctx, repository, "master", "", 100)
	testutil.MustDo(t, "list master commits", err)
	if diff := deep.Equal(br21List, masterList[1:]); diff != nil {
		t.Error("master commits list mismatch with br_2_1_list", diff)
	}

	br11List, _, err := c.ListCommits(ctx, repository, "br_1_1", "", 100)
	testutil.MustDo(t, "list br_1_1 commits", err)
	if diff := deep.Equal(br11BaseList, br11List); diff != nil {
		t.Error("br_1_1 commits changed before merge", diff)
	}
	_, err = c.Merge(ctx, repository, "master", "br_1_1", "tester", "merge master to br_1_1", nil)
	testutil.MustDo(t, "merge master  into br_1_1", err)
	br11List, _, err = c.ListCommits(ctx, repository, "br_1_1", "", 100)
	testutil.MustDo(t, "list br_1_1 commits", err)
	if diff := deep.Equal(masterList[:5], br11List[1:6]); diff != nil {
		t.Error("master 5 first different from br_1_1 [1:6]", diff)
	}
	if diff := deep.Equal(masterList[6:], br11List[9:]); diff != nil {
		t.Error("master 5 first different from br_1_1 [1:6]", diff)
	}
	// test that a change to br_2_2 does not propagate to master
	if err := c.CreateEntry(ctx, repository, "br_2_2", catalog.Entry{
		Path:            "no-propagate-file",
		Checksum:        "aaaaaaaa",
		PhysicalAddress: "yybbbbbbyyy",
		Size:            20000,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("Write no-propagate-file to br_2_2 failed", err)
	}
	_, err = c.Commit(ctx, repository, "br_2_2", "commit master-file to br_2_2", "tester", nil)
	if err != nil {
		t.Fatalf("no-propagate-Commit for list repository commits failed '%s': %s", "br_2_2  commit failed", err)
	}
	_, err = c.Merge(ctx, repository, "br_2_2", "br_2_1", "tester", "merge br_2_2 to br_2_1", nil)
	testutil.MustDo(t, "second merge br_2_2  into br_2_1", err)
	newBr21List, _, err := c.ListCommits(ctx, repository, "br_2_1", "", 100)
	testutil.MustDo(t, "second list br_2_1 commits", err)
	if diff := deep.Equal(br21List, newBr21List); diff == nil {
		t.Error("br_2_1 commits did not changed after merge", diff)
	}
	newMasterList, _, err := c.ListCommits(ctx, repository, "master", "", 100)
	testutil.MustDo(t, "third list master commits", err)
	if diff := deep.Equal(newMasterList, masterList); diff != nil {
		t.Error("master commits  changed without merge", diff)
	}
}

func TestCataloger_ListCommits_Order(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	var wg sync.WaitGroup
	const concurrency = 5
	errors := make([]error, concurrency)
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			repository := testCatalogerRepo(t, ctx, c, "repo", "master")

			testCatalogerCreateEntry(t, ctx, c, repository, "master", "files/first", nil, "")
			const commit1Msg = "first"
			_, err := c.Commit(ctx, repository, "master", commit1Msg, "barak.amar", nil)
			if err != nil {
				errors[id] = fmt.Errorf("%s: %w", commit1Msg, err)
				return
			}

			testCatalogerBranch(t, ctx, c, repository, "branch1", "master")

			commitsLog, _, err := c.ListCommits(ctx, repository, "branch1", "", 300)
			if err != nil {
				errors[id] = fmt.Errorf("list branch1 commits: %w", err)
				return
			}

			// get all commits without the first one
			commits := make([]string, len(commitsLog))
			for i := range commitsLog {
				commits[i] = commitsLog[i].Message
			}
			expectedCommits := []string{
				FormatBranchCommitMessage("branch1", "master"),
				commit1Msg,
				createRepositoryCommitMessage,
				createRepositoryImportCommitMessage,
			}

			if diff := deep.Equal(commits, expectedCommits); diff != nil {
				errors[id] = fmt.Errorf("branch1 did not had the expected commits: %s", diff)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errors {
		if err != nil {
			t.Errorf("worker %d failed: %s", i, err)
		}
	}
}
