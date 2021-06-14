package ref

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
	gtestutil "github.com/treeverse/lakefs/pkg/graveler/testutil"
)

type testExpirationDateGetter struct {
	expirationDates map[string]time.Time
}

func (t *testExpirationDateGetter) Get(c *graveler.CommitRecord) time.Time {
	return t.expirationDates[string(c.CommitID)]
}

type testCommit struct {
	daysPassed int
	parents    []graveler.CommitID
}

func newTestCommit(daysPassed int, parents ...graveler.CommitID) testCommit {
	return testCommit{
		daysPassed: daysPassed,
		parents:    parents,
	}
}

func newCommitSet(commitIDs []string) map[graveler.CommitID]bool {
	res := make(map[graveler.CommitID]bool, 0)
	for _, commitID := range commitIDs {
		res[graveler.CommitID(commitID)] = true
	}
	return res
}

func TestExpiredCommits(t *testing.T) {
	tests := map[string]struct {
		commits            map[string]testCommit
		headsRetentionDays map[string]int
		previouslyExpired  []string
		expectedActiveIDs  []string
		expectedExpiredIDs []string
	}{
		"two_branches": {
			commits: map[string]testCommit{
				"a": newTestCommit(15),
				"b": newTestCommit(10, "a"),
				"c": newTestCommit(10, "a"),
				"d": newTestCommit(5, "c"),
				"e": newTestCommit(5, "b"),
				"f": newTestCommit(1, "e"),
			},
			headsRetentionDays: map[string]int{"f": 7, "d": 3},
			expectedActiveIDs:  []string{"b", "d", "e", "f"},
			expectedExpiredIDs: []string{"a", "c"},
		},
		"old_heads": {
			commits: map[string]testCommit{
				"a": newTestCommit(15),
				"b": newTestCommit(20, "a"),
				"c": newTestCommit(20, "a"),
				"d": newTestCommit(20, "a"),
			},
			headsRetentionDays: map[string]int{"b": 7, "c": 7, "d": 7},
			expectedActiveIDs:  []string{"b", "c", "d"},
			expectedExpiredIDs: []string{"a"},
		},
		"all_commits_active": {
			commits: map[string]testCommit{
				"a": newTestCommit(5),
				"b": newTestCommit(4, "a"),
				"c": newTestCommit(3, "b"),
				"d": newTestCommit(2, "b"),
				"e": newTestCommit(1, "b"),
			},
			headsRetentionDays: map[string]int{"d": 15, "e": 7, "c": 2},
			expectedActiveIDs:  []string{"a", "b", "c", "d", "e"},
			expectedExpiredIDs: []string{},
		},
		"merge": {
			commits: map[string]testCommit{
				"a": newTestCommit(7),
				"b": newTestCommit(6, "a"),
				"c": newTestCommit(7),
				"d": newTestCommit(6, "c", "a"),
			},
			headsRetentionDays: map[string]int{"b": 3, "d": 10},
			expectedActiveIDs:  []string{"b", "c", "d"},
			expectedExpiredIDs: []string{"a"},
		},
		"two_branches_with_previously_expired": {
			commits: map[string]testCommit{
				"a": newTestCommit(15),
				"b": newTestCommit(10, "a"),
				"c": newTestCommit(10, "a"),
				"d": newTestCommit(5, "c"),
				"e": newTestCommit(5, "b"),
				"f": newTestCommit(1, "e"),
			},
			headsRetentionDays: map[string]int{"f": 7, "d": 3},
			previouslyExpired:  []string{"a"},
			expectedActiveIDs:  []string{"b", "d", "e", "f"},
			expectedExpiredIDs: []string{"c"},
		},
		"many_previously_expired": {
			commits: map[string]testCommit{
				"e7": newTestCommit(6),
				"e6": newTestCommit(6, "e7"),
				"e5": newTestCommit(6, "e6"),
				"e4": newTestCommit(6, "e5"),
				"e3": newTestCommit(6, "e4"),
				"e2": newTestCommit(6, "e3"),
				"e1": newTestCommit(6, "e2"),
				"a":  newTestCommit(6, "e1"),
				"b":  newTestCommit(5, "a"),
				"c":  newTestCommit(5, "a"),
			},
			headsRetentionDays: map[string]int{"c": 7, "b": 7},
			previouslyExpired:  []string{"e1", "e2", "e3", "e4", "e5", "e6", "e7"},
			expectedActiveIDs:  []string{"a", "b", "c"},
			expectedExpiredIDs: []string{},
		},
		"merge_in_history": {
			// graph taken from git core tests
			// E---D---C---B---A
			// \"-_         \   \
			//  \  `---------G   \
			//   \                \
			//    F----------------H
			commits: map[string]testCommit{
				"e": newTestCommit(21),
				"d": newTestCommit(20, "e"),
				"f": newTestCommit(19, "e"),
				"c": newTestCommit(18, "e"),
				"b": newTestCommit(17, "d"),
				"a": newTestCommit(4, "c"),
				"g": newTestCommit(4, "b", "e"),
				"h": newTestCommit(3, "a", "f"),
			},
			headsRetentionDays: map[string]int{"h": 14, "g": 7, "f": 7},
			previouslyExpired:  []string{},
			expectedActiveIDs:  []string{"h", "a", "b", "c", "f", "g"},
			expectedExpiredIDs: []string{"e", "d"},
		},
	}
	now := time.Now()
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			branchRecords := make([]*graveler.BranchRecord, 0, len(tst.headsRetentionDays))
			expirationDates := make(map[string]time.Time)
			ctrl := gomock.NewController(t)
			refManagerMock := mock.NewMockRefManager(ctrl)
			ctx := context.Background()
			for head, retentionDays := range tst.headsRetentionDays {
				branchRecords = append(branchRecords, &graveler.BranchRecord{
					Branch: &graveler.Branch{CommitID: graveler.CommitID(head)},
				})
				expirationDates[head] = now.AddDate(0, 0, -retentionDays)
			}
			sort.Slice(branchRecords, func(i, j int) bool {
				return expirationDates[string(branchRecords[i].CommitID)].Before(expirationDates[string(branchRecords[j].CommitID)])
			})
			branchIterator := gtestutil.NewFakeBranchIterator(branchRecords)
			refManagerMock.EXPECT().ListBranches(ctx, graveler.RepositoryID("test")).Return(branchIterator, nil)
			commitMap := make(map[graveler.CommitID]*graveler.Commit)
			previouslyExpired := newCommitSet(tst.previouslyExpired)
			for commitID, testCommit := range tst.commits {
				id := graveler.CommitID(commitID)
				commitMap[id] = &graveler.Commit{Message: commitID, Parents: testCommit.parents, CreationDate: now.AddDate(0, 0, -testCommit.daysPassed)}
				if !previouslyExpired[id] {
					refManagerMock.EXPECT().GetCommit(ctx, graveler.RepositoryID("test"), id).Return(commitMap[id], nil)
				}
			}
			finder := ExpiredCommitsFinder{
				commitGetter: refManagerMock,
				branchLister: refManagerMock,
				expirationDateGetter: &testExpirationDateGetter{
					expirationDates: expirationDates,
				},
			}
			previouslyExpiredCommitIDs := make([]graveler.CommitID, len(tst.previouslyExpired))
			for i := range tst.previouslyExpired {
				previouslyExpiredCommitIDs[i] = graveler.CommitID(tst.previouslyExpired[i])
			}
			activeCommits, expiredCommits, err := finder.GetExpiredCommits(ctx, "test", previouslyExpiredCommitIDs)
			if err != nil {
				t.Fatalf("failed to find expired commits: %v", err)
			}
			sort.Strings(tst.expectedActiveIDs)
			sort.Slice(activeCommits, func(i, j int) bool {
				return activeCommits[i].Ref() < activeCommits[j].Ref()
			})
			if diff := deep.Equal(tst.expectedActiveIDs, testToStringArray(activeCommits)); diff != nil {
				t.Errorf("active commits ids diff=%s", diff)
			}

			sort.Strings(tst.expectedExpiredIDs)
			sort.Slice(expiredCommits, func(i, j int) bool {
				return expiredCommits[i].Ref() < expiredCommits[j].Ref()
			})
			if diff := deep.Equal(tst.expectedExpiredIDs, testToStringArray(expiredCommits)); diff != nil {
				t.Errorf("expired commits ids diff=%s", diff)
			}
		})
	}
}

func testToStringArray(commitIDs []graveler.CommitID) []string {
	res := make([]string, len(commitIDs))
	for i := range commitIDs {
		res[i] = string(commitIDs[i])
	}
	return res
}
