package retention

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

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

// findMainAncestryLeaves returns commits which are not the first parent of any child.
func findMainAncestryLeaves(now time.Time, heads map[string]int32, commits map[string]testCommit) []*graveler.CommitRecord {
	var res []*graveler.CommitRecord
	for commitID1, commit1 := range commits {
		if _, ok := heads[commitID1]; ok {
			continue
		}
		isLeaf := true
		for _, commit2 := range commits {
			if len(commit2.parents) == 0 {
				continue
			}
			if commitID1 == string(commit2.parents[0]) {
				isLeaf = false
			}
		}
		if isLeaf {
			res = append(res, &graveler.CommitRecord{
				CommitID: graveler.CommitID(commitID1),
				Commit: &graveler.Commit{
					Version:      graveler.CurrentCommitVersion,
					CreationDate: now.AddDate(0, 0, -commit1.daysPassed),
					Parents:      commit1.parents,
				},
			})
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].CommitID < res[j].CommitID
	})
	return res
}

// fakeRepositoryCommitGetter is a RepositoryCommitGetter used to test CommitsMap.
type fakeRepositoryCommitGetter struct {
	// Commits is the list of pre-existing commits.
	Commits []*graveler.CommitRecord
	// AnotherCommit, if not nil, is returned from Get (but not List).
	// It can only be returned once.
	AnotherCommit *graveler.CommitRecord
	// GetCalled is true after the first call to Get.
	GetCalled bool
}

var errTooManyGets = errors.New("more than one Get on fakeRepositoryCommitGetter")

func (c *fakeRepositoryCommitGetter) List(_ context.Context) (graveler.CommitIterator, error) {
	return testutil.NewFakeCommitIterator(c.Commits), nil
}

func (c *fakeRepositoryCommitGetter) Get(_ context.Context, id graveler.CommitID) (*graveler.Commit, error) {
	if c.AnotherCommit == nil {
		return nil, fmt.Errorf("%w: Get(%s) when no other commits", graveler.ErrNotFound, id)
	}
	if id != c.AnotherCommit.CommitID {
		return nil, fmt.Errorf("%w: Get(%s) when expecting Get(%s)", graveler.ErrNotFound, id, c.AnotherCommit.CommitID)
	}
	if c.GetCalled {
		return nil, fmt.Errorf("%s: %w", id, errTooManyGets)
	}
	c.GetCalled = true
	return c.AnotherCommit.Commit, nil
}

func TestCommitsMap(t *testing.T) {
	cases := []struct {
		Name         string
		CommitGetter *fakeRepositoryCommitGetter
	}{
		{
			Name: "FromList",
			CommitGetter: &fakeRepositoryCommitGetter{
				Commits: []*graveler.CommitRecord{
					{
						CommitID: "a",
						Commit: &graveler.Commit{
							MetaRangeID: graveler.MetaRangeID("metarange:A"),
						},
					},
					{
						CommitID: "b",
						Commit: &graveler.Commit{
							MetaRangeID: graveler.MetaRangeID("metarange:B"),
						},
					},
				},
				AnotherCommit: nil,
			},
		}, {
			Name: "FromGet",
			CommitGetter: &fakeRepositoryCommitGetter{
				Commits: []*graveler.CommitRecord{
					{
						CommitID: "a",
						Commit: &graveler.Commit{
							MetaRangeID: graveler.MetaRangeID("metarange:A"),
						},
					},
				},
				AnotherCommit: &graveler.CommitRecord{
					CommitID: "b",
					Commit: &graveler.Commit{
						MetaRangeID: graveler.MetaRangeID("metarange:B"),
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			commitsMap, err := NewCommitsMap(context.Background(), tc.CommitGetter)
			if err != nil {
				t.Fatal(err)
			}

			a, err := commitsMap.Get(graveler.CommitID("a"))
			if err != nil {
				t.Errorf("Failed to get a: %s", err)
			}
			if a.MetaRangeID != "metarange:A" {
				t.Errorf("Got metarange %s for a, expected \"metarange:A\"", a.MetaRangeID)
			}
			b, err := commitsMap.Get(graveler.CommitID("b"))
			if err != nil {
				t.Errorf("Failed to get b: %s", err)
			}
			if b.MetaRangeID != "metarange:B" {
				t.Errorf("Got metarange %s for b, expected \"metarange:B\"", b.MetaRangeID)
			}
			c, err := commitsMap.Get(graveler.CommitID("c"))
			if !errors.Is(err, graveler.ErrNotFound) {
				t.Errorf("Got node %+v, error %s for c, expected not found", c.MetaRangeID, err)
			}
		})
	}
}

func TestActiveCommits(t *testing.T) {
	tests := map[string]struct {
		commits            map[string]testCommit
		headsRetentionDays map[string]int32
		expectedActiveIDs  []string
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
			headsRetentionDays: map[string]int32{"f": 7, "d": 3},
			expectedActiveIDs:  []string{"b", "d", "e", "f"},
		},
		"old_heads": {
			commits: map[string]testCommit{
				"a": newTestCommit(15),
				"b": newTestCommit(20, "a"),
				"c": newTestCommit(20, "a"),
				"d": newTestCommit(20, "a"),
			},
			headsRetentionDays: map[string]int32{"b": 7, "c": 7, "d": 7},
			expectedActiveIDs:  []string{"b", "c", "d"},
		},
		"all_commits_active": {
			commits: map[string]testCommit{
				"a": newTestCommit(5),
				"b": newTestCommit(4, "a"),
				"c": newTestCommit(3, "b"),
				"d": newTestCommit(2, "b"),
				"e": newTestCommit(1, "b"),
			},
			headsRetentionDays: map[string]int32{"d": 15, "e": 7, "c": 2},
			expectedActiveIDs:  []string{"a", "b", "c", "d", "e"},
		},
		"merge": {
			commits: map[string]testCommit{
				"a": newTestCommit(7),
				"b": newTestCommit(6, "a"),
				"c": newTestCommit(7),
				"d": newTestCommit(6, "c", "a"),
			},
			headsRetentionDays: map[string]int32{"b": 3, "d": 10},
			expectedActiveIDs:  []string{"b", "c", "d"},
		},
		"two_branches_with_previously_expired": {
			commits: map[string]testCommit{
				"a": newTestCommit(15),
				"b": newTestCommit(10, "a"),
				"c": newTestCommit(10, "a"),
				"d": newTestCommit(5, "c"),
				"e": newTestCommit(7, "b"),
				"f": newTestCommit(1, "e"),
			},
			headsRetentionDays: map[string]int32{"f": 7, "d": 3},
			expectedActiveIDs:  []string{"d", "e", "f"},
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
			headsRetentionDays: map[string]int32{"h": 14, "g": 7, "f": 7},
			expectedActiveIDs:  []string{"h", "a", "b", "c", "f", "g"},
		},
		"dangling_commits_active": {
			commits: map[string]testCommit{
				"a": newTestCommit(15),
				"b": newTestCommit(10, "a"),
				"c": newTestCommit(10, "a"),
				"d": newTestCommit(5, "c"),
				"e": newTestCommit(5, "b"),
				"f": newTestCommit(1, "e"),
				"g": newTestCommit(8, "c"),
				"h": newTestCommit(7, "g"),
				"i": newTestCommit(4, "h"),
			},
			headsRetentionDays: map[string]int32{"f": 7, "d": 3},
			expectedActiveIDs:  []string{"b", "d", "e", "f", "h", "i"},
		},
		"dangling_commits_expired": {
			commits: map[string]testCommit{
				"a": newTestCommit(15),
				"b": newTestCommit(10, "a"),
				"c": newTestCommit(10, "a"),
				"d": newTestCommit(5, "c"),
				"e": newTestCommit(5, "b"),
				"f": newTestCommit(1, "e"),
				"g": newTestCommit(8, "c"),
				"h": newTestCommit(7, "g"),
				"i": newTestCommit(6, "h"),
			},
			headsRetentionDays: map[string]int32{"f": 7, "d": 3},
			expectedActiveIDs:  []string{"b", "d", "e", "f"},
		},

		"dangling_from_before_expired": {
			commits: map[string]testCommit{
				"root":        newTestCommit(20),
				"pre_expired": newTestCommit(20, "root"),
				"e1":          newTestCommit(15, "pre_expired"),
				"b":           newTestCommit(10, "e1"),
				"c":           newTestCommit(10, "e1"),
				"d":           newTestCommit(5, "c"),
				"e":           newTestCommit(8, "b"),
				"f":           newTestCommit(1, "e"),
				"g":           newTestCommit(10, "root"), // dangling
				"h":           newTestCommit(6, "g"),     // dangling
			},
			headsRetentionDays: map[string]int32{"f": 7, "d": 3},
			expectedActiveIDs:  []string{"d", "e", "f"},
		},
		"retained_by_non_leaf_head": {
			// commit x is retained because of the rule of head2, and not the rule of head1.
			commits: map[string]testCommit{
				"root":  newTestCommit(20),
				"x":     newTestCommit(14, "root"),
				"head2": newTestCommit(10, "x"),
				"head1": newTestCommit(9, "head2"),
			},
			headsRetentionDays: map[string]int32{"head1": 9, "head2": 12},
			expectedActiveIDs:  []string{"head1", "head2", "x"},
		},
		/*
			<ep1- 8 days>
				\
				  <e4- 7 days> -- <e1- 7 days> -- <h1- 1 day>        (5-day-retention)
				/
			<ep2- 8 days> -- <e2- 7 days> -- <h2- 1 day>             (5-day-retention)
				\
				  <e5- 6 days> -- <e3- 6 days> -- <h3- 1 day>        (5-day-retention)
		*/
		"reachable_previously_expired": {
			commits: map[string]testCommit{
				"ep1": newTestCommit(8),
				"ep2": newTestCommit(8),
				"e5":  newTestCommit(6, "ep2"),
				"e4":  newTestCommit(7, "ep1", "ep2"),
				"e3":  newTestCommit(6, "e5"),  // expired yet active
				"e2":  newTestCommit(6, "ep2"), // expired yet active
				"e1":  newTestCommit(7, "e4"),  // expired yet active
				"h3":  newTestCommit(1, "e3"),
				"h2":  newTestCommit(1, "e2"),
				"h1":  newTestCommit(1, "e1"),
			},
			headsRetentionDays: map[string]int32{"h1": 5, "h2": 5, "h3": 5},
			expectedActiveIDs:  []string{"h1", "h2", "h3", "e1", "e2", "e3"},
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			ctrl := gomock.NewController(t)
			refManagerMock := mock.NewMockRefManager(ctrl)
			ctx := context.Background()
			repositoryRecord := &graveler.RepositoryRecord{
				RepositoryID: "test",
			}
			garbageCollectionRules := &graveler.GarbageCollectionRules{DefaultRetentionDays: 5, BranchRetentionDays: make(map[string]int32)}
			var branches []*graveler.BranchRecord
			for head, retentionDays := range tst.headsRetentionDays {
				branches = append(branches, &graveler.BranchRecord{
					BranchID: graveler.BranchID(head),
					Branch: &graveler.Branch{
						CommitID: graveler.CommitID(head),
					},
				})
				garbageCollectionRules.BranchRetentionDays[head] = retentionDays
			}
			sort.Slice(branches, func(i, j int) bool {
				return branches[i].CommitID < branches[j].CommitID
			})

			var commitsRecords []*graveler.CommitRecord
			for commitID, commit := range tst.commits {
				commitsRecords = append(commitsRecords, &graveler.CommitRecord{
					CommitID: graveler.CommitID(commitID),
					Commit: &graveler.Commit{
						Parents:      commit.parents,
						CreationDate: now.AddDate(0, 0, -commit.daysPassed),
						Version:      graveler.CurrentCommitVersion,
						MetaRangeID:  graveler.MetaRangeID("mr-" + commitID),
					},
				})
			}

			refManagerMock.EXPECT().ListCommits(ctx, repositoryRecord).Return(testutil.NewFakeCommitIterator(commitsRecords), nil).MaxTimes(1)

			gcCommits, err := GetGarbageCollectionCommits(ctx, NewGCStartingPointIterator(
				testutil.NewFakeCommitIterator(findMainAncestryLeaves(now, tst.headsRetentionDays, tst.commits)),
				testutil.NewFakeBranchIterator(branches)), &repositoryCommitGetter{
				refManager: refManagerMock,
				repository: repositoryRecord,
			}, garbageCollectionRules)
			if err != nil {
				t.Fatalf("failed to find expired commits: %v", err)
			}
			validateMetaRangeIDs(t, gcCommits)
			activeCommitIDs := testMapToCommitIDs(gcCommits)

			sort.Strings(tst.expectedActiveIDs)
			sort.Slice(activeCommitIDs, func(i, j int) bool {
				return activeCommitIDs[i].Ref() < activeCommitIDs[j].Ref()
			})
			if diff := deep.Equal(tst.expectedActiveIDs, testToStringArray(activeCommitIDs)); diff != nil {
				t.Errorf("active commits ids diff=%s", diff)
			}
		})
	}
}

func validateMetaRangeIDs(t *testing.T, commits map[graveler.CommitID]graveler.MetaRangeID) {
	for commitID, metaRangeID := range commits {
		if string(metaRangeID) != "mr-"+string(commitID) {
			t.Errorf("unexpected metarange ID for commit %s. expected=%s, got=%s.", commitID, "mr-"+commitID, metaRangeID)
		}
	}
}

func testMapToCommitIDs(commits map[graveler.CommitID]graveler.MetaRangeID) []graveler.CommitID {
	res := make([]graveler.CommitID, 0, len(commits))
	for commitID := range commits {
		res = append(res, commitID)
	}
	return res
}

func testToStringArray(commitIDs []graveler.CommitID) []string {
	res := make([]string, len(commitIDs))
	for i := range commitIDs {
		res[i] = string(commitIDs[i])
	}
	return res
}
