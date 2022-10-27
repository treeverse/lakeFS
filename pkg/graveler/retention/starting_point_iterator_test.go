package retention

import (
	"sort"
	"testing"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

func TestStartingPointIterator(t *testing.T) {
	tests := map[string]struct {
		branches []string
		commits  []string
	}{
		"empty": {},
		"no_repetitions": {
			branches: []string{"1", "3", "5"},
			commits:  []string{"2", "4"},
		},
		"repetitions": {
			branches: []string{"2", "3", "4"},
			commits:  []string{"1", "4", "5"},
		},
		"commits_empty": {
			branches: []string{"2", "3", "4"},
			commits:  []string{},
		},
		"branches_empty": {
			branches: []string{},
			commits:  []string{"2", "3", "4"},
		},
		"no_overlap": {
			branches: []string{"1", "2", "3", "4"},
			commits:  []string{"5", "6", "7", "8"},
		},
		"no_overlap_reverse": {
			branches: []string{"5", "6", "7", "8"},
			commits:  []string{"1", "2", "3", "4"},
		},
		"many_repetitions": {
			branches: []string{"1", "2", "4", "5", "6", "7", "8"},
			commits:  []string{"0", "1", "2", "3", "6", "9"},
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			var branchRecords []*graveler.BranchRecord
			var commitRecords []*graveler.CommitRecord
			expected := make([]string, 0)
			branchMap := make(map[string]struct{})
			for _, b := range tst.branches {
				branchMap[b] = struct{}{}
				expected = append(expected, b)
				branchRecords = append(branchRecords, &graveler.BranchRecord{
					BranchID: graveler.BranchID(b),
					Branch: &graveler.Branch{
						CommitID: graveler.CommitID(b),
					},
				})
			}
			for _, c := range tst.commits {
				if _, ok := branchMap[c]; !ok {
					expected = append(expected, c)
				}
				commitRecords = append(commitRecords, &graveler.CommitRecord{
					CommitID: graveler.CommitID(c),
				})
			}
			sort.Strings(expected)
			branchIterator := testutil.NewFakeBranchIterator(branchRecords)
			commitIterator := testutil.NewFakeCommitIterator(commitRecords)
			it := NewGCStartingPointIterator(commitIterator, branchIterator)
			i := 0
			for it.Next() {
				val := it.Value()
				if string(val.CommitID) != expected[i] {
					t.Fatalf("unexpected returned commit id in index %d. expected=%s, actual=%s", i, expected[i], val)
				}
				expectedBranchID := ""
				if _, ok := branchMap[string(val.CommitID)]; ok {
					expectedBranchID = string(val.CommitID)
				}
				if string(val.BranchID) != expectedBranchID {
					t.Fatalf("got unexpected branch_id for commit %s. expected=%s, got=%s", val.CommitID, expectedBranchID, val.BranchID)
				}
				i++
			}
			if it.Err() != nil {
				t.Fatalf("unexpected error: %v", it.Err())
			}
			it.Close()
			if i != len(expected) {
				t.Fatalf("got unexpected number of results. expected=%d, got=%d", len(expected), i)
			}
		})
	}
}
