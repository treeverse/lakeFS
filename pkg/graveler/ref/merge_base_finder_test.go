package ref_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/testutil"
)

type MockCommitGetter struct {
	byCommitID map[graveler.CommitID]*graveler.Commit
	visited    map[graveler.CommitID]int
}

var repository = &graveler.RepositoryRecord{RepositoryID: "ref-test-repo"}

func (g *MockCommitGetter) GetCommit(_ context.Context, _ *graveler.RepositoryRecord, commitID graveler.CommitID) (*graveler.Commit, error) {
	if commit, ok := g.byCommitID[commitID]; ok {
		g.visited[commitID] += 1
		return commit, nil
	}
	return nil, graveler.ErrNotFound
}

func computeGeneration(byCommitID map[graveler.CommitID]*graveler.Commit, commit *graveler.Commit) int {
	if commit.Generation > 0 {
		return commit.Generation
	}
	if len(commit.Parents) == 0 {
		return 1
	}
	maxGeneration := 0
	for _, parent := range commit.Parents {
		parentCommit := byCommitID[parent]
		parentGeneration := computeGeneration(byCommitID, parentCommit)
		if parentGeneration > maxGeneration {
			maxGeneration = parentGeneration
		}
	}
	commit.Generation = maxGeneration + 1
	return commit.Generation
}

func newReader(kv map[graveler.CommitID]*graveler.Commit) *MockCommitGetter {
	for _, v := range kv {
		v.Generation = computeGeneration(kv, v)
	}

	return &MockCommitGetter{
		byCommitID: kv,
		visited:    map[graveler.CommitID]int{},
	}

}

func TestFindMergeBase(t *testing.T) {
	cases := []struct {
		Name     string
		Left     graveler.CommitID
		Right    graveler.CommitID
		Getter   func() *MockCommitGetter
		Expected []string
	}{
		{
			Name:  "root_match",
			Left:  "c7",
			Right: "c6",
			Getter: func() *MockCommitGetter {
				c0 := &graveler.Commit{Message: "c0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "c1", Parents: []graveler.CommitID{"c0"}}
				c2 := &graveler.Commit{Message: "c2", Parents: []graveler.CommitID{"c0"}}
				c3 := &graveler.Commit{Message: "c3", Parents: []graveler.CommitID{"c1"}}
				c4 := &graveler.Commit{Message: "c4", Parents: []graveler.CommitID{"c2"}}
				c5 := &graveler.Commit{Message: "c5", Parents: []graveler.CommitID{"c3"}}
				c6 := &graveler.Commit{Message: "c6", Parents: []graveler.CommitID{"c4"}}
				c7 := &graveler.Commit{Message: "c7", Parents: []graveler.CommitID{"c5"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4, "c5": c5, "c6": c6, "c7": c7,
				})
			},
			Expected: []string{"c0"},
		},
		{
			Name:  "close_ancestor",
			Left:  "c3",
			Right: "c4",
			Getter: func() *MockCommitGetter {
				c0 := &graveler.Commit{Message: "c0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "c1", Parents: []graveler.CommitID{"c0"}}
				c2 := &graveler.Commit{Message: "c2", Parents: []graveler.CommitID{"c1"}}
				c3 := &graveler.Commit{Message: "c3", Parents: []graveler.CommitID{"c2"}}
				c4 := &graveler.Commit{Message: "c4", Parents: []graveler.CommitID{"c2"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4,
				})
			},
			Expected: []string{"c2"},
		},
		{
			Name:  "criss_cross",
			Left:  "c5",
			Right: "c6",
			Getter: func() *MockCommitGetter {
				c0 := &graveler.Commit{Message: "c0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "c1", Parents: []graveler.CommitID{"c0"}}
				c2 := &graveler.Commit{Message: "c2", Parents: []graveler.CommitID{"c0"}}
				c3 := &graveler.Commit{Message: "c3", Parents: []graveler.CommitID{"c1", "c2"}}
				c4 := &graveler.Commit{Message: "c4", Parents: []graveler.CommitID{"c1", "c2"}}
				c5 := &graveler.Commit{Message: "c5", Parents: []graveler.CommitID{"c3"}}
				c6 := &graveler.Commit{Message: "c6", Parents: []graveler.CommitID{"c4"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4, "c5": c5, "c6": c6,
				})
			},
			Expected: []string{"c1", "c2"},
		},
		{
			Name:  "contained",
			Left:  "c2",
			Right: "c1",
			Getter: func() *MockCommitGetter {
				c0 := &graveler.Commit{Message: "c0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "c1", Parents: []graveler.CommitID{"c0"}}
				c2 := &graveler.Commit{Message: "c2", Parents: []graveler.CommitID{"c1"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2,
				})
			},
			Expected: []string{"c1"},
		},
		{
			Name:  "parallel",
			Left:  "c7",
			Right: "c3",
			Getter: func() *MockCommitGetter {
				c0 := &graveler.Commit{Message: "c0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "c1", Parents: []graveler.CommitID{"c0"}}
				c2 := &graveler.Commit{Message: "c2", Parents: []graveler.CommitID{"c1"}}
				c3 := &graveler.Commit{Message: "c3", Parents: []graveler.CommitID{"c2"}}
				c4 := &graveler.Commit{Message: "c4", Parents: []graveler.CommitID{}}
				c5 := &graveler.Commit{Message: "c5", Parents: []graveler.CommitID{"c4"}}
				c6 := &graveler.Commit{Message: "c6", Parents: []graveler.CommitID{"c5"}}
				c7 := &graveler.Commit{Message: "c7", Parents: []graveler.CommitID{"c6"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4, "c5": c5, "c6": c6, "c7": c7,
				})
			},
			Expected: []string{},
		},
		{
			Name:  "already_merged",
			Left:  "c3",
			Right: "c4",
			Getter: func() *MockCommitGetter {
				c0 := &graveler.Commit{Message: "c0", Parents: []graveler.CommitID{}}
				c2 := &graveler.Commit{Message: "c2", Parents: []graveler.CommitID{"c0"}}
				c1 := &graveler.Commit{Message: "c1", Parents: []graveler.CommitID{"c0", "c2"}}
				c3 := &graveler.Commit{Message: "c3", Parents: []graveler.CommitID{"c1"}}
				c4 := &graveler.Commit{Message: "c4", Parents: []graveler.CommitID{"c2"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4,
				})
			},
			Expected: []string{"c2"},
		},
		{
			Name:  "higher ancestor is closer on dag",
			Left:  "x",
			Right: "y",
			Getter: func() *MockCommitGetter {
				c1 := &graveler.Commit{Message: "c1", Parents: []graveler.CommitID{}}
				c2 := &graveler.Commit{Message: "c2", Parents: []graveler.CommitID{"c1"}}
				c3 := &graveler.Commit{Message: "c3", Parents: []graveler.CommitID{"c2"}}
				c4 := &graveler.Commit{Message: "c4", Parents: []graveler.CommitID{"c3"}}
				x := &graveler.Commit{Message: "x", Parents: []graveler.CommitID{"c4", "c1"}}
				y := &graveler.Commit{Message: "y", Parents: []graveler.CommitID{"c2"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c1": c1, "c2": c2, "c3": c3, "c4": c4, "x": x, "y": y,
				})
			},
			Expected: []string{"c2"},
		},
		{
			Name: "merges in history (from git core tests)",
			// E---D---C---B---A
			// \"-_         \   \
			//  \  `---------G   \
			//   \                \
			//    F----------------H
			Left:  "g",
			Right: "h",

			Getter: func() *MockCommitGetter {
				e := &graveler.Commit{Message: "e", Parents: []graveler.CommitID{}}
				d := &graveler.Commit{Message: "d", Parents: []graveler.CommitID{"e"}}
				f := &graveler.Commit{Message: "f", Parents: []graveler.CommitID{"e"}}
				c := &graveler.Commit{Message: "c", Parents: []graveler.CommitID{"d"}}
				b := &graveler.Commit{Message: "b", Parents: []graveler.CommitID{"c"}}
				a := &graveler.Commit{Message: "a", Parents: []graveler.CommitID{"b"}}
				g := &graveler.Commit{Message: "g", Parents: []graveler.CommitID{"b", "e"}}
				h := &graveler.Commit{Message: "h", Parents: []graveler.CommitID{"a", "f"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"e": e, "d": d, "f": f, "c": c, "b": b, "a": a, "g": g, "h": h,
				})
			},
			Expected: []string{"b"},
		},
		{
			Name:  "same_node",
			Left:  "c2",
			Right: "c2",
			Getter: func() *MockCommitGetter {
				c0 := &graveler.Commit{Message: "c0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "c1", Parents: []graveler.CommitID{"c0"}}
				c2 := &graveler.Commit{Message: "c2", Parents: []graveler.CommitID{"c0"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2,
				})
			},
			Expected: []string{"c2"},
		},
		{
			Name: "no redundant parent access",
			// ROOT---------R
			// \
			//  `---a---c---L
			//   \     /   /
			//    `---b---d
			//
			// Verifying the fix introduced with https://github.com/treeverse/lakeFS/pull/2968. The following commits tree
			// will generate multiple accesses to commit 'b' as it is a parent commit for both 'd' and 'c' and per BFS algo,
			// it will be reached via both paths before ROOT is reached from L.
			// The abovementioned fix eliminates that
			Left:  "l",
			Right: "r",
			Getter: func() *MockCommitGetter {
				root := &graveler.Commit{Message: "root", Parents: []graveler.CommitID{}}
				a := &graveler.Commit{Message: "a", Parents: []graveler.CommitID{"root"}}
				b := &graveler.Commit{Message: "b", Parents: []graveler.CommitID{"root"}}
				c := &graveler.Commit{Message: "c", Parents: []graveler.CommitID{"a", "b"}}
				d := &graveler.Commit{Message: "d", Parents: []graveler.CommitID{"b"}}
				l := &graveler.Commit{Message: "L", Parents: []graveler.CommitID{"c", "d"}}
				r := &graveler.Commit{Message: "R", Parents: []graveler.CommitID{"root"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"root": root, "a": a, "b": b, "c": c, "d": d, "l": l, "r": r,
				})
			},
			Expected: []string{"root"},
		},
		{
			Name: "complex graph with multiple merges and common ancestor in the middle",
			//              ---ROOT---
			//             /   /  \   \
			//            /   a    b   \
			//           /     \  /     \
			//          |       ab       |
			//          |     /    \     |
			//          |    /  /\  \    |
			//          |   /  /  \  \   |
			//           \ /  |    |  \ /
			//            l0  |    |  r0
			//            /\  /    \  /\
			//           /  l1      r1  \
			//           \  /\      /\  /
			//            l2  \    /  r2
			//            /\  /    \  /\
			//           /  l3      r3  \
			//           \  /\      /\  /
			//            l4  \    /  r4
			//            /\  /    \  /\
			//           /  l5      r6  \
			//           \  /\      /\  /
			//            l7  \    /  r7
			//            /\  /    \  /\
			//           /  l8      r8  \
			//           \  /\      /\  /
			//            l9  \    /  r9
			//             \  /    \  /
			//             LEFT    RIGHT
			Left:  "left",
			Right: "right",
			Getter: func() *MockCommitGetter {
				root := &graveler.Commit{Message: "root", Parents: []graveler.CommitID{}}
				a := &graveler.Commit{Message: "a", Parents: []graveler.CommitID{"root"}}
				b := &graveler.Commit{Message: "b", Parents: []graveler.CommitID{"root"}}
				ab := &graveler.Commit{Message: "ab", Parents: []graveler.CommitID{"a", "b"}}
				l0 := &graveler.Commit{Message: "l0", Parents: []graveler.CommitID{"root", "ab"}}
				l1 := &graveler.Commit{Message: "l1", Parents: []graveler.CommitID{"ab", "l0"}}
				l2 := &graveler.Commit{Message: "l2", Parents: []graveler.CommitID{"l0", "l1"}}
				l3 := &graveler.Commit{Message: "l3", Parents: []graveler.CommitID{"l1", "l2"}}
				l4 := &graveler.Commit{Message: "l4", Parents: []graveler.CommitID{"l2", "l3"}}
				l5 := &graveler.Commit{Message: "l5", Parents: []graveler.CommitID{"l3", "l4"}}
				l6 := &graveler.Commit{Message: "l6", Parents: []graveler.CommitID{"l4", "l5"}}
				l7 := &graveler.Commit{Message: "l7", Parents: []graveler.CommitID{"l5", "l6"}}
				l8 := &graveler.Commit{Message: "l8", Parents: []graveler.CommitID{"l6", "l7"}}
				l9 := &graveler.Commit{Message: "l9", Parents: []graveler.CommitID{"l7", "l8"}}
				left := &graveler.Commit{Message: "left", Parents: []graveler.CommitID{"l8", "l9"}}
				r0 := &graveler.Commit{Message: "r0", Parents: []graveler.CommitID{"root", "ab"}}
				r1 := &graveler.Commit{Message: "r1", Parents: []graveler.CommitID{"ab", "r0"}}
				r2 := &graveler.Commit{Message: "r2", Parents: []graveler.CommitID{"r0", "r1"}}
				r3 := &graveler.Commit{Message: "r3", Parents: []graveler.CommitID{"r1", "r2"}}
				r4 := &graveler.Commit{Message: "r4", Parents: []graveler.CommitID{"r2", "r3"}}
				r5 := &graveler.Commit{Message: "r5", Parents: []graveler.CommitID{"r3", "r4"}}
				r6 := &graveler.Commit{Message: "r6", Parents: []graveler.CommitID{"r4", "r5"}}
				r7 := &graveler.Commit{Message: "r7", Parents: []graveler.CommitID{"r5", "r6"}}
				r8 := &graveler.Commit{Message: "r8", Parents: []graveler.CommitID{"r6", "r7"}}
				r9 := &graveler.Commit{Message: "r9", Parents: []graveler.CommitID{"r7", "r8"}}
				right := &graveler.Commit{Message: "right", Parents: []graveler.CommitID{"r8", "r9"}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"root": root, "a": a, "b": b, "ab": ab,
					"l0": l0, "l1": l1, "l2": l2, "l3": l3, "l4": l4,
					"l5": l5, "l6": l6, "l7": l7, "l8": l8, "l9": l9,
					"r0": r0, "r1": r1, "r2": r2, "r3": r3, "r4": r4,
					"r5": r5, "r6": r6, "r7": r7, "r8": r8, "r9": r9,
					"right": right, "left": left,
				})
			},
			Expected: []string{"ab"},
		},
	}
	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			getter := cas.Getter()
			base, err := ref.FindMergeBase(context.Background(), getter, repository, cas.Left, cas.Right)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			verifyResult(t, base, cas.Expected, getter.visited)

			// flip right and left and expect the same result, reset visited to keep track of the second round visits
			getter.visited = map[graveler.CommitID]int{}
			base, err = ref.FindMergeBase(
				context.Background(), getter, repository, cas.Right, cas.Left)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			verifyResult(t, base, cas.Expected, getter.visited)
		})
	}
}

func TestGrid(t *testing.T) {
	// Construct the following grid, taken from https://github.com/git/git/blob/master/t/t6600-test-reach.sh
	//             (10,10)
	//            /       \
	//         (10,9)    (9,10)
	//        /     \   /      \
	//    (10,8)    (9,9)      (8,10)
	//   /     \    /   \      /    \
	//         ( continued...)
	//   \     /    \   /      \    /
	//    (3,1)     (2,2)      (1,3)
	//        \     /    \     /
	//         (2,1)      (2,1)
	//              \    /
	//              (1,1)
	grid := make([][]*graveler.Commit, 10)
	kv := make(map[graveler.CommitID]*graveler.Commit)
	for i := 0; i < 10; i++ {
		grid[i] = make([]*graveler.Commit, 10)
		for j := 0; j < 10; j++ {
			parents := make([]graveler.CommitID, 0, 2)
			if i > 0 {
				parents = append(parents, graveler.CommitID(fmt.Sprintf("%d-%d", i-1, j)))
			}
			if j > 0 {
				parents = append(parents, graveler.CommitID(fmt.Sprintf("%d-%d", i, j-1)))
			}
			grid[i][j] = &graveler.Commit{Message: fmt.Sprintf("%d-%d", i, j), Parents: parents}
			kv[graveler.CommitID(fmt.Sprintf("%d-%d", i, j))] = grid[i][j]
		}
	}
	getter := newReader(kv)
	c, err := ref.FindMergeBase(context.Background(), getter, repository, "7-4", "5-6")
	testutil.Must(t, err)
	verifyResult(t, c, []string{"5-4"}, getter.visited)

	getter.visited = map[graveler.CommitID]int{}
	c, err = ref.FindMergeBase(context.Background(), getter, repository, "1-2", "2-1")
	testutil.Must(t, err)
	verifyResult(t, c, []string{"1-1"}, getter.visited)

	getter.visited = map[graveler.CommitID]int{}
	c, err = ref.FindMergeBase(context.Background(), getter, repository, "0-9", "9-0")
	testutil.Must(t, err)
	verifyResult(t, c, []string{"0-0"}, getter.visited)

	getter.visited = map[graveler.CommitID]int{}
	c, err = ref.FindMergeBase(context.Background(), getter, repository, "6-9", "9-6")
	testutil.Must(t, err)
	verifyResult(t, c, []string{"6-6"}, getter.visited)
}

func verifyResult(t *testing.T, base *graveler.Commit, expected []string, visited map[graveler.CommitID]int) {
	if base == nil {
		if len(expected) != 0 {
			t.Fatalf("got nil result, expected %s", expected)
		}
		return
	}
	for id, numVisits := range visited {
		if string(id) == base.Message && numVisits > 2 {
			t.Fatalf("visited base commit %d, expected max 2 visits", numVisits)
		} else if string(id) != base.Message && numVisits > 1 {
			t.Fatalf("visited non-base commit %d, expected max 1 visit", numVisits)
		}
	}
	for _, expectedKey := range expected {
		if base.Message == expectedKey {
			return
		}
	}
	t.Fatalf("expected one of (%v) got (%v)", expected, base.Message)
}
