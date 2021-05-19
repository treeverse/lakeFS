package ref_test

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/ident"
)

type MockCommitGetter struct {
	byHumanID  map[string]*graveler.Commit
	byCommitID map[graveler.CommitID]*graveler.Commit
	visited    map[graveler.CommitID]interface{}
}

func (g *MockCommitGetter) GetCommit(_ context.Context, _ graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error) {
	if commit, ok := g.byCommitID[commitID]; ok {
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

func newReader(kv map[string]*graveler.Commit) *MockCommitGetter {
	byCommitID := make(map[graveler.CommitID]*graveler.Commit)
	for _, commit := range kv {
		byCommitID[caddr(commit)] = commit
	}
	for _, v := range kv {
		v.Generation = computeGeneration(byCommitID, v)
	}
	return &MockCommitGetter{
		byHumanID:  kv,
		byCommitID: byCommitID,
		visited:    make(map[graveler.CommitID]interface{}),
	}

}

func caddr(commit *graveler.Commit) graveler.CommitID {
	if commit == nil {
		return ""
	}
	return graveler.CommitID(ident.NewHexAddressProvider().ContentAddress(commit))
}

func TestFindLowestCommonAncestor(t *testing.T) {
	cases := []struct {
		Name     string
		Left     string
		Right    string
		Getter   func() *MockCommitGetter
		Expected []string
	}{
		{
			Name:  "root_match",
			Left:  "c7",
			Right: "c6",
			Getter: func() *MockCommitGetter {
				c0 := &graveler.Commit{Message: "0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "1", Parents: []graveler.CommitID{caddr(c0)}}
				c2 := &graveler.Commit{Message: "2", Parents: []graveler.CommitID{caddr(c0)}}
				c3 := &graveler.Commit{Message: "3", Parents: []graveler.CommitID{caddr(c1)}}
				c4 := &graveler.Commit{Message: "4", Parents: []graveler.CommitID{caddr(c2)}}
				c5 := &graveler.Commit{Message: "5", Parents: []graveler.CommitID{caddr(c3)}}
				c6 := &graveler.Commit{Message: "6", Parents: []graveler.CommitID{caddr(c4)}}
				c7 := &graveler.Commit{Message: "7", Parents: []graveler.CommitID{caddr(c5)}}
				return newReader(map[string]*graveler.Commit{
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
				c0 := &graveler.Commit{Message: "0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "1", Parents: []graveler.CommitID{caddr(c0)}}
				c2 := &graveler.Commit{Message: "2", Parents: []graveler.CommitID{caddr(c1)}}
				c3 := &graveler.Commit{Message: "3", Parents: []graveler.CommitID{caddr(c2)}}
				c4 := &graveler.Commit{Message: "4", Parents: []graveler.CommitID{caddr(c2)}}
				return newReader(map[string]*graveler.Commit{
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
				c0 := &graveler.Commit{Message: "0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "1", Parents: []graveler.CommitID{caddr(c0)}}
				c2 := &graveler.Commit{Message: "2", Parents: []graveler.CommitID{caddr(c0)}}
				c3 := &graveler.Commit{Message: "3", Parents: []graveler.CommitID{caddr(c1), caddr(c2)}}
				c4 := &graveler.Commit{Message: "4", Parents: []graveler.CommitID{caddr(c1), caddr(c2)}}
				c5 := &graveler.Commit{Message: "5", Parents: []graveler.CommitID{caddr(c3)}}
				c6 := &graveler.Commit{Message: "6", Parents: []graveler.CommitID{caddr(c4)}}
				return newReader(map[string]*graveler.Commit{
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
				c0 := &graveler.Commit{Message: "0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "1", Parents: []graveler.CommitID{caddr(c0)}}
				c2 := &graveler.Commit{Message: "2", Parents: []graveler.CommitID{caddr(c1)}}
				return newReader(map[string]*graveler.Commit{
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
				c0 := &graveler.Commit{Message: "0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "1", Parents: []graveler.CommitID{caddr(c0)}}
				c2 := &graveler.Commit{Message: "2", Parents: []graveler.CommitID{caddr(c1)}}
				c3 := &graveler.Commit{Message: "3", Parents: []graveler.CommitID{caddr(c2)}}
				c4 := &graveler.Commit{Message: "4", Parents: []graveler.CommitID{}}
				c5 := &graveler.Commit{Message: "5", Parents: []graveler.CommitID{caddr(c4)}}
				c6 := &graveler.Commit{Message: "6", Parents: []graveler.CommitID{caddr(c5)}}
				c7 := &graveler.Commit{Message: "7", Parents: []graveler.CommitID{caddr(c6)}}
				return newReader(map[string]*graveler.Commit{
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
				c0 := &graveler.Commit{Message: "0", Parents: []graveler.CommitID{}}
				c2 := &graveler.Commit{Message: "2", Parents: []graveler.CommitID{caddr(c0)}}
				c1 := &graveler.Commit{Message: "1", Parents: []graveler.CommitID{caddr(c0), caddr(c2)}}
				c3 := &graveler.Commit{Message: "3", Parents: []graveler.CommitID{caddr(c1)}}
				c4 := &graveler.Commit{Message: "5", Parents: []graveler.CommitID{caddr(c2)}}
				return newReader(map[string]*graveler.Commit{
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
				c1 := &graveler.Commit{Message: "1", Parents: []graveler.CommitID{}}
				c2 := &graveler.Commit{Message: "2", Parents: []graveler.CommitID{caddr(c1)}}
				c3 := &graveler.Commit{Message: "3", Parents: []graveler.CommitID{caddr(c2)}}
				c4 := &graveler.Commit{Message: "4", Parents: []graveler.CommitID{caddr(c3)}}
				x := &graveler.Commit{Message: "x", Parents: []graveler.CommitID{caddr(c4), caddr(c1)}}
				y := &graveler.Commit{Message: "y", Parents: []graveler.CommitID{caddr(c2)}}
				return newReader(map[string]*graveler.Commit{
					"c1": c1, "c2": c2, "c3": c3, "c4": c4, "x": x, "y": y,
				})
			},
			Expected: []string{"c2"},
		},
	}
	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			getter := cas.Getter()
			base, err := ref.FindLowestCommonAncestor(
				context.Background(), getter, "", caddr(getter.byHumanID[cas.Left]), caddr(getter.byHumanID[cas.Right]))
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			verifyResult(t, base, getter, cas.Expected)

			// flip right and left and expect the same result
			base, err = ref.FindLowestCommonAncestor(
				context.Background(), getter, "", caddr(getter.byHumanID[cas.Right]), caddr(getter.byHumanID[cas.Left]))
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			verifyResult(t, base, getter, cas.Expected)

		})
	}
}

func verifyResult(t *testing.T, base *graveler.Commit, getter *MockCommitGetter, expected []string) {
	var addr graveler.CommitID
	if base == nil {
		if len(expected) != 0 {
			t.Fatalf("got nil result, expected %s", expected)
		}
		return
	}
	addr = caddr(base)
	expectedCommitIDs := make([]string, 0, len(expected))
	for _, expectedKey := range expected {
		expectedCommitIDs = append(expectedCommitIDs, expectedKey)
		if caddr(getter.byHumanID[expectedKey]) == addr {
			return
		}
	}
	t.Fatalf("expected one of (%v) got (%v)", expectedCommitIDs, addr)
}
