package ref_test

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/ident"
)

type MockCommitGetter struct {
	kv      map[graveler.CommitID]*graveler.Commit
	visited map[graveler.CommitID]interface{}
}

func (g *MockCommitGetter) GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error) {
	for _, v := range g.kv {
		if caddr(v) == commitID {
			g.visited[commitID] = struct{}{}
			return v, nil
		}
	}
	return nil, graveler.ErrNotFound
}

func newReader(kv map[graveler.CommitID]*graveler.Commit) *MockCommitGetter {
	return &MockCommitGetter{
		kv:      kv,
		visited: make(map[graveler.CommitID]interface{}),
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
		Left     graveler.CommitID
		Right    graveler.CommitID
		Getter   func() *MockCommitGetter
		Expected graveler.CommitID
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
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4, "c5": c5, "c6": c6, "c7": c7,
				})
			},
			Expected: "c0",
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
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4,
				})
			},
			Expected: "c2",
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
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4, "c5": c5, "c6": c6,
				})
			},
			Expected: "c1",
		},
		{
			Name:  "contained",
			Left:  "c2",
			Right: "c1",
			Getter: func() *MockCommitGetter {
				c0 := &graveler.Commit{Message: "0", Parents: []graveler.CommitID{}}
				c1 := &graveler.Commit{Message: "1", Parents: []graveler.CommitID{caddr(c0)}}
				c2 := &graveler.Commit{Message: "2", Parents: []graveler.CommitID{caddr(c1)}}
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2,
				})
			},
			Expected: "c1",
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
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4, "c5": c5, "c6": c6, "c7": c7,
				})
			},
			Expected: "",
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
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4,
				})
			},
			Expected: "c2",
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
				return newReader(map[graveler.CommitID]*graveler.Commit{
					"c1": c1, "c2": c2, "c3": c3, "c4": c4, "x": x, "y": y,
				})
			},
			Expected: "c2",
		},
	}
	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			getter := cas.Getter()
			base := ref.FindLowestCommonAncestor(
				context.Background(), getter, ident.NewHexAddressProvider(), "", caddr(getter.kv[cas.Left]), caddr(getter.kv[cas.Right]))
			verifyResult(t, base, getter, cas.Expected)

			// flip right and left and expect the same result
			base = ref.FindLowestCommonAncestor(
				context.Background(), getter, ident.NewHexAddressProvider(), "", caddr(getter.kv[cas.Right]), caddr(getter.kv[cas.Left]))
			verifyResult(t, base, getter, cas.Expected)

		})
	}
}

func verifyResult(t *testing.T, base *graveler.Commit, getter *MockCommitGetter, expected graveler.CommitID) {
	var addr graveler.CommitID
	if base != nil {
		addr = caddr(base)
	}
	if addr != caddr(getter.kv[expected]) {
		key := "unknown"
		for k, v := range getter.kv {
			if addr == caddr(v) {
				key = string(k)
				break
			}
		}
		t.Fatalf("expected %v (%v) got %v (%v)", expected, caddr(getter.kv[expected]), key, addr)
	}
}
