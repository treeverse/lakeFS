package ref_test

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/ref"
	"github.com/treeverse/lakefs/ident"
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
		Name            string
		Left            graveler.CommitID
		Right           graveler.CommitID
		Getter          func() *MockCommitGetter
		Expected        graveler.CommitID
		NoVisitExpected []graveler.CommitID
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
			Expected:        "c2",
			NoVisitExpected: []graveler.CommitID{"c0"},
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
			Expected:        "c1",
			NoVisitExpected: []graveler.CommitID{"c0"},
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
	}
	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			getter := cas.Getter()
			base, err := ref.FindLowestCommonAncestor(
				context.Background(), getter, ident.NewHexAddressProvider(), "", caddr(getter.kv[cas.Left]), caddr(getter.kv[cas.Right]))
			if err != nil {
				t.Fatal(err)
			}
			var addr graveler.CommitID
			if base != nil {
				addr = caddr(base)
			}
			if addr != caddr(getter.kv[cas.Expected]) {
				key := "unknown"
				for k, v := range getter.kv {
					if addr == caddr(v) {
						key = string(k)
						break
					}
				}
				t.Fatalf("expected %v (%v) got %v (%v)", cas.Expected, caddr(getter.kv[cas.Expected]), key, addr)
			}

			//check efficiency i.e check that we didn't iterate over unnecessary nodes
			for _, addr := range cas.NoVisitExpected {
				if getter.visited[addr] != nil {
					t.Fatalf("commit %s should not be visited", addr)
				}
			}
		})
	}
}
