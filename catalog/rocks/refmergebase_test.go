package rocks_test

import (
	"context"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/ident"
	"testing"
)

type MockCommitGetter struct {
	kv      map[rocks.CommitID]*rocks.Commit
	visited map[rocks.CommitID]interface{}
}

func (g *MockCommitGetter) GetCommit(ctx context.Context, repositoryID rocks.RepositoryID, commitID rocks.CommitID) (*rocks.Commit, error) {
	for _, v := range g.kv {
		if caddr(v) == commitID {
			g.visited[commitID] = struct{}{}
			return v, nil
		}
	}
	return nil, rocks.ErrNotFound
}

func newReader(kv map[rocks.CommitID]*rocks.Commit) *MockCommitGetter {
	return &MockCommitGetter{
		kv:      kv,
		visited: make(map[rocks.CommitID]interface{}),
	}
}

func caddr(commit *rocks.Commit) rocks.CommitID {
	if commit == nil {
		return rocks.CommitID("")
	}
	return rocks.CommitID(ident.ContentAddress(commit))
}

func TestFindLowestCommonAncestor(t *testing.T) {
	cases := []struct {
		Name            string
		Left            rocks.CommitID
		Right           rocks.CommitID
		Getter          func() *MockCommitGetter
		Expected        rocks.CommitID
		NoVisitExpected []rocks.CommitID
	}{
		{
			Name:  "root_match",
			Left:  "c7",
			Right: "c6",
			Getter: func() *MockCommitGetter {
				c0 := &rocks.Commit{Message: "0", Parents: []rocks.CommitID{}}
				c1 := &rocks.Commit{Message: "1", Parents: []rocks.CommitID{caddr(c0)}}
				c2 := &rocks.Commit{Message: "2", Parents: []rocks.CommitID{caddr(c0)}}
				c3 := &rocks.Commit{Message: "3", Parents: []rocks.CommitID{caddr(c1)}}
				c4 := &rocks.Commit{Message: "4", Parents: []rocks.CommitID{caddr(c2)}}
				c5 := &rocks.Commit{Message: "5", Parents: []rocks.CommitID{caddr(c3)}}
				c6 := &rocks.Commit{Message: "6", Parents: []rocks.CommitID{caddr(c4)}}
				c7 := &rocks.Commit{Message: "7", Parents: []rocks.CommitID{caddr(c5)}}
				return newReader(map[rocks.CommitID]*rocks.Commit{
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
				c0 := &rocks.Commit{Message: "0", Parents: []rocks.CommitID{}}
				c1 := &rocks.Commit{Message: "1", Parents: []rocks.CommitID{caddr(c0)}}
				c2 := &rocks.Commit{Message: "2", Parents: []rocks.CommitID{caddr(c1)}}
				c3 := &rocks.Commit{Message: "3", Parents: []rocks.CommitID{caddr(c2)}}
				c4 := &rocks.Commit{Message: "4", Parents: []rocks.CommitID{caddr(c2)}}
				return newReader(map[rocks.CommitID]*rocks.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4,
				})
			},
			Expected:        "c2",
			NoVisitExpected: []rocks.CommitID{"c0"},
		},
		{
			Name:  "criss_cross",
			Left:  "c5",
			Right: "c6",
			Getter: func() *MockCommitGetter {
				c0 := &rocks.Commit{Message: "0", Parents: []rocks.CommitID{}}
				c1 := &rocks.Commit{Message: "1", Parents: []rocks.CommitID{caddr(c0)}}
				c2 := &rocks.Commit{Message: "2", Parents: []rocks.CommitID{caddr(c0)}}
				c3 := &rocks.Commit{Message: "3", Parents: []rocks.CommitID{caddr(c1), caddr(c2)}}
				c4 := &rocks.Commit{Message: "4", Parents: []rocks.CommitID{caddr(c1), caddr(c2)}}
				c5 := &rocks.Commit{Message: "5", Parents: []rocks.CommitID{caddr(c3)}}
				c6 := &rocks.Commit{Message: "6", Parents: []rocks.CommitID{caddr(c4)}}
				return newReader(map[rocks.CommitID]*rocks.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4, "c5": c5, "c6": c6,
				})
			},
			Expected:        "c1",
			NoVisitExpected: []rocks.CommitID{"c0"},
		},
		{
			Name:  "contained",
			Left:  "c2",
			Right: "c1",
			Getter: func() *MockCommitGetter {
				c0 := &rocks.Commit{Message: "0", Parents: []rocks.CommitID{}}
				c1 := &rocks.Commit{Message: "1", Parents: []rocks.CommitID{caddr(c0)}}
				c2 := &rocks.Commit{Message: "2", Parents: []rocks.CommitID{caddr(c1)}}
				return newReader(map[rocks.CommitID]*rocks.Commit{
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
				c0 := &rocks.Commit{Message: "0", Parents: []rocks.CommitID{}}
				c1 := &rocks.Commit{Message: "1", Parents: []rocks.CommitID{caddr(c0)}}
				c2 := &rocks.Commit{Message: "2", Parents: []rocks.CommitID{caddr(c1)}}
				c3 := &rocks.Commit{Message: "3", Parents: []rocks.CommitID{caddr(c2)}}
				c4 := &rocks.Commit{Message: "4", Parents: []rocks.CommitID{}}
				c5 := &rocks.Commit{Message: "5", Parents: []rocks.CommitID{caddr(c4)}}
				c6 := &rocks.Commit{Message: "6", Parents: []rocks.CommitID{caddr(c5)}}
				c7 := &rocks.Commit{Message: "7", Parents: []rocks.CommitID{caddr(c6)}}
				return newReader(map[rocks.CommitID]*rocks.Commit{
					"c0": c0, "c1": c1, "c2": c2, "c3": c3, "c4": c4, "c5": c5, "c6": c6, "c7": c7,
				})
			},
			Expected: "",
		},
	}
	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			getter := cas.Getter()
			base, err := rocks.FindLowestCommonAncestor(
				context.Background(), getter, "", caddr(getter.kv[cas.Left]), caddr(getter.kv[cas.Right]))
			if err != nil {
				t.Fatal(err)
			}
			var addr rocks.CommitID
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
