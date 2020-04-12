package dag_test

import (
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/index/dag"

	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/index/model"
)

type MockCommitReader struct {
	kv      map[string]*model.Commit
	visited map[string]interface{}
}

func (r *MockCommitReader) ReadCommit(addr string) (*model.Commit, error) {
	var sentinel = struct{}{}
	commit, ok := r.kv[addr]
	if !ok {
		return nil, db.ErrNotFound
	}
	r.visited[addr] = sentinel
	return commit, nil
}

func newReader(kv map[string]*model.Commit) dag.CommitReader {
	visited := make(map[string]interface{})
	return &MockCommitReader{kv, visited}
}

func TestBfsScan(t *testing.T) {
	cases := []struct {
		Name            string
		Addr            string
		results         int
		after           string
		Reader          dag.CommitReader
		Expected        []string
		ExpectedHasMore bool
	}{
		{
			Name: "non_forking_graph",
			Addr: "9",
			Reader: newReader(map[string]*model.Commit{
				"9": {Parents: []string{"8"}, Address: "9"},
				"8": {Parents: []string{"7"}, Address: "8"},
				"7": {Parents: []string{"6"}, Address: "7"},
				"6": {Parents: []string{}, Address: "6"},
			}),
			Expected: []string{"9", "8", "7", "6"},
		},
		{
			Name:    "non_forking_graph_2_results",
			Addr:    "9",
			results: 2,
			Reader: newReader(map[string]*model.Commit{
				"9": {Parents: []string{"8"}, Address: "9"},
				"8": {Parents: []string{"7"}, Address: "8"},
				"7": {Parents: []string{"6"}, Address: "7"},
				"6": {Parents: []string{}, Address: "6"},
			}),
			Expected:        []string{"9", "8"},
			ExpectedHasMore: true,
		},
		{
			Name:  "non_forking_graph_after_8",
			Addr:  "9",
			after: "8",
			Reader: newReader(map[string]*model.Commit{
				"9": {Parents: []string{"8"}, Address: "9"},
				"8": {Parents: []string{"7"}, Address: "8"},
				"7": {Parents: []string{"6"}, Address: "7"},
				"6": {Parents: []string{}, Address: "6"},
			}),
			Expected: []string{"7", "6"},
		},

		{
			Name: "forking_graph",
			Addr: "9",
			Reader: newReader(map[string]*model.Commit{
				"9": {Parents: []string{"8"}, Address: "9"},
				"8": {Parents: []string{"6", "7"}, Address: "8"},
				"7": {Parents: []string{"5"}, Address: "7"},
				"6": {Parents: []string{"3", "4"}, Address: "6"},
				"5": {Parents: []string{"2"}, Address: "5"},
				"4": {Parents: []string{"2"}, Address: "4"},
				"3": {Parents: []string{"1"}, Address: "3"},
				"2": {Parents: []string{"0"}, Address: "2"},
				"1": {Parents: []string{"0"}, Address: "1"},
				"0": {Parents: []string{}, Address: "0"},
			}),
			Expected: []string{"9", "8", "6", "7", "3", "4", "5", "1", "2", "0"},
		},

		{
			Name: "other_forking_graph_dont_travel_all",
			Addr: "8",
			Reader: newReader(map[string]*model.Commit{
				"9": {Parents: []string{"8"}, Address: "9"},
				"8": {Parents: []string{"6", "7"}, Address: "8"},
				"7": {Parents: []string{"5"}, Address: "7"},
				"6": {Parents: []string{"3", "4"}, Address: "6"},
				"5": {Parents: []string{"2"}, Address: "5"},
				"4": {Parents: []string{"2"}, Address: "4"},
				"3": {Parents: []string{"1"}, Address: "3"},
				"2": {Parents: []string{"0"}, Address: "2"},
				"1": {Parents: []string{"0"}, Address: "1"},
				"0": {Parents: []string{}, Address: "0"},
			}),
			Expected: []string{"8", "6", "7", "3", "4", "5", "1", "2", "0"},
		},
		{
			Name:    "forking_graph_after_brother", //i.e paginate starts from a node such that the next expected node is his brother (no connection between them)
			Addr:    "6",
			after:   "3",
			results: 2,
			Reader: newReader(map[string]*model.Commit{
				"6": {Parents: []string{"4", "5"}, Address: "6"},
				"5": {Parents: []string{"1"}, Address: "5"},
				"4": {Parents: []string{"3", "2"}, Address: "4"},
				"3": {Parents: []string{"0"}, Address: "3"},
				"2": {Parents: []string{"0"}, Address: "2"},
				"1": {Parents: []string{"0"}, Address: "1"},
				"0": {Parents: []string{}, Address: "0"},
			}),
			Expected:        []string{"2", "1"},
			ExpectedHasMore: true,
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.Name, func(t *testing.T) {
			commits, hasMore, err := dag.BfsScan(tcase.Reader, tcase.Addr, tcase.results, tcase.after)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, len(commits))
			for cid, commit := range commits {
				ids[cid] = commit.Address
			}
			if !reflect.DeepEqual(tcase.Expected, ids) {
				t.Fatalf("expected %v got %v", tcase.Expected, ids)
			}
			if tcase.ExpectedHasMore != hasMore {
				t.Fatalf("hasMore excetption : expected %v got %v", tcase.Expected, ids)
			}
		})
	}
}

func TestFindLowestCommonAncestor(t *testing.T) {
	cases := []struct {
		Name            string
		Left            string
		Right           string
		Reader          dag.CommitReader
		Expected        string
		NoVisitExpected []string
	}{
		{
			Name:  "root_match",
			Left:  "7",
			Right: "6",
			Reader: newReader(map[string]*model.Commit{
				"7": {Parents: []string{"5"}, Address: "7"},
				"6": {Parents: []string{"4"}, Address: "6"},
				"5": {Parents: []string{"3"}, Address: "5"},
				"4": {Parents: []string{"2"}, Address: "4"},
				"3": {Parents: []string{"1"}, Address: "3"},
				"2": {Parents: []string{"0"}, Address: "2"},
				"1": {Parents: []string{"0"}, Address: "1"},
				"0": {Parents: []string{}, Address: "0"},
			}),
			Expected: "0",
		},
		{
			Name:  "close_ancestor",
			Left:  "3",
			Right: "4",
			Reader: newReader(map[string]*model.Commit{
				"4": {Parents: []string{"2"}, Address: "4"},
				"3": {Parents: []string{"2"}, Address: "3"},
				"2": {Parents: []string{"1"}, Address: "2"},
				"1": {Parents: []string{"0"}, Address: "1"},
				"0": {Parents: []string{}, Address: "0"},
			}),
			Expected:        "2",
			NoVisitExpected: []string{"0"},
		},
		{
			Name:  "criss_cross",
			Left:  "5",
			Right: "6",
			Reader: newReader(map[string]*model.Commit{
				"6": {Parents: []string{"4"}, Address: "6"},
				"5": {Parents: []string{"3"}, Address: "5"},
				"4": {Parents: []string{"1", "2"}, Address: "4"},
				"3": {Parents: []string{"1", "2"}, Address: "3"},
				"2": {Parents: []string{"0"}, Address: "2"},
				"1": {Parents: []string{"0"}, Address: "1"},
				"0": {Parents: []string{}, Address: "0"},
			}),
			Expected:        "1",
			NoVisitExpected: []string{"0"},
		},
		{
			Name:  "contained",
			Left:  "2",
			Right: "1",
			Reader: newReader(map[string]*model.Commit{
				"2": {Parents: []string{"1"}, Address: "2"},
				"1": {Parents: []string{"0"}, Address: "1"},
				"0": {Parents: []string{}, Address: "0"},
			}),
			Expected: "1",
		},
		{
			Name:  "parallel",
			Left:  "7",
			Right: "3",
			Reader: newReader(map[string]*model.Commit{
				"7": {Parents: []string{"6"}, Address: "7"},
				"6": {Parents: []string{"5"}, Address: "6"},
				"5": {Parents: []string{"4"}, Address: "5"},
				"4": {Parents: []string{}, Address: "4"},
				"3": {Parents: []string{"2"}, Address: "3"},
				"2": {Parents: []string{"1"}, Address: "2"},
				"1": {Parents: []string{"0"}, Address: "1"},
				"0": {Parents: []string{}, Address: "0"},
			}),
			Expected: "",
		},
	}
	for _, tcase := range cases {
		t.Run(tcase.Name, func(t *testing.T) {
			lca, err := dag.FindLowestCommonAncestor(tcase.Reader, tcase.Left, tcase.Right)
			if err != nil {
				t.Fatal(err)
			}
			addr := ""
			if lca != nil {
				addr = lca.Address
			}
			if addr != tcase.Expected {
				t.Fatalf("expected %v got %v", tcase.Expected, addr)
			}

			//check efficiency i.e check that we didn't iterate over unnecessary nodes
			for _, addr := range tcase.NoVisitExpected {
				if tcase.Reader.(*MockCommitReader).visited[addr] != nil {
					t.Fatalf("commit %s should not be visited", addr)
				}
			}
		})
	}
}
