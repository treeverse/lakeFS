package dag_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/treeverse/lakefs/index/dag"

	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/index/model"
)

type MockCommitReader struct {
	kv map[string]*model.Commit
}

func (r *MockCommitReader) ReadCommit(addr string) (*model.Commit, error) {
	commit, ok := r.kv[addr]
	if !ok {
		return nil, db.ErrNotFound
	}
	return commit, nil
}

func newReader(kv map[string]*model.Commit) dag.CommitReader {
	return &MockCommitReader{kv}
}

func TestBfsScan(t *testing.T) {
	cases := []struct {
		Name     string
		Addr     string
		Reader   dag.CommitReader
		Expected []string
	}{
		{
			Name: "non_forking_graph",
			Addr: "9",
			Reader: newReader(map[string]*model.Commit{
				"9": &model.Commit{Parents: []string{"8"}, Address: "9"},
				"8": &model.Commit{Parents: []string{"7"}, Address: "8"},
				"7": &model.Commit{Parents: []string{"6"}, Address: "7"},
				"6": &model.Commit{Parents: []string{}, Address: "6"},
			}),
			Expected: []string{"9", "8", "7", "6"},
		},

		{
			Name: "forking_graph",
			Addr: "9",
			Reader: newReader(map[string]*model.Commit{
				"9": &model.Commit{Parents: []string{"8"}, Address: "9"},
				"8": &model.Commit{Parents: []string{"6", "7"}, Address: "8"},
				"7": &model.Commit{Parents: []string{"5"}, Address: "7"},
				"6": &model.Commit{Parents: []string{"3", "4"}, Address: "6"},
				"5": &model.Commit{Parents: []string{"2"}, Address: "5"},
				"4": &model.Commit{Parents: []string{"2"}, Address: "4"},
				"3": &model.Commit{Parents: []string{"1"}, Address: "3"},
				"2": &model.Commit{Parents: []string{"0"}, Address: "2"},
				"1": &model.Commit{Parents: []string{"0"}, Address: "1"},
				"0": &model.Commit{Parents: []string{}, Address: "0"},
			}),
			Expected: []string{"9", "8", "6", "7", "3", "4", "5", "1", "2", "0"},
		},

		{
			Name: "other_forking_graph_dont_travel_all",
			Addr: "8",
			Reader: newReader(map[string]*model.Commit{
				"9": &model.Commit{Parents: []string{"8"}, Address: "9"},
				"8": &model.Commit{Parents: []string{"6", "7"}, Address: "8"},
				"7": &model.Commit{Parents: []string{"5"}, Address: "7"},
				"6": &model.Commit{Parents: []string{"3", "4"}, Address: "6"},
				"5": &model.Commit{Parents: []string{"2"}, Address: "5"},
				"4": &model.Commit{Parents: []string{"2"}, Address: "4"},
				"3": &model.Commit{Parents: []string{"1"}, Address: "3"},
				"2": &model.Commit{Parents: []string{"0"}, Address: "2"},
				"1": &model.Commit{Parents: []string{"0"}, Address: "1"},
				"0": &model.Commit{Parents: []string{}, Address: "0"},
			}),
			Expected: []string{"8", "6", "7", "3", "4", "5", "1", "2", "0"},
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.Name, func(t *testing.T) {
			commits, err := dag.BfsScan(tcase.Reader, tcase.Addr)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, len(commits))
			for cid, commit := range commits {
				ids[cid] = commit.GetAddress()
			}
			if !reflect.DeepEqual(tcase.Expected, ids) {
				t.Fatalf("expected %v got %v", tcase.Expected, ids)
			}
		})
	}
}

func TestFindLowestCommonAncestor(t *testing.T) {
	cases := []struct {
		Name     string
		Left     string
		Right    string
		Reader   dag.CommitReader
		Expected []string
	}{
		{
			Name:  "root_match",
			Left:  "7",
			Right: "6",
			Reader: newReader(map[string]*model.Commit{
				"7": &model.Commit{Parents: []string{"5"}, Address: "7"},
				"6": &model.Commit{Parents: []string{"4"}, Address: "6"},
				"5": &model.Commit{Parents: []string{"3"}, Address: "5"},
				"4": &model.Commit{Parents: []string{"2"}, Address: "4"},
				"3": &model.Commit{Parents: []string{"1"}, Address: "3"},
				"2": &model.Commit{Parents: []string{"0"}, Address: "2"},
				"1": &model.Commit{Parents: []string{"0"}, Address: "1"},
				"0": &model.Commit{Parents: []string{}, Address: "0"},
			}),
			Expected: []string{"0"},
		},
		{
			Name:  "close_ancestor",
			Left:  "3",
			Right: "4",
			Reader: newReader(map[string]*model.Commit{
				"4": &model.Commit{Parents: []string{"2"}, Address: "4"},
				"3": &model.Commit{Parents: []string{"2"}, Address: "3"},
				"2": &model.Commit{Parents: []string{"1"}, Address: "2"},
				"1": &model.Commit{Parents: []string{"0"}, Address: "1"},
				"0": &model.Commit{Parents: []string{}, Address: "0"},
			}),
			Expected: []string{"2"},
		},
		{
			Name:  "criss_cross",
			Left:  "5",
			Right: "6",
			Reader: newReader(map[string]*model.Commit{
				"6": &model.Commit{Parents: []string{"4"}, Address: "6"},
				"5": &model.Commit{Parents: []string{"3"}, Address: "5"},
				"4": &model.Commit{Parents: []string{"1", "2"}, Address: "4"},
				"3": &model.Commit{Parents: []string{"1", "2"}, Address: "3"},
				"2": &model.Commit{Parents: []string{"0"}, Address: "2"},
				"1": &model.Commit{Parents: []string{"0"}, Address: "1"},
				"0": &model.Commit{Parents: []string{}, Address: "0"},
			}),
			Expected: []string{"1", "2"},
		},
		{
			Name:  "contained",
			Left:  "2",
			Right: "1",
			Reader: newReader(map[string]*model.Commit{
				"2": &model.Commit{Parents: []string{"1"}, Address: "2"},
				"1": &model.Commit{Parents: []string{"0"}, Address: "1"},
				"0": &model.Commit{Parents: []string{}, Address: "0"},
			}),
			Expected: []string{"1"},
		},
	}
	for _, tcase := range cases {
		t.Run(tcase.Name, func(t *testing.T) {
			candidates, err := dag.FindLowestCommonAncestor(tcase.Reader, tcase.Left, tcase.Right)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, len(candidates))
			for cid, commit := range candidates {
				ids[cid] = commit.GetAddress()
			}
			sort.Strings(ids)
			if !reflect.DeepEqual(ids, tcase.Expected) {
				t.Fatalf("expected %v got %v", tcase.Expected, ids)
			}
		})
	}
}
