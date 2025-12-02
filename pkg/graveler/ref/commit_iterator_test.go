package ref_test

import (
	"context"
	"errors"
	"github.com/go-test/deep"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
)

func TestCommitIterator(t *testing.T) {
	ctx := context.Background()
	baseTime, err := time.Parse(time.RFC3339, "1999-12-31T23:59:59Z")
	if err != nil {
		t.Fatal(err)
	}
	o := func(hours int) time.Time {
		return baseTime.Add(time.Duration(hours) * time.Hour)
	}
	// Message field will also be the commit ID.
	commits := []graveler.Commit{
		{Message: "c0", Parents: []graveler.CommitID{}, CreationDate: o(0)},
		{Message: "c1", Parents: []graveler.CommitID{"c0"}, CreationDate: o(4)},
		{Message: "c1.5", Parents: []graveler.CommitID{"c1"}, CreationDate: o(3)},
		{Message: "c2", Parents: []graveler.CommitID{"c0"}, CreationDate: o(2)},
		{Message: "c3", Parents: []graveler.CommitID{"c1", "c0"}, CreationDate: o(6)},
		{Message: "c4", Parents: []graveler.CommitID{"c2", "c3"}, CreationDate: o(8)},
		{Message: "c5", Parents: []graveler.CommitID{"c3"}, CreationDate: o(12)},
		{Message: "c6", Parents: []graveler.CommitID{"c4"}, CreationDate: o(10)},
		{Message: "c7", Parents: []graveler.CommitID{"c5"}, CreationDate: o(14)},
		{Message: "c8", Parents: []graveler.CommitID{"c4", "c5"}, CreationDate: o(16)},
		{Message: "c9", Parents: []graveler.CommitID{"c8", "c1.5"}, CreationDate: o(18)},
		{Message: "c10", Parents: []graveler.CommitID{"c1.5", "c9"}, CreationDate: o(20)},
		{Message: "c11", Parents: []graveler.CommitID{"c8", "c3", "c4", "c10", "c2", "c5"}, CreationDate: o(22)},
	}
	kv := make(map[graveler.CommitID]*graveler.Commit, len(commits))
	for idx := range commits {
		commit := commits[idx] // Copy commit to detach map from commits.
		kv[graveler.CommitID(commit.Message)] = &commit
	}
	var commitGetter ref.CommitGetter = newReader(kv)

	cases := []struct {
		Start       graveler.CommitID
		FirstParent bool
		Expected    []graveler.CommitID
		ExpectedErr error
	}{
		{"not-found", false, nil, graveler.ErrNotFound},
		{"c0", false, []graveler.CommitID{"c0"}, nil},
		{"c0", true, []graveler.CommitID{"c0"}, nil},
		{"c3", false, []graveler.CommitID{"c3", "c1", "c0"}, nil},
		{"c3", true, []graveler.CommitID{"c3", "c1", "c0"}, nil},
		{"c4", false, []graveler.CommitID{"c4", "c3", "c1", "c2", "c0"}, nil},
		{"c4", true, []graveler.CommitID{"c4", "c2", "c0"}, nil},
		{"c8", false, []graveler.CommitID{"c8", "c5", "c4", "c3", "c1", "c2", "c0"}, nil},
		{"c8", true, []graveler.CommitID{"c8", "c4", "c2", "c0"}, nil},
		{"c9", false, []graveler.CommitID{"c9", "c8", "c5", "c4", "c3", "c1", "c1.5", "c2", "c0"}, nil},
		{"c10", false, []graveler.CommitID{"c10", "c9", "c8", "c5", "c4", "c3", "c1", "c1.5", "c2", "c0"}, nil},
		{"c11", false, []graveler.CommitID{"c11", "c10", "c9", "c8", "c5", "c4", "c3", "c1", "c1.5", "c2", "c0"}, nil},
	}

	for _, tc := range cases {
		name := string(tc.Start)
		if tc.FirstParent {
			name += "_FirstOnly"
		}
		t.Run(name, func(t *testing.T) {
			iteratorConfig := &ref.CommitIteratorConfig{
				Repository:  &graveler.RepositoryRecord{RepositoryID: "abc"},
				Start:       tc.Start,
				FirstParent: tc.FirstParent,
				Manager:     commitGetter,
				Since:       nil,
			}
			it := ref.NewCommitIterator(ctx, iteratorConfig)
			got := make([]graveler.CommitID, 0, len(tc.Expected))
			for it.Next() {
				value := it.Value()
				got = append(got, value.CommitID)
			}
			err := it.Err()
			if err != nil {
				if !errors.Is(err, tc.ExpectedErr) {
					t.Errorf("Got error %s != %s", err, tc.ExpectedErr)
				}
			} else {
				if tc.ExpectedErr != nil {
					t.Errorf("Succeeded when expecting error %s", tc.ExpectedErr)
				}
				if diffs := deep.Equal(got, tc.Expected); diffs != nil {
					t.Errorf("Different ordering.\n\tGot\t%s\n\tExpected\t%s\n\t%s",
						got, tc.Expected, diffs)
				}
			}
		})
	}
}
