package index_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/treeverse/lakefs/index/errors"
	"github.com/treeverse/lakefs/index/merkle"
	"github.com/treeverse/lakefs/testutil"
)

func TestMerge(t *testing.T) {
	metadata := make(map[string]string)
	metadata["property-1"] = "value-1"

	t.Run("simplest merge", func(t *testing.T) {
		deps := getDependencies(t)
		index := deps.meta
		uploadObject(t, deps, "t/v/s", "master", 1024)
		firstCommit := testCommit(t, index, "master", "master-1")
		createBranch(t, index, "br-1", "master")

		uploadObject(t, deps, "t/v1/s", "master", 10000)
		secondCommit := testCommit(t, index, "master", "master-2")
		diffs, err := index.Merge(REPO, "master", "br-1", "user-1")
		if err != nil {
			t.Error("merge unexpected error", err)
		}
		expectedCreated, expectedUpdated, expectedRemoved := 1, 0, 0
		createdCount, updatedCount, removedCount := countSuccess(diffs)
		if createdCount != expectedCreated ||
			removedCount != expectedRemoved ||
			updatedCount != expectedUpdated {
			t.Errorf("success counters wrong added %d, changed %d, removed %d - expected (%d, %d, %d)",
				createdCount, updatedCount, removedCount,
				expectedCreated, expectedUpdated, expectedRemoved)
		}
		_ = getObject(t, index, REPO, "br-1", "t/v1/s", true, "merge failed - document not copied")

		// verify that merge commit record holds the right parents
		branch, err := index.GetBranch(REPO, "br-1")
		testutil.Must(t, err)
		// access merge commit
		mergeCommit, err := index.GetCommit(REPO, branch.CommitId)
		testutil.Must(t, err)
		// verify that we have two parents
		expectedParentsLen := 2
		if len(mergeCommit.Parents) != expectedParentsLen {
			t.Errorf("merge commit to have %d parent(s), expected %d", len(mergeCommit.Parents), expectedParentsLen)
		}
		// verify parents addresses based on merged commits
		sort.Strings(mergeCommit.Parents)
		for _, address := range []string{firstCommit.Address, secondCommit.Address} {
			i := sort.SearchStrings(mergeCommit.Parents, address)
			if i == len(mergeCommit.Parents) || mergeCommit.Parents[i] != address {
				t.Errorf("merge commit parents %v expected to hold %s address", mergeCommit.Parents, address)
			}
		}
		// remove last file (v1) and verify that diff will show it was removed
		err = index.DeleteObject(REPO, "master", "t/v1/s")
		testutil.Must(t, err)
		_ = testCommit(t, index, "master", "master-3")
		diff, err := index.Diff(REPO, "master", "br-1")
		testutil.Must(t, err)
		if len(diff) != 1 {
			t.Fatalf("Diff should show %d change, expected 1", len(diff))
		}
		diffChange := diff[0].String()
		expectedDiffChange := "<-D t/v1/"
		if diffChange != expectedDiffChange {
			t.Errorf("Diff change >%s< expected >%s<", diffChange, expectedDiffChange)
		}
	})

	t.Run("merge with remove", func(t *testing.T) {
		deps := getDependencies(t)
		index := deps.meta
		uploadObject(t, deps, "t/v/s", "master", 1024)
		uploadObject(t, deps, "t/v/s1", "master", 2048)
		testCommit(t, index, "master", "master-1")
		createBranch(t, index, "br-1", "master")
		testCommit(t, index, "br-1", "br-1-1")
		err := index.DeleteObject(REPO, "master", "t/v/s")
		if err != nil {
			t.Error("could not delete object\n")
		}
		testCommit(t, index, "master", "master-2")

		diffs, err := index.Merge(REPO, "master", "br-1", "user-1")
		if err != nil {
			t.Error("unexpected error:", err)
		}
		createdCount, updatedCount, removedCount := countSuccess(diffs)
		if createdCount != 0 ||
			removedCount != 1 ||
			updatedCount != 0 {
			t.Errorf("success counters wrong added %d, changed %d, removed %d\n ", createdCount, updatedCount, removedCount)
		}
		_ = getObject(t, index, REPO, "br-1", "t/v/s", false, "merge failed - document not deleted")
	})

	t.Run("merge with conflict", func(t *testing.T) {
		deps := getDependencies(t)
		index := deps.meta
		uploadObject(t, deps, "t/v/s", "master", 1024)
		testCommit(t, index, "master", "master-1")
		createBranch(t, index, "br-1", "master")
		testCommit(t, index, "br-1", "br-1-1")

		uploadObject(t, deps, "t/v/s1", "master", 10000)
		uploadObject(t, deps, "t/v/s1", "br-1", 5000)
		testCommit(t, index, "br-1", "br-1-1")
		testCommit(t, index, "master", "master-2")

		diffs, err := index.Merge(REPO, "master", "br-1", "user-1")
		if err != errors.ErrMergeConflict {
			t.Error("did not identify conflict  ", err)
		}

		if diffs == nil {
			t.Error("no conflict result")
		} else {
			z := (diffs)[0]
			if z.Type != 5 || z.Direction != 2 {
				t.Error("incorrect conflict values", z)
			}
		}
	})

	t.Run("large tree", func(t *testing.T) {
		deps := getDependencies(t)
		index := deps.meta
		uploadTree(t, deps, "master", "base", []string{"lva", "lvb"}, []int{0, 0}, []int{10, 10}, 4096)
		testCommit(t, index, "master", "master-1")
		createBranch(t, index, "br-1", "master")
		testCommit(t, index, "br-1", "br-1-1")
		uploadTree(t, deps, "br-1", "base", []string{"lva", "lvb"}, []int{0, 0}, []int{10, 10}, 5020)
		uploadTree(t, deps, "master", "base", []string{"lv1", "lv2"}, []int{10, 0}, []int{10, 10}, 5020)
		testCommit(t, index, "br-1", "br-1-2")
		diffs, err := index.Merge(REPO, "master", "br-1", "user-1")
		if err != nil {
			t.Error("unexpected error:", err)
		}
		createdCount, updatedCount, removedCount := countSuccess(diffs)
		if createdCount != 0 ||
			removedCount != 0 ||
			updatedCount != 0 {
			t.Errorf("success counters wrong added %d, changed %d, removed %d\n ", createdCount, updatedCount, removedCount)
		}
	})

	t.Run("large tree with large addition", func(t *testing.T) {
		deps := getDependencies(t)
		index := deps.meta
		uploadTree(t, deps, "master", "base", []string{"lva", "lvb"}, []int{0, 0}, []int{10, 10}, 4096)
		testCommit(t, index, "master", "master-1")
		createBranch(t, index, "br-1", "master")
		testCommit(t, index, "br-1", "br-1-1")
		uploadTree(t, deps, "br-1", "base", []string{"lva", "lvb"}, []int{0, 0}, []int{10, 10}, 5020)
		uploadTree(t, deps, "master", "base", []string{"lv1", "lv2"}, []int{10, 0}, []int{10, 10}, 5020)
		testCommit(t, index, "br-1", "br-1-2")
		testCommit(t, index, "master", "master-2")

		diffs, err := index.Merge(REPO, "master", "br-1", "user-1")
		if err != nil {
			t.Error("unexpected error:", err)
		}
		createdCount, updatedCount, removedCount := countSuccess(diffs)
		if createdCount != 10 ||
			removedCount != 0 ||
			updatedCount != 0 {
			t.Errorf("success counters wrong added %d, changed %d, removed %d\n ", createdCount, updatedCount, removedCount)
		}
	})

	t.Run("large tree with many conflicts", func(t *testing.T) {
		deps := getDependencies(t)
		index := deps.meta

		uploadTree(t, deps, "master", "base", []string{"lva", "lvb"}, []int{0, 0}, []int{10, 10}, 1)
		testCommit(t, index, "master", "master-1")
		createBranch(t, index, "br-1", "master")
		testCommit(t, index, "br-1", "br-1-1")
		uploadTree(t, deps, "br-1", "base", []string{"lva", "lvb"}, []int{0, 0}, []int{10, 10}, 1)
		uploadTree(t, deps, "master", "base", []string{"lv1", "lv2"}, []int{10, 0}, []int{10, 10}, 1)
		uploadTree(t, deps, "br-1", "base", []string{"lv1", "lv2"}, []int{10, 5}, []int{10, 10}, 1)
		testCommit(t, index, "br-1", "br-1-2")
		testCommit(t, index, "master", "master-2")

		diffs, err := index.Merge(REPO, "master", "br-1", "user-1")
		if err != errors.ErrMergeConflict {
			t.Error("did not identify conflict ", err)
		}
		if countConflict(diffs) != 50 {
			t.Error("number of conflicts is ", len(diffs))
		}
	})
}

func uploadTree(t *testing.T, deps *dependencies, branch, base string, nm []string, startLevel, numInLevel []int, size int64) {
	for i := 0; i < numInLevel[0]; i++ {
		for j := 0; j < numInLevel[1]; j++ {
			path := fmt.Sprintf("%s/%s%d/%s%d", base, nm[0], i+startLevel[0], nm[1], j+startLevel[1])
			uploadObject(t, deps, path, branch, size)
		}
	}
}

func countSuccess(result merkle.Differences) (added, changed, removed int) {
	for _, r := range result {
		switch r.Type {
		case merkle.DifferenceTypeAdded:
			added++
		case merkle.DifferenceTypeChanged:
			changed++
		case merkle.DifferenceTypeRemoved:
			removed++
		}
	}
	return
}

func countConflict(result merkle.Differences) (conflict int) {
	for _, r := range result {
		if r.Direction == merkle.DifferenceDirectionConflict {
			conflict++
		}
	}
	return
}
