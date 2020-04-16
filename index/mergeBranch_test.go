package index_test

import (
	"github.com/ory/dockertest/v3"
	log "github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/index/errors"
	"github.com/treeverse/lakefs/index/merkle"
	"github.com/treeverse/lakefs/testutil"
	"strconv"
	"testing"
)

var Created, Updated, Removed int
var closer func()

func TestMerge(t *testing.T) {

	metadata := make(map[string]string)
	metadata["property-1"] = "value-1"
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	databaseUri, closer = testutil.GetDBInstance(pool)
	defer closer()

	t.Run("simplest merge", func(t *testing.T) {
		deps := getDependencies(t)
		index := deps.meta
		uploadObject(t, deps, "t/v/s", "master", 1024)
		testCommit(t, index, "master", "master-1")
		createBranch(t, index, "br-1", "master")
		testCommit(t, index, "br-1", "br-1-1")

		uploadObject(t, deps, "t/v1/s", "master", 10000)
		testCommit(t, index, "master", "master-2")
		diffs, err := index.Merge(REPO, "master", "br-1", "user-1")
		if err != nil {
			t.Error("unexpected error:", err)
		}
		Created, Updated, Removed = countSuccess(diffs)
		if Created != 1 ||
			Removed != 0 ||
			Updated != 0 {
			t.Errorf("success counters wrong added %d, changed %d, removed %d\n ", Created, Updated, Removed)
		}
		_ = getObject(t, index, REPO, "br-1", "t/v1/s", true, "merge failed - document not copied")

	})

	t.Run("merge with removee", func(t *testing.T) {
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
		Created, Updated, Removed = countSuccess(diffs)
		if Created != 0 ||
			Removed != 1 ||
			Updated != 0 {
			t.Errorf("success counters wrong added %d, changed %d, removed %d\n ", Created, Updated, Removed)
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
		Created, Updated, Removed = countSuccess(diffs)
		if Created != 0 ||
			Removed != 0 ||
			Updated != 0 {
			t.Errorf("success counters wrong added %d, changed %d, removed %d\n ", Created, Updated, Removed)
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
		Created, Updated, Removed = countSuccess(diffs)
		if Created != 10 ||
			Removed != 0 ||
			Updated != 0 {
			t.Errorf("success counters wrong added %d, changed %d, removed %d\n ", Created, Updated, Removed)
		}
	})

	t.Run("large tree with many conflicts", func(t *testing.T) {
		deps := getDependencies(t)
		index := deps.meta

		uploadTree(t, deps, "master", "base", []string{"lva", "lvb"}, []int{0, 0}, []int{10, 10}, 4096)
		testCommit(t, index, "master", "master-1")
		createBranch(t, index, "br-1", "master")
		testCommit(t, index, "br-1", "br-1-1")
		uploadTree(t, deps, "br-1", "base", []string{"lva", "lvb"}, []int{0, 0}, []int{10, 10}, 5020)
		uploadTree(t, deps, "master", "base", []string{"lv1", "lv2"}, []int{10, 0}, []int{10, 10}, 5020)
		uploadTree(t, deps, "br-1", "base", []string{"lv1", "lv2"}, []int{10, 5}, []int{10, 10}, 4096)
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
			path := base + "/" + nm[0] + strconv.Itoa(i+startLevel[0]) + "/"
			path += nm[1] + strconv.Itoa(j+startLevel[1])
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
