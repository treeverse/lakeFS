package ref_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func newFakeAddressProvider(commits []*graveler.Commit) *fakeAddressProvider {
	identityToFakeIdentity := make(map[string]string)
	for _, c := range commits {
		identityToFakeIdentity[hex.EncodeToString(c.Identity())] = c.Message
	}
	return &fakeAddressProvider{identityToFakeIdentity: identityToFakeIdentity}
}

type testCommit struct {
	id      string
	parents []string
	version *int
}

func TestDBOrderedCommitIterator(t *testing.T) {
	ctx := context.Background()
	var (
		cases = []struct {
			Name                   string
			Commits                []*testCommit
			expectedCommits        []string
			expectedAncestryLeaves []string
		}{
			{
				Name: "two_ancestries",
				Commits: []*testCommit{
					{id: "e0", parents: []string{}},
					{id: "c1", parents: []string{"e0"}},
					{id: "b2", parents: []string{"e0"}},
					{id: "d3", parents: []string{"c1"}},
					{id: "e4", parents: []string{"b2"}},
					{id: "f5", parents: []string{"d3"}},
					{id: "a6", parents: []string{"e4"}},
					{id: "c7", parents: []string{"f5"}}},
				expectedCommits:        []string{"a6", "b2", "c1", "c7", "d3", "e0", "e4", "f5"},
				expectedAncestryLeaves: []string{"a6", "c7"},
			},
			{
				Name: "with_merge",
				Commits: []*testCommit{
					{id: "e0", parents: []string{}},
					{id: "c1", parents: []string{"e0"}},
					{id: "b2", parents: []string{"e0"}},
					{id: "d3", parents: []string{"c1"}},
					{id: "e4", parents: []string{"b2"}},
					{id: "f5", parents: []string{"d3"}},
					{id: "a6", parents: []string{"e4"}},
					{id: "c7", parents: []string{"f5", "a6"}}},
				expectedCommits:        []string{"a6", "b2", "c1", "c7", "d3", "e0", "e4", "f5"},
				expectedAncestryLeaves: []string{"a6", "c7"},
			},
			{
				Name: "with_merge_old_version",
				Commits: []*testCommit{
					{id: "e0", parents: []string{}},
					{id: "c1", parents: []string{"e0"}},
					{id: "b2", parents: []string{"e0"}},
					{id: "d3", parents: []string{"c1"}},
					{id: "e4", parents: []string{"b2"}},
					{id: "f5", parents: []string{"d3"}},
					{id: "a6", parents: []string{"e4"}},
					{id: "c7", parents: []string{"f5", "a6"}, version: swag.Int(int(graveler.CommitVersionInitial))}},
				expectedCommits:        []string{"a6", "b2", "c1", "c7", "d3", "e0", "e4", "f5"},
				expectedAncestryLeaves: []string{"c7", "f5"},
			},
			{
				Name: "criss_cross_merge",
				Commits: []*testCommit{
					{id: "e0", parents: []string{}},
					{id: "c1", parents: []string{"e0"}},
					{id: "b2", parents: []string{"e0"}},
					{id: "d3", parents: []string{"c1", "b2"}},
					{id: "a4", parents: []string{"c1", "b2"}},
					{id: "c5", parents: []string{"d3"}},
					{id: "f6", parents: []string{"a4"}},
				},
				expectedCommits:        []string{"a4", "b2", "c1", "c5", "d3", "e0", "f6"},
				expectedAncestryLeaves: []string{"b2", "c5", "f6"},
			},
			{
				Name: "merges_in_history",
				// from git core tests:
				// E---D---C---B---A
				// \"-_         \   \
				//  \  `---------G   \
				//   \                \
				//    F----------------H
				Commits: []*testCommit{
					{id: "e", parents: []string{}},
					{id: "d", parents: []string{"e"}},
					{id: "f", parents: []string{"e"}},
					{id: "c", parents: []string{"d"}},
					{id: "b", parents: []string{"c"}},
					{id: "a", parents: []string{"b"}},
					{id: "g", parents: []string{"b", "e"}},
					{id: "h", parents: []string{"a", "f"}},
				},
				expectedCommits:        []string{"a", "b", "c", "d", "e", "f", "g", "h"},
				expectedAncestryLeaves: []string{"f", "g", "h"},
			},
			{
				Name: "merge_to_and_from_main",
				// E---D----C---B------A
				//  \      /     \    /
				//   F----G---H---I--J
				Commits: []*testCommit{
					{id: "e", parents: []string{}},
					{id: "d", parents: []string{"e"}},
					{id: "f", parents: []string{"e"}},
					{id: "g", parents: []string{"f"}},
					{id: "c", parents: []string{"d", "g"}},
					{id: "b", parents: []string{"c"}},
					{id: "h", parents: []string{"g"}},
					{id: "i", parents: []string{"h", "b"}},
					{id: "j", parents: []string{"i"}},
					{id: "a", parents: []string{"b", "j"}},
				},
				expectedCommits:        []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
				expectedAncestryLeaves: []string{"a", "j"},
			},
		}
	)
	for _, tst := range cases {
		t.Run(tst.Name, func(t *testing.T) {
			var commits []*graveler.Commit
			for _, tstCommit := range tst.Commits {
				parents := make([]graveler.CommitID, 0, len(tstCommit.parents))
				for _, parent := range tstCommit.parents {
					parents = append(parents, graveler.CommitID(parent))
				}
				version := graveler.CurrentCommitVersion
				if tstCommit.version != nil {
					version = graveler.CommitVersion(*tstCommit.version)
				}
				commits = append(commits, &graveler.Commit{
					Message: tstCommit.id,
					Parents: parents,
					Version: version,
				})
			}
			r, db := testRefManagerWithDBAndAddressProvider(t, newFakeAddressProvider(commits))
			repoID := graveler.RepositoryID(tst.Name)
			repository, err := r.CreateBareRepository(ctx, repoID, graveler.Repository{
				StorageNamespace: "s3://foo",
				CreationDate:     time.Now(),
				DefaultBranchID:  "main",
			})
			testutil.Must(t, err)
			for _, c := range commits {
				_, err := r.AddCommit(ctx, repository, *c)
				testutil.Must(t, err)
			}

			iterator, err := ref.NewDBOrderedCommitIterator(ctx, db, repository, 1000)
			if err != nil {
				t.Fatal("create db ordered commit iterator", err)
			}
			var actualOrderedCommits []string
			for iterator.Next() {
				c := iterator.Value()
				actualOrderedCommits = append(
					actualOrderedCommits,
					string(c.CommitID),
				)
			}
			testutil.Must(t, iterator.Err())
			if diff := deep.Equal(tst.expectedCommits, actualOrderedCommits); diff != nil {
				t.Errorf("Unexpected ordered commits from iterator. diff=%s", diff)
			}
			iterator.Close()
			iterator, err = ref.NewDBOrderedCommitIterator(ctx, db, repository, 1000, ref.WithOnlyAncestryLeaves())
			if err != nil {
				t.Fatal("create db ordered commit iterator", err)
			}
			var actualAncestryLeaves []string
			for iterator.Next() {
				c := iterator.Value()
				actualAncestryLeaves = append(
					actualAncestryLeaves,
					string(c.CommitID),
				)
			}
			testutil.Must(t, iterator.Err())
			if diff := deep.Equal(tst.expectedAncestryLeaves, actualAncestryLeaves); diff != nil {
				t.Errorf("Unexpected ancestry leaves from iterator. diff=%s", diff)
			}
			iterator.Close()
		})
	}
}

func TestDBOrderedCommitIteratorGrid(t *testing.T) {
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
	//         (2,1)      (1, 2)
	//              \    /
	//              (1,1)
	grid := make([][]*graveler.Commit, 10)
	commits := make([]*graveler.Commit, 0, 100)
	expectedCommitIDS := make([]string, 0, 100)
	expectedAncestryLeaves := make([]string, 0, 10)
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
			c := &graveler.Commit{Message: fmt.Sprintf("%d-%d", i, j), Parents: parents, Version: graveler.CurrentCommitVersion}
			grid[i][j] = c
			expectedCommitIDS = append(expectedCommitIDS, fmt.Sprintf("%d-%d", i, j))
			if i == 9 {
				// nodes with i == 9 are never the first parent of any node, since for all nodes
				expectedAncestryLeaves = append(expectedAncestryLeaves, fmt.Sprintf("%d-%d", i, j))
			}
			commits = append(commits, c)
		}
	}
	sort.Strings(expectedCommitIDS)
	r, db := testRefManagerWithDBAndAddressProvider(t, newFakeAddressProvider(commits))
	ctx := context.Background()
	repository, err := r.CreateBareRepository(ctx, "repo", graveler.Repository{
		StorageNamespace: "s3://foo",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)
	for _, c := range commits {
		_, err := r.AddCommit(ctx, repository, *c)
		testutil.Must(t, err)
	}
	iterator, err := ref.NewDBOrderedCommitIterator(ctx, db, repository, 10)
	if err != nil {
		t.Fatal("create db ordered commit iterator", err)
	}
	var actualOrderedCommits []string
	for iterator.Next() {
		c := iterator.Value()
		actualOrderedCommits = append(
			actualOrderedCommits,
			string(c.CommitID),
		)
	}
	testutil.Must(t, iterator.Err())
	if diff := deep.Equal(expectedCommitIDS, actualOrderedCommits); diff != nil {
		t.Errorf("Unexpected ordered commits from iterator. diff=%s", diff)
	}
	iterator.Close()
	iterator, err = ref.NewDBOrderedCommitIterator(ctx, db, repository, 10, ref.WithOnlyAncestryLeaves())
	if err != nil {
		t.Fatal("create db ordered commit iterator", err)
	}
	var actualAncestryLeaves []string
	for iterator.Next() {
		c := iterator.Value()
		actualAncestryLeaves = append(
			actualAncestryLeaves,
			string(c.CommitID),
		)
	}
	testutil.Must(t, iterator.Err())
	if diff := deep.Equal(expectedAncestryLeaves, actualAncestryLeaves); diff != nil {
		t.Errorf("Unexpected ancestry leaves from iterator. diff=%s", diff)
	}
	iterator.Close()

}

func TestKVOrderedCommitIterator(t *testing.T) {
	ctx := context.Background()
	var (
		cases = []struct {
			Name                   string
			Commits                []*testCommit
			expectedCommits        []string
			expectedAncestryLeaves []string
		}{
			{
				Name: "two_ancestries",
				Commits: []*testCommit{
					{id: "e0", parents: []string{}},
					{id: "c1", parents: []string{"e0"}},
					{id: "b2", parents: []string{"e0"}},
					{id: "d3", parents: []string{"c1"}},
					{id: "e4", parents: []string{"b2"}},
					{id: "f5", parents: []string{"d3"}},
					{id: "a6", parents: []string{"e4"}},
					{id: "c7", parents: []string{"f5"}}},
				expectedCommits:        []string{"a6", "b2", "c1", "c7", "d3", "e0", "e4", "f5"},
				expectedAncestryLeaves: []string{"a6", "c7"},
			},
			{
				Name: "with_merge",
				Commits: []*testCommit{
					{id: "e0", parents: []string{}},
					{id: "c1", parents: []string{"e0"}},
					{id: "b2", parents: []string{"e0"}},
					{id: "d3", parents: []string{"c1"}},
					{id: "e4", parents: []string{"b2"}},
					{id: "f5", parents: []string{"d3"}},
					{id: "a6", parents: []string{"e4"}},
					{id: "c7", parents: []string{"f5", "a6"}}},
				expectedCommits:        []string{"a6", "b2", "c1", "c7", "d3", "e0", "e4", "f5"},
				expectedAncestryLeaves: []string{"a6", "c7"},
			},
			{
				Name: "with_merge_old_version",
				Commits: []*testCommit{
					{id: "e0", parents: []string{}},
					{id: "c1", parents: []string{"e0"}},
					{id: "b2", parents: []string{"e0"}},
					{id: "d3", parents: []string{"c1"}},
					{id: "e4", parents: []string{"b2"}},
					{id: "f5", parents: []string{"d3"}},
					{id: "a6", parents: []string{"e4"}},
					{id: "c7", parents: []string{"f5", "a6"}, version: swag.Int(int(graveler.CommitVersionInitial))}},
				expectedCommits:        []string{"a6", "b2", "c1", "c7", "d3", "e0", "e4", "f5"},
				expectedAncestryLeaves: []string{"c7", "f5"},
			},
			{
				Name: "criss_cross_merge",
				Commits: []*testCommit{
					{id: "e0", parents: []string{}},
					{id: "c1", parents: []string{"e0"}},
					{id: "b2", parents: []string{"e0"}},
					{id: "d3", parents: []string{"c1", "b2"}},
					{id: "a4", parents: []string{"c1", "b2"}},
					{id: "c5", parents: []string{"d3"}},
					{id: "f6", parents: []string{"a4"}},
				},
				expectedCommits:        []string{"a4", "b2", "c1", "c5", "d3", "e0", "f6"},
				expectedAncestryLeaves: []string{"b2", "c5", "f6"},
			},
			{
				Name: "merges_in_history",
				// from git core tests:
				// E---D---C---B---A
				// \"-_         \   \
				//  \  `---------G   \
				//   \                \
				//    F----------------H
				Commits: []*testCommit{
					{id: "e", parents: []string{}},
					{id: "d", parents: []string{"e"}},
					{id: "f", parents: []string{"e"}},
					{id: "c", parents: []string{"d"}},
					{id: "b", parents: []string{"c"}},
					{id: "a", parents: []string{"b"}},
					{id: "g", parents: []string{"b", "e"}},
					{id: "h", parents: []string{"a", "f"}},
				},
				expectedCommits:        []string{"a", "b", "c", "d", "e", "f", "g", "h"},
				expectedAncestryLeaves: []string{"f", "g", "h"},
			},
			{
				Name: "merge_to_and_from_main",
				// E---D----C---B------A
				//  \      /     \    /
				//   F----G---H---I--J
				Commits: []*testCommit{
					{id: "e", parents: []string{}},
					{id: "d", parents: []string{"e"}},
					{id: "f", parents: []string{"e"}},
					{id: "g", parents: []string{"f"}},
					{id: "c", parents: []string{"d", "g"}},
					{id: "b", parents: []string{"c"}},
					{id: "h", parents: []string{"g"}},
					{id: "i", parents: []string{"h", "b"}},
					{id: "j", parents: []string{"i"}},
					{id: "a", parents: []string{"b", "j"}},
				},
				expectedCommits:        []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
				expectedAncestryLeaves: []string{"a", "j"},
			},
		}
	)
	for _, tst := range cases {
		t.Run(tst.Name, func(t *testing.T) {
			var commits []*graveler.Commit
			for _, tstCommit := range tst.Commits {
				parents := make([]graveler.CommitID, 0, len(tstCommit.parents))
				for _, parent := range tstCommit.parents {
					parents = append(parents, graveler.CommitID(parent))
				}
				version := graveler.CurrentCommitVersion
				if tstCommit.version != nil {
					version = graveler.CommitVersion(*tstCommit.version)
				}
				commits = append(commits, &graveler.Commit{
					Message: tstCommit.id,
					Parents: parents,
					Version: version,
				})
			}
			r, store := testRefManagerWithKVAndAddressProvider(t, newFakeAddressProvider(commits))
			repository, err := r.CreateBareRepository(ctx, graveler.RepositoryID(tst.Name), graveler.Repository{
				StorageNamespace: "s3://foo",
				CreationDate:     time.Now(),
				DefaultBranchID:  "main",
			})
			testutil.Must(t, err)
			for _, c := range commits {
				_, err := r.AddCommit(ctx, repository, *c)
				testutil.Must(t, err)
			}

			iterator, err := ref.NewKVOrderedCommitIterator(ctx, &store, repository, false)
			if err != nil {
				t.Fatal("create kv ordered commit iterator", err)
			}
			var actualOrderedCommits []string
			for iterator.Next() {
				c := iterator.Value()
				actualOrderedCommits = append(
					actualOrderedCommits,
					string(c.CommitID),
				)
			}
			testutil.Must(t, iterator.Err())
			if diff := deep.Equal(tst.expectedCommits, actualOrderedCommits); diff != nil {
				t.Errorf("Unexpected ordered commits from iterator. diff=%s", diff)
			}
			iterator.Close()
			iterator, err = ref.NewKVOrderedCommitIterator(ctx, &store, repository, true)
			if err != nil {
				t.Fatal("create kv ordered commit iterator", err)
			}
			var actualAncestryLeaves []string
			for iterator.Next() {
				c := iterator.Value()
				actualAncestryLeaves = append(
					actualAncestryLeaves,
					string(c.CommitID),
				)
			}
			testutil.Must(t, iterator.Err())
			if diff := deep.Equal(tst.expectedAncestryLeaves, actualAncestryLeaves); diff != nil {
				t.Errorf("Unexpected ancestry leaves from iterator. diff=%s", diff)
			}
			iterator.Close()
		})
	}
}

func TestKVOrderedCommitIteratorGrid(t *testing.T) {
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
	//         (2,1)      (1, 2)
	//              \    /
	//              (1,1)
	grid := make([][]*graveler.Commit, 10)
	commits := make([]*graveler.Commit, 0, 100)
	expectedCommitIDS := make([]string, 0, 100)
	expectedAncestryLeaves := make([]string, 0, 10)
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
			c := &graveler.Commit{Message: fmt.Sprintf("%d-%d", i, j), Parents: parents, Version: graveler.CurrentCommitVersion}
			grid[i][j] = c
			expectedCommitIDS = append(expectedCommitIDS, fmt.Sprintf("%d-%d", i, j))
			if i == 9 {
				// nodes with i == 9 are never the first parent of any node, since for all nodes
				expectedAncestryLeaves = append(expectedAncestryLeaves, fmt.Sprintf("%d-%d", i, j))
			}
			commits = append(commits, c)
		}
	}
	sort.Strings(expectedCommitIDS)
	r, store := testRefManagerWithKVAndAddressProvider(t, newFakeAddressProvider(commits))
	ctx := context.Background()
	repo := graveler.RepositoryRecord{
		RepositoryID: graveler.RepositoryID("repo"),
		Repository: &graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		},
	}
	repository, err := r.CreateBareRepository(ctx, repo.RepositoryID, *repo.Repository)
	testutil.Must(t, err)
	for _, c := range commits {
		_, err := r.AddCommit(ctx, repository, *c)
		testutil.Must(t, err)
	}
	iterator, err := ref.NewKVOrderedCommitIterator(ctx, &store, &repo, false)
	if err != nil {
		t.Fatal("create kv ordered commit iterator", err)
	}
	var actualOrderedCommits []string
	for iterator.Next() {
		c := iterator.Value()
		actualOrderedCommits = append(
			actualOrderedCommits,
			string(c.CommitID),
		)
	}
	testutil.Must(t, iterator.Err())
	if diff := deep.Equal(expectedCommitIDS, actualOrderedCommits); diff != nil {
		t.Errorf("Unexpected ordered commits from iterator. diff=%s", diff)
	}
	iterator.Close()
	iterator, err = ref.NewKVOrderedCommitIterator(ctx, &store, &repo, true)
	if err != nil {
		t.Fatal("create kv ordered commit iterator", err)
	}
	var actualAncestryLeaves []string
	for iterator.Next() {
		c := iterator.Value()
		actualAncestryLeaves = append(
			actualAncestryLeaves,
			string(c.CommitID),
		)
	}
	testutil.Must(t, iterator.Err())
	if diff := deep.Equal(expectedAncestryLeaves, actualAncestryLeaves); diff != nil {
		t.Errorf("Unexpected ancestry leaves from iterator. diff=%s", diff)
	}
	iterator.Close()

}
