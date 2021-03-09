package ref_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/assert"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestManager_GetRepository(t *testing.T) {
	r := testRefManager(t)
	t.Run("repo_doesnt_exist", func(t *testing.T) {
		_, err := r.GetRepository(context.Background(), "example-repo")
		if !errors.Is(err, graveler.ErrRepositoryNotFound) {
			t.Fatalf("expected ErrRepositoryNotFound got error: %v", err)
		}
	})
	t.Run("repo_exists", func(t *testing.T) {
		repoID := graveler.RepositoryID("example-repo")
		branchID := graveler.BranchID("weird-branch")

		testutil.Must(t, r.CreateRepository(context.Background(), repoID, graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  branchID,
		}, ""))

		repo, err := r.GetRepository(context.Background(), repoID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if repo.DefaultBranchID != "weird-branch" {
			t.Fatalf("got unexpected branch ID: %s", repo.DefaultBranchID)
		}
		branch, err := r.GetBranch(context.Background(), repoID, branchID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if branch.CommitID == "" {
			t.Fatal("empty first commit - first commit wasn't created")
		}

		commit, err := r.GetCommit(context.Background(), repoID, branch.CommitID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(commit.Parents) != 0 {
			t.Fatalf("first commit parents should be empty: %v", commit.Parents)
		}
		if commit.MetaRangeID != "" {
			t.Fatalf("first commit metarange should be empty: %v", commit.MetaRangeID)
		}
	})
}

func TestManager_ListRepositories(t *testing.T) {
	r := testRefManager(t)
	repos := []graveler.RepositoryID{"a", "aa", "b", "c", "e", "d"}
	for _, repoId := range repos {
		testutil.Must(t, r.CreateRepository(context.Background(), repoId, graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "master",
		}, ""))
	}

	t.Run("listing all repos", func(t *testing.T) {
		iter, err := r.ListRepositories(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(repoIds, []graveler.RepositoryID{"a", "aa", "b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})

	t.Run("listing repos from prefix", func(t *testing.T) {
		iter, err := r.ListRepositories(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		iter.SeekGE("aaa")

		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(repoIds, []graveler.RepositoryID{"b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})
}

func TestManager_DeleteRepository(t *testing.T) {
	r := testRefManager(t)
	t.Run("repo_exists", func(t *testing.T) {
		testutil.Must(t, r.CreateRepository(context.Background(), "example-repo", graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "weird-branch",
		}, ""))

		_, err := r.GetRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = r.DeleteRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = r.GetRepository(context.Background(), "example-repo")
		if !errors.Is(err, graveler.ErrRepositoryNotFound) {
			t.Fatalf("expected ErrRepositoryNotFound, got: %v", err)
		}
	})

	t.Run("repo_does_not_exist", func(t *testing.T) {
		err := r.DeleteRepository(context.Background(), "example-repo11111")
		if !errors.Is(err, graveler.ErrRepositoryNotFound) {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestManager_GetBranch(t *testing.T) {
	r := testRefManager(t)
	t.Run("get_branch_exists", func(t *testing.T) {
		testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
			StorageNamespace: "s3://",
			CreationDate:     time.Now(),
			DefaultBranchID:  "master",
		}, ""))
		branch, err := r.GetBranch(context.Background(), "repo1", "master")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if branch.CommitID == "" {
			t.Fatal("unexpected empty branch commit received")
		}
	})

	t.Run("get_branch_doesnt_exists", func(t *testing.T) {
		_, err := r.GetBranch(context.Background(), "repo1", "masterrrrr")
		if !errors.Is(err, graveler.ErrBranchNotFound) {
			t.Fatalf("expected ErrBranchNotFound, got error: %v", err)
		}
	})
}

func TestManager_SetBranch(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, ""))

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", graveler.Branch{
		CommitID: "c2",
	}))

	b, err := r.GetBranch(context.Background(), "repo1", "branch2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b.CommitID != "c2" {
		t.Fatalf("unexpected commit for branch2: %s - expected: c2", b.CommitID)
	}

	// overwrite
	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", graveler.Branch{
		CommitID: "c3",
	}))

	b, err = r.GetBranch(context.Background(), "repo1", "branch2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b.CommitID != "c3" {
		t.Fatalf("unexpected commit for branch2: %s - expected: c3", b.CommitID)
	}

}

func TestManager_DeleteBranch(t *testing.T) {
	r := testRefManager(t)
	ctx := context.Background()
	testutil.Must(t, r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, ""))

	testutil.Must(t, r.SetBranch(ctx, "repo1", "branch2", graveler.Branch{
		CommitID: "c2",
	}))

	testutil.Must(t, r.DeleteBranch(ctx, "repo1", "branch2"))

	_, err := r.GetBranch(ctx, "repo1", "branch2")
	if !errors.Is(err, graveler.ErrBranchNotFound) {
		t.Fatalf("Expected ErrBranchNotFound, got error: %v", err)
	}
}

func TestManager_ListBranches(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, ""))

	for _, b := range []graveler.BranchID{"a", "aa", "c", "b", "z", "f"} {
		testutil.Must(t, r.SetBranch(context.Background(), "repo1", b, graveler.Branch{
			CommitID: "c2",
		}))
	}

	iter, err := r.ListBranches(context.Background(), "repo1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var bs []graveler.BranchID
	for iter.Next() {
		b := iter.Value()
		bs = append(bs, b.BranchID)
	}
	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}
	if !reflect.DeepEqual(bs, []graveler.BranchID{"a", "aa", "b", "c", "f", "master", "z"}) {
		t.Fatalf("unexpected branch list: %v", bs)
	}
}

func TestManager_GetTag(t *testing.T) {
	r := testRefManager(t)
	t.Run("exists", func(t *testing.T) {
		ctx := context.Background()
		err := r.CreateRepository(ctx, "repo1", graveler.Repository{
			StorageNamespace: "s3://",
			CreationDate:     time.Now(),
			DefaultBranchID:  "master",
		}, "")
		testutil.MustDo(t, "create repo", err)
		err = r.CreateTag(ctx, "repo1", "v1.0", "c1")
		testutil.MustDo(t, "set tag", err)
		commitID, err := r.GetTag(context.Background(), "repo1", "v1.0")
		testutil.MustDo(t, "get existing tag", err)
		if commitID == nil {
			t.Fatal("get tag, missing commit id")
		}
		if *commitID != "c1" {
			t.Fatalf("get tag, commit id: %s, expected c1", *commitID)
		}
	})

	t.Run("not_exists", func(t *testing.T) {
		commitID, err := r.GetTag(context.Background(), "repo1", "v1.bad")
		if !errors.Is(err, graveler.ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got error: %v", err)
		}
		if commitID != nil {
			t.Fatalf("get not existing commitID: %s, expected nil", *commitID)
		}
	})
}

func TestManager_CreateTag(t *testing.T) {
	r := testRefManager(t)
	ctx := context.Background()
	testutil.Must(t, r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, ""))

	err := r.CreateTag(ctx, "repo1", "v2", "c2")
	testutil.MustDo(t, "create tag v2", err)

	commit, err := r.GetTag(ctx, "repo1", "v2")
	testutil.MustDo(t, "get v2 tag", err)
	if commit == nil {
		t.Fatal("get tag got nil")
	}
	if *commit != "c2" {
		t.Fatalf("unexpected commit for tag v2: %s - expected: c2", *commit)
	}

	// check we can't create existing
	err = r.CreateTag(ctx, "repo1", "v2", "c5")
	if !errors.Is(err, graveler.ErrTagAlreadyExists) {
		t.Fatalf("CreateTag() err = %s, expected already exists", err)
	}
	// overwrite by delete and create
	err = r.DeleteTag(ctx, "repo1", "v2")
	testutil.MustDo(t, "delete tag v2", err)

	err = r.CreateTag(ctx, "repo1", "v2", "c3")
	testutil.MustDo(t, "re-create tag v2", err)

	commit, err = r.GetTag(ctx, "repo1", "v2")
	testutil.MustDo(t, "get tag v2", err)
	if commit == nil {
		t.Fatal("get tag got nil")
	}
	if *commit != "c3" {
		t.Fatalf("unexpected commit for v2: %s - expected: c3", *commit)
	}
}

func TestManager_DeleteTag(t *testing.T) {
	r := testRefManager(t)
	ctx := context.Background()
	testutil.Must(t, r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, ""))

	testutil.Must(t, r.CreateTag(ctx, "repo1", "v1", "c2"))

	testutil.Must(t, r.DeleteTag(ctx, "repo1", "v1"))

	commitID, err := r.GetTag(ctx, "repo1", "v1")
	if !errors.Is(err, graveler.ErrNotFound) {
		t.Fatal("unexpected error:", err)
	}
	if commitID != nil {
		t.Fatal("expected commit ID:", *commitID)
	}
}

func TestManager_ListTags(t *testing.T) {
	r := testRefManager(t)
	ctx := context.Background()
	testutil.Must(t, r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, ""))

	var commitsTagged []graveler.CommitID
	tags := []string{"tag-a", "tag-b", "the-end", "v1", "v1.1"}
	sort.Strings(tags)
	for i, tag := range tags {
		commitID := graveler.CommitID(fmt.Sprintf("c%d", i))
		commitsTagged = append(commitsTagged, commitID)
		err := r.CreateTag(ctx, "repo1", graveler.TagID(tag), commitID)
		testutil.MustDo(t, "set tag "+tag, err)
	}

	iter, err := r.ListTags(ctx, "repo1")
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	var commits []graveler.CommitID
	for iter.Next() {
		commits = append(commits, iter.Value().CommitID)
	}
	testutil.MustDo(t, "list tags completed", iter.Err())

	if diff := deep.Equal(commits, commitsTagged); diff != nil {
		t.Fatal("ListTags found mismatch:", diff)
	}
}

func TestManager_AddCommit(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, ""))

	ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z00:00")
	c := graveler.Commit{
		Committer:    "user1",
		Message:      "message1",
		MetaRangeID:  "deadbeef123",
		CreationDate: ts,
		Parents:      graveler.CommitParents{"deadbeef1", "deadbeef12"},
		Metadata:     graveler.Metadata{"foo": "bar"},
	}

	cid, err := r.AddCommit(context.Background(), "repo1", c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cid != "2277b5abd2d3ba6b4d35c48a0e358b0c4bcf5cd6d891c67437fb4c4af0d2fd4b" {
		t.Fatalf("unexpected commit ID: %s", cid)
	}

	commit, err := r.GetCommit(context.Background(), "repo1", cid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if commit.Parents[0] != "deadbeef1" {
		t.Fatalf("expected parent1 to be deadbeef1, got %v", commit.Parents)
	}

	if commit.Metadata["foo"] != "bar" {
		t.Fatalf("unexpected metadata value for foo: %v", commit.Metadata["foo"])
	}
}

func TestManager_Log(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, ""))

	ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z")
	var previous graveler.CommitID
	for i := 0; i < 20; i++ {
		c := graveler.Commit{
			Committer:    "user1",
			Message:      "message1",
			MetaRangeID:  "deadbeef123",
			CreationDate: ts,
			Parents:      graveler.CommitParents{},
			Metadata:     graveler.Metadata{"foo": "bar"},
		}
		if previous != "" {
			c.Parents = append(c.Parents, previous)
		}
		cid, err := r.AddCommit(context.Background(), "repo1", c)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		previous = cid
		ts = ts.Add(time.Second)
	}

	iter, err := r.Log(context.Background(), "repo1", previous)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ids := make([]graveler.CommitID, 0)
	for iter.Next() {
		c := iter.Value()
		ids = append(ids, c.CommitID)
	}
	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}

	expected := []graveler.CommitID{
		"663b7520a2a05aaeed17de6136fa80eb5cd8417982011eb551230571ee412f2f",
		"87856d024fbe092852118edd958717d905019fa4eae40bac18a719e2f869e0f7",
		"25e51c6a8675c52558f8e303757624fca629bbc81f53afffa71a560df1c03948",
		"7653a24da53a43b229a64c7fec4a3259ed2cd3cba8b0021650dd54ea286a06cd",
		"d19e92e3b0717236255b529b35a7f1ec33e716be58af01c0f2fda80f4ded5a7a",
		"ac2f92fbefff7914f82c148a391c4705555aacb4ef9fe2c43c21e88f92e459ec",
		"fecd3e1f97cc1e54df6a06737d98931a8298c7ab1c870042666a10b42f7f7c0a",
		"1d6e9e55a600eceead14e70a903cea94df5a7b74a6ca4de6f8206ab45d60a5bd",
		"1f147e667ad0db53c2e9392d4bd35cb649269762f1da19e9e4d2e7b444dfd875",
		"61d527f08cc67522728f8ffc93bcb91cc789de80beb9c43a41ee225fb9c446b2",
		"32700dc4b5355be186976745fbef029f8ef7533170c0766ed77c9e3f574178c5",
		"4d82f11b02d6cb609d2bc1007620f578733ffff971a749ff4462dc69b834c20a",
		"2b4bb867adb1ac94c2f569dca92b9156a40ba0cd22a4bdc63cac1eb6b21b6f63",
		"72ee57f5cb8dddb264624f9ac7266c6ebc82af509e69f84d592a6732e74af06c",
		"3bbd01827326eba3f2a60e2ec98573cff1d2ead6c53336a5796ccf9d6401b052",
		"8107b1d0a1ce6f75a1848f31ab3261eb86acdfe9b4e84b7eaf329b3904179de9",
		"988c38b9f7d9b5df7242c3e837dac93e91dc0ff73da7dae1e010bcf18e3e0fa6",
		"cb5dca579b23b81f8148fc9153a4c9c733d830c26be1d5f8d12496300c02dd89",
		"ebbd689937253304ae29a541a727bfd11ab59a5659bb293e8ab2ed407c9a74c1",
		"fac53a04432b2e6e7185f3ac8314a874c556b2557adf3aa8d5b1df985cf96566",
	}
	if diff := deep.Equal(ids, expected); diff != nil {
		t.Fatal("Commits log wrong result:", diff)
	}
}

func TestManager_LogGraph(t *testing.T) {
	r := testRefManager(t)
	ctx := context.Background()
	err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, "")
	testutil.MustDo(t, "Create repository", err)

	/*
		---1----2----4----7
		    \	           \
			 3----5----6----8---
	*/
	nextCommitNumber := 0
	nextCommitTS, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z")
	addNextCommit := func(parents ...graveler.CommitID) graveler.CommitID {
		nextCommitTS = nextCommitTS.Add(time.Minute)
		nextCommitNumber++
		id := "c" + strconv.Itoa(nextCommitNumber)
		c := graveler.Commit{
			Committer:    "user1",
			Message:      id,
			MetaRangeID:  "fefe1221",
			CreationDate: nextCommitTS,
			Parents:      parents,
			Metadata:     graveler.Metadata{"foo": "bar"},
		}
		cid, err := r.AddCommit(ctx, "repo1", c)
		testutil.MustDo(t, "Add commit "+id, err)
		return cid
	}
	c1 := addNextCommit()
	c2 := addNextCommit(c1)
	c3 := addNextCommit(c1)
	c4 := addNextCommit(c2)
	c5 := addNextCommit(c3)
	c6 := addNextCommit(c5)
	c7 := addNextCommit(c4)
	c8 := addNextCommit(c6, c7)

	expected := []string{
		"c8", "c7", "c6", "c5", "c4", "c3", "c2", "c1",
	}

	// iterate over the commits
	it, err := r.Log(ctx, "repo1", c8)
	if err != nil {
		t.Fatal("Error during create Log iterator", err)
	}
	defer it.Close()

	var commits []string
	for it.Next() {
		c := it.Value()
		commits = append(commits, c.Message)
	}
	if err := it.Err(); err != nil {
		t.Fatal("Iteration ended with error", err)
	}
	if diff := deep.Equal(commits, expected); diff != nil {
		t.Fatal("Found diff between expected commits:", diff)
	}

	// test SeekGE to "c4"
	it.SeekGE(c4)
	expectedAfterSeek := []string{
		"c4", "c3", "c2", "c1",
	}
	var commitsAfterSeek []string
	for it.Next() {
		c := it.Value()
		commitsAfterSeek = append(commitsAfterSeek, c.Message)
	}
	if err := it.Err(); err != nil {
		t.Fatal("Iteration ended with error", err)
	}
	if diff := deep.Equal(commitsAfterSeek, expectedAfterSeek); diff != nil {
		t.Fatal("Found diff between expected commits (after seek):", diff)
	}
}

type fakeAddressProvider struct {
	identities []string
	idx        int
}

func (f *fakeAddressProvider) ContentAddress(_ ident.Identifiable) string {
	res := f.identities[f.idx]
	f.idx = (f.idx + 1) % len(f.identities)
	return res
}

func TestConsistentCommitIdentity(t *testing.T) {
	hex := ident.NewHexAddressProvider()

	commit := graveler.Commit{
		Committer:    "some-committer",
		Message:      "I just committed",
		MetaRangeID:  "123456789987654321",
		CreationDate: time.Date(2021, time.January, 24, 15, 10, 11, 1564956600, time.UTC),
		Parents: graveler.CommitParents{
			graveler.CommitID("132456987153687sdfsdf"),
			graveler.CommitID("1324569csfvdkjhcsdkjc"),
		},
		Metadata: map[string]string{
			"sdkafjnb":       "1234",
			"sdkjvcnbkjndsc": "asnjkdl",
		},
	}

	// Should NOT be changed (unless you really know what you're doing):
	// If this is failing and you're tempted to change this value,
	// then you probably introducing a breaking change to commit's identity.
	// All previous references to commits (if not migrated) may be lost.
	const expected = "f1a106bbeb12d3eb54418d6000f4507501d289d0d0879dcce6f4d31425587df1"

	// Running many times to check that it's actually consistent (see issue #1291)
	const iterations = 10000

	for i := 0; i < iterations; i++ {
		res := hex.ContentAddress(commit)
		assert.Equalf(t, expected, res, "iteration %d content mismatch", i+1)
	}
}

func TestManager_GetCommitByPrefix(t *testing.T) {
	commitIDs := []string{"c1234", "d1", "b1", "c1245", "a1"}
	r := testRefManagerWithAddressProvider(t, &fakeAddressProvider{identities: append([]string{"zero-commit-id"}, commitIDs...)})

	ctx := context.Background()
	err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, "")
	testutil.MustDo(t, "Create repository", err)
	for _, commitID := range commitIDs {
		c := graveler.Commit{Committer: "user1",
			Message:      fmt.Sprintf("id_%s", commitID),
			MetaRangeID:  "deadbeef123",
			CreationDate: time.Now(),
			Parents:      graveler.CommitParents{"deadbeef1"},
			Metadata:     graveler.Metadata{"foo": "bar"},
		}
		_, err := r.AddCommit(ctx, "repo1", c)
		testutil.MustDo(t, "add commit", err)
		if err != nil {
			t.Fatalf("unexpected error on adding commit: %v", err)
		}
	}
	tests := []struct {
		Prefix                string
		ExpectedCommitMessage string
		ExpectedErr           error
	}{
		{
			Prefix:                "a",
			ExpectedCommitMessage: "id_a1",
		},
		{
			Prefix:                "c123",
			ExpectedCommitMessage: "id_c1234",
		},
		{
			Prefix:      "c1",
			ExpectedErr: graveler.ErrCommitNotFound,
		},
		{
			Prefix:      "e",
			ExpectedErr: graveler.ErrCommitNotFound,
		},
	}
	for _, tst := range tests {
		t.Run(tst.Prefix, func(t *testing.T) {
			c, err := r.GetCommitByPrefix(ctx, "repo1", graveler.CommitID(tst.Prefix))
			if !errors.Is(err, tst.ExpectedErr) {
				t.Fatalf("expected error %v, got=%v", tst.ExpectedErr, err)
			}
			if tst.ExpectedErr != nil {
				return
			}
			if c.Message != tst.ExpectedCommitMessage {
				t.Fatalf("got commit different than expected. expected=%s, got=%s", tst.ExpectedCommitMessage, c.Message)
			}
		})
	}
}
