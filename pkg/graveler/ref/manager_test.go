package ref_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/mock"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
	"go.uber.org/ratelimit"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Mock AdapterConfig only ID method returns the id field
type AdapterConfigMock struct {
	config.AdapterConfig
	id string
}

func (s *AdapterConfigMock) ID() string {
	return s.id
}

// Mock Store only GetStorageByID method returns
// for SingleBlockstoreID AdapterConfig that return the bcID for the ID method
// for any other AdapterConfig that return the storageID for the ID method
type storeMock struct {
	config.StorageConfig
	bcID string
	t    *testing.T
}

func (s *storeMock) GetStorageByID(storageID string) config.AdapterConfig {
	if storageID == config.SingleBlockstoreID {
		return &AdapterConfigMock{
			id: s.bcID,
		}
	}
	return &AdapterConfigMock{
		id: storageID,
	}
}

func NewStorageConfigMock(bcID string) config.StorageConfig {
	return &storeMock{
		bcID: bcID,
	}
}

// TestManager_GetRepositoryCache test get repository information while using cache. Match the number of times we
// call get repository vs number of times we fetch the data.
func TestManager_GetRepositoryCache(t *testing.T) {
	const (
		times = 1
		calls = 3
	)
	ctrl := gomock.NewController(t)
	mockStore := mock.NewMockStore(ctrl)
	ctx := context.Background()
	mockStore.EXPECT().Get(ctx, []byte("graveler"), []byte("repos/repo1")).Times(times).Return(&kv.ValueWithPredicate{}, nil)
	cacheConfig := ref.CacheConfig{
		Size:   100,
		Expiry: 20 * time.Millisecond,
		Jitter: 0,
	}
	cfg := ref.ManagerConfig{
		Executor:              batch.NopExecutor(),
		KVStore:               mockStore,
		AddressProvider:       ident.NewHexAddressProvider(),
		RepositoryCacheConfig: cacheConfig,
		CommitCacheConfig:     cacheConfig,
	}

	refManager := ref.NewRefManager(cfg, NewStorageConfigMock(config.SingleBlockstoreID))
	for i := 0; i < calls; i++ {
		_, err := refManager.GetRepository(ctx, "repo1")
		if err != nil {
			t.Fatalf("Failed to get repository (iteration %d): %s", i, err)
		}
	}

	// wait for cache to expire and call again
	time.Sleep(cacheConfig.Expiry + cacheConfig.Jitter + time.Second)
	mockStore.EXPECT().Get(ctx, []byte("graveler"), []byte("repos/repo1")).Times(1).Return(&kv.ValueWithPredicate{}, nil)
	_, err := refManager.GetRepository(ctx, "repo1")
	if err != nil {
		t.Fatalf("Failed to get repository: %s", err)
	}
}

// TestManager_GetCommitCache test gets commit record while using cache. We match the number of times we call get repository vs number of times we fetch the data.
func TestManager_GetCommitCache(t *testing.T) {
	const (
		times = 1
		calls = 3
	)
	ctrl := gomock.NewController(t)
	mockStore := mock.NewMockStore(ctrl)
	ctx := context.Background()

	const commitID = "8a3e3f677ed588ab1e19b6cdb050cbce383f9f1166200e7b7252932ceb61189c"
	const repoID = "repo2"
	const repoInstanceID = "iuid"
	mockStore.EXPECT().
		Get(ctx, []byte(repoID+"-"+repoInstanceID), []byte("commits/"+commitID)).
		Times(times).
		Return(&kv.ValueWithPredicate{}, nil)

	cacheConfig := ref.CacheConfig{
		Size:   100,
		Expiry: 20 * time.Millisecond,
	}
	cfg := ref.ManagerConfig{
		Executor:              batch.NopExecutor(),
		KVStore:               mockStore,
		AddressProvider:       ident.NewHexAddressProvider(),
		RepositoryCacheConfig: cacheConfig,
		CommitCacheConfig:     cacheConfig,
	}
	refManager := ref.NewRefManager(cfg, NewStorageConfigMock(config.SingleBlockstoreID))
	for i := 0; i < calls; i++ {
		_, err := refManager.GetCommit(ctx, &graveler.RepositoryRecord{
			RepositoryID: repoID,
			Repository:   &graveler.Repository{InstanceUID: repoInstanceID},
		}, commitID)
		if err != nil {
			t.Fatalf("Failed to get commit (iteration %d): %s", i, err)
		}
	}

	// wait for cache to expire and call again
	time.Sleep(cacheConfig.Expiry + cacheConfig.Jitter + time.Second)
	mockStore.EXPECT().
		Get(ctx, []byte(repoID+"-"+repoInstanceID), []byte("commits/"+commitID)).
		Times(times).
		Return(&kv.ValueWithPredicate{}, nil)
	_, err := refManager.GetCommit(ctx, &graveler.RepositoryRecord{
		RepositoryID: repoID,
		Repository:   &graveler.Repository{InstanceUID: repoInstanceID},
	}, commitID)
	if err != nil {
		t.Fatalf("Failed to get repository: %s", err)
	}
}

func TestManager_GetRepository(t *testing.T) {
	r, _ := testRefManager(t)
	t.Run("repo_doesnt_exist", func(t *testing.T) {
		_, err := r.GetRepository(context.Background(), "example-repo")
		if !errors.Is(err, graveler.ErrRepositoryNotFound) {
			t.Fatalf("expected ErrRepositoryNotFound got error: %v", err)
		}
	})
	t.Run("repo_exists", func(t *testing.T) {
		repoID := graveler.RepositoryID("example-repo")
		branchID := graveler.BranchID("weird-branch")

		repository, err := r.CreateRepository(context.Background(), repoID, graveler.Repository{
			StorageID:        "sid",
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  branchID,
		})
		testutil.Must(t, err)

		repo, err := r.GetRepository(context.Background(), repoID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if repo.DefaultBranchID != repository.DefaultBranchID {
			t.Fatalf("got '%s' branch ID, expected '%s'", repo.DefaultBranchID, repository.DefaultBranchID)
		}
		branch, err := r.GetBranch(context.Background(), repo, branchID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if branch.CommitID == "" {
			t.Fatal("empty first commit - first commit wasn't created")
		}

		commit, err := r.GetCommit(context.Background(), repo, branch.CommitID)
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
	r, _ := testRefManager(t)
	repoIDs := []graveler.RepositoryID{"a", "aa", "b", "c", "e", "d"}
	for _, repoId := range repoIDs {
		_, err := r.CreateRepository(context.Background(), repoId, graveler.Repository{
			StorageID:        "sid",
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		})
		testutil.Must(t, err)
	}

	t.Run("listing all repos", func(t *testing.T) {
		iter, err := r.ListRepositories(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer iter.Close()

		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}

		if !reflect.DeepEqual(repoIds, []graveler.RepositoryID{"a", "aa", "b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})

	t.Run("listing repos from prefix", func(t *testing.T) {
		iter, err := r.ListRepositories(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer iter.Close()
		iter.SeekGE("aaa")

		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}

		if !reflect.DeepEqual(repoIds, []graveler.RepositoryID{"b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})
}

func TestManager_DeleteRepository(t *testing.T) {
	r, store := testRefManager(t)
	ctx := context.Background()
	repoID := graveler.RepositoryID("example-repo")

	t.Run("repo_exists", func(t *testing.T) {
		repository, err := r.CreateRepository(ctx, repoID, graveler.Repository{
			StorageID:        "sid",
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "weird-branch",
		})
		testutil.Must(t, err)

		_, err = r.GetRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Create repository entities and ensure their deletion afterwards
		testutil.Must(t, r.CreateTag(ctx, repository, "v1.0", "c1"))
		testutil.Must(t, r.CreateBranch(ctx, repository, "f1", graveler.Branch{CommitID: "c1", StagingToken: "s1"}))
		c := graveler.Commit{
			Committer:    "user1",
			Message:      "message1",
			MetaRangeID:  "deadbeef123",
			CreationDate: time.Now(),
			Parents:      graveler.CommitParents{"deadbeef1", "deadbeef12"},
			Metadata:     graveler.Metadata{"foo": "bar"},
		}
		_, err = r.AddCommit(ctx, repository, c)
		testutil.Must(t, err)

		err = r.DeleteRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// wait for cache expiry
		time.Sleep(testRepoCacheConfig.Expiry + testRepoCacheConfig.Jitter + time.Second)

		_, err = r.GetRepository(context.Background(), "example-repo")
		if !errors.Is(err, graveler.ErrRepositoryNotFound) {
			t.Fatalf("expected ErrRepositoryNotFound, got: %v", err)
		}

		// Verify no keys on repo partition
		itr := kv.NewPartitionIterator(ctx, store, (&graveler.RepoMetadata{}).ProtoReflect().Type(), graveler.RepoPartition(repository), 10)
		defer itr.Close()
		for itr.Next() {
			entry := itr.Entry()
			t.Fatalf("partition expected empty: %s", string(entry.Key))
		}
		// Check itr.Next() not false on an error
		require.NoError(t, itr.Err())

		// Create after delete
		_, err = r.CreateRepository(ctx, repoID, graveler.Repository{
			StorageID:        "sid",
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "weird-branch",
		})
		testutil.Must(t, err)
		_, err = r.GetRepository(context.Background(), "example-repo")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
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
	r, _ := testRefManager(t)
	repository, err := r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	t.Run("get_branch_exists", func(t *testing.T) {
		branch, err := r.GetBranch(context.Background(), repository, "main")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if branch.CommitID == "" {
			t.Fatal("unexpected empty branch commit received")
		}
	})

	t.Run("get_branch_doesnt_exists", func(t *testing.T) {
		_, err := r.GetBranch(context.Background(), repository, "mainnnnn")
		if !errors.Is(err, graveler.ErrBranchNotFound) {
			t.Fatalf("expected ErrBranchNotFound, got error: %v", err)
		}
	})
}

func TestManager_CreateBranch(t *testing.T) {
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	testCases := []struct {
		Name   string
		Hidden bool
	}{
		{
			Name:   "not hidden",
			Hidden: false,
		},
		{
			Name:   "hidden",
			Hidden: true,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			branchName := graveler.BranchID(tt.Name)
			err = r.CreateBranch(ctx, repository, branchName, graveler.Branch{CommitID: "c1", StagingToken: "s1", Hidden: tt.Hidden})
			testutil.MustDo(t, "create branch", err)

			br, err := r.GetBranch(ctx, repository, branchName)
			testutil.MustDo(t, "get branch", err)
			if br == nil {
				t.Fatal("get branch got nil")
			}
			if br.CommitID != "c1" {
				t.Fatalf("unexpected commit for branch: %s - expected: c1", br.CommitID)
			}
			require.Equal(t, tt.Hidden, br.Hidden)

			// check we can't create existing
			err = r.CreateBranch(ctx, repository, branchName, graveler.Branch{CommitID: "c2", StagingToken: "s2", Hidden: tt.Hidden})
			if !errors.Is(err, graveler.ErrBranchExists) {
				t.Fatalf("CreateBranch() err = %s, expected already exists", err)
			}
			// overwrite by delete and create
			err = r.DeleteBranch(ctx, repository, branchName)
			testutil.MustDo(t, "delete branch", err)

			err = r.CreateBranch(ctx, repository, branchName, graveler.Branch{CommitID: "c2", StagingToken: "s2", Hidden: tt.Hidden})
			testutil.MustDo(t, "create branch", err)

			br, err = r.GetBranch(ctx, repository, branchName)
			testutil.MustDo(t, "get f1 branch", err)

			if br == nil {
				t.Fatal("get branch got nil")
			}
			if br.CommitID != "c2" {
				t.Fatalf("unexpected commit for branch: %s - expected: c2", br.CommitID)
			}
		})
	}
}

func TestManager_SetBranch(t *testing.T) {
	r, _ := testRefManager(t)
	repository, err := r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	testutil.Must(t, r.SetBranch(context.Background(), repository, "branch2", graveler.Branch{
		CommitID: "c2",
	}))

	b, err := r.GetBranch(context.Background(), repository, "branch2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b.CommitID != "c2" {
		t.Fatalf("unexpected commit for branch2: %s - expected: c2", b.CommitID)
	}

	// overwrite
	testutil.Must(t, r.SetBranch(context.Background(), repository, "branch2", graveler.Branch{
		CommitID: "c3",
	}))

	b, err = r.GetBranch(context.Background(), repository, "branch2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b.CommitID != "c3" {
		t.Fatalf("unexpected commit for branch2: %s - expected: c3", b.CommitID)
	}
}

func TestManager_BranchUpdate(t *testing.T) {
	ctx := context.Background()
	r, _ := testRefManager(t)
	const (
		repoID    = "repo1"
		branchID  = "branch1"
		commitID1 = "c1"
		commitID2 = "c2"
	)
	repository, err := r.CreateRepository(context.Background(), repoID, graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)
	tests := []struct {
		name           string
		f              graveler.BranchUpdateFunc
		err            error
		expectedCommit string
	}{
		{
			name: "success_branch_update",
			f: func(*graveler.Branch) (*graveler.Branch, error) {
				newBranch := &graveler.Branch{
					CommitID:     commitID2,
					StagingToken: "",
					SealedTokens: nil,
				}
				return newBranch, nil
			},
			expectedCommit: commitID2,
		},
		{
			name: "failed_branch_update_due_to_branch_change",
			f: func(*graveler.Branch) (*graveler.Branch, error) {
				b := graveler.Branch{
					CommitID: "Another commit during validation",
				}
				_ = r.SetBranch(ctx, repository, branchID, b)
				return &b, nil
			},
			err:            kv.ErrPredicateFailed,
			expectedCommit: "Another commit during validation",
		},
		{
			name: "failed_branch_update_on_validation",
			f: func(*graveler.Branch) (*graveler.Branch, error) {
				return nil, graveler.ErrInvalid
			},
			err:            graveler.ErrInvalid,
			expectedCommit: commitID1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testutil.Must(t, r.SetBranch(context.Background(), repository, branchID, graveler.Branch{
				CommitID: commitID1,
			}))

			err := r.BranchUpdate(ctx, repository, branchID, tt.f)
			require.ErrorIs(t, err, tt.err)

			b, err := r.GetBranch(context.Background(), repository, branchID)
			require.NoError(t, err)
			require.Equal(t, tt.expectedCommit, b.CommitID.String())
		})
	}
}

func TestManager_BranchUpdateRaceCondition(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	executor := batch.NewExecutor(logging.Dummy())
	cfg := ref.ManagerConfig{
		Executor:              executor,
		KVStore:               kvStore,
		KVStoreLimited:        kv.NewStoreLimiter(kvStore, ratelimit.NewUnlimited()),
		AddressProvider:       ident.NewHexAddressProvider(),
		RepositoryCacheConfig: testRepoCacheConfig,
		CommitCacheConfig:     testCommitCacheConfig,
		MaxBatchDelay:         10 * time.Millisecond,
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go executor.Run(cancelCtx)
	r := ref.NewRefManager(cfg, NewStorageConfigMock(config.SingleBlockstoreID))

	const (
		repoID   = "repo1"
		branchID = "race-branch"
	)
	repository, err := r.CreateRepository(context.Background(), repoID, graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://test-bucket",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	// Set initial branch state
	testutil.Must(t, r.SetBranch(ctx, repository, branchID, graveler.Branch{
		CommitID:     "test-commit-id",
		StagingToken: "staging1",
	}))

	// Use goroutines to create a proper race condition
	var wg sync.WaitGroup

	// Start goroutines that will modify the branch concurrently
	const concurrentGoroutines = 5
	wg.Add(concurrentGoroutines)
	for i := range concurrentGoroutines {
		go func() {
			defer wg.Done()

			_ = r.BranchUpdate(ctx, repository, branchID, func(branch *graveler.Branch) (*graveler.Branch, error) {
				// Modify the branch information
				branch.StagingToken = graveler.StagingToken("staging" + strconv.Itoa(i))
				branch.SealedTokens = slices.Repeat([]graveler.StagingToken{branch.StagingToken}, i)
				return branch, nil
			})
		}()
	}
	wg.Wait()

	branch, err := r.GetBranch(ctx, repository, branchID)
	require.NoError(t, err)
	require.NotNil(t, branch)
}

func TestManager_DeleteBranch(t *testing.T) {
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	testutil.Must(t, r.SetBranch(ctx, repository, "branch2", graveler.Branch{
		CommitID: "c2",
	}))

	testutil.Must(t, r.DeleteBranch(ctx, repository, "branch2"))

	_, err = r.GetBranch(ctx, repository, "branch2")
	if !errors.Is(err, graveler.ErrBranchNotFound) {
		t.Fatalf("Expected ErrBranchNotFound, got error: %v", err)
	}
}

func TestManager_ListBranches(t *testing.T) {
	r, _ := testRefManager(t)
	repository, err := r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	visibleBranches := []graveler.BranchID{"a", "ab", "ca", "ba", "za", "fa"}
	hiddenBranches := []graveler.BranchID{"aa", "ac", "cb", "bb", "zb", "fb"}
	allBranches := slices.Concat(visibleBranches, hiddenBranches)
	for _, b := range visibleBranches {
		testutil.Must(t, r.SetBranch(context.Background(), repository, b, graveler.Branch{
			CommitID: "c2",
		}))
	}
	for _, b := range hiddenBranches {
		testutil.Must(t, r.SetBranch(context.Background(), repository, b, graveler.Branch{
			CommitID: "c2",
			Hidden:   true,
		}))
	}

	// List only visible branches
	iter, err := r.ListBranches(context.Background(), repository, graveler.ListOptions{ShowHidden: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer iter.Close()

	var bvs []graveler.BranchID
	for iter.Next() {
		b := iter.Value()
		bvs = append(bvs, b.BranchID)
	}
	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}
	visibleBranches = append(visibleBranches, "main")
	sort.Slice(visibleBranches, func(i, j int) bool {
		return visibleBranches[i] < visibleBranches[j]
	})
	if !reflect.DeepEqual(bvs, visibleBranches) {
		t.Fatalf("unexpected branch list: %v", bvs)
	}

	// List all branches
	iter, err = r.ListBranches(context.Background(), repository, graveler.ListOptions{ShowHidden: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer iter.Close()

	var bs []graveler.BranchID
	for iter.Next() {
		b := iter.Value()
		bs = append(bs, b.BranchID)
	}
	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}
	allBranches = append(allBranches, "main")
	sort.Slice(allBranches, func(i, j int) bool {
		return allBranches[i] < allBranches[j]
	})
	if !reflect.DeepEqual(bs, allBranches) {
		t.Fatalf("unexpected branch list: %v", bs)
	}
}

func TestManager_GetTag(t *testing.T) {
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.MustDo(t, "create repo", err)

	t.Run("exists", func(t *testing.T) {
		err := r.CreateTag(ctx, repository, "v1.0", "c1")
		testutil.MustDo(t, "set tag", err)
		commitID, err := r.GetTag(context.Background(), repository, "v1.0")
		testutil.MustDo(t, "get existing tag", err)
		if commitID == nil {
			t.Fatal("get tag, missing commit id")
		}
		if *commitID != "c1" {
			t.Fatalf("get tag, commit id: %s, expected c1", *commitID)
		}
	})

	t.Run("not_exists", func(t *testing.T) {
		commitID, err := r.GetTag(context.Background(), repository, "v1.bad")
		if !errors.Is(err, graveler.ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got error: %v", err)
		}
		if commitID != nil {
			t.Fatalf("get not existing commitID: %s, expected nil", *commitID)
		}
	})
}

func TestManager_CreateTag(t *testing.T) {
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	err = r.CreateTag(ctx, repository, "v2", "c2")
	testutil.MustDo(t, "create tag v2", err)

	commit, err := r.GetTag(ctx, repository, "v2")
	testutil.MustDo(t, "get v2 tag", err)
	if commit == nil {
		t.Fatal("get tag got nil")
	}
	if *commit != "c2" {
		t.Fatalf("unexpected commit for tag v2: %s - expected: c2", *commit)
	}

	// check we can't create existing
	err = r.CreateTag(ctx, repository, "v2", "c5")
	if !errors.Is(err, graveler.ErrTagAlreadyExists) {
		t.Fatalf("CreateTag() err = %s, expected already exists", err)
	}
	// overwrite by delete and create
	err = r.DeleteTag(ctx, repository, "v2")
	testutil.MustDo(t, "delete tag v2", err)

	err = r.CreateTag(ctx, repository, "v2", "c3")
	testutil.MustDo(t, "re-create tag v2", err)

	commit, err = r.GetTag(ctx, repository, "v2")
	testutil.MustDo(t, "get tag v2", err)
	if commit == nil {
		t.Fatal("get tag got nil")
	}
	if *commit != "c3" {
		t.Fatalf("unexpected commit for v2: %s - expected: c3", *commit)
	}
}

func TestManager_DeleteTag(t *testing.T) {
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)
	testutil.Must(t, r.CreateTag(ctx, repository, "v1", "c2"))
	testutil.Must(t, r.DeleteTag(ctx, repository, "v1"))
	commitID, err := r.GetTag(ctx, repository, "v1")
	if !errors.Is(err, graveler.ErrNotFound) {
		t.Fatal("unexpected error:", err)
	}
	if commitID != nil {
		t.Fatal("expected commit ID:", *commitID)
	}
}

func TestManager_ListTags(t *testing.T) {
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	var commitsTagged []graveler.CommitID
	tags := []string{"tag-a", "tag-b", "the-end", "v1", "v1.1"}
	sort.Strings(tags)
	for i, tag := range tags {
		commitID := graveler.CommitID(fmt.Sprintf("c%d", i))
		commitsTagged = append(commitsTagged, commitID)
		err := r.CreateTag(ctx, repository, graveler.TagID(tag), commitID)
		testutil.MustDo(t, "set tag "+tag, err)
	}

	iter, err := r.ListTags(ctx, repository)
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	defer iter.Close()
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
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z00:00")
	c := graveler.Commit{
		Committer:    "user1",
		Message:      "message1",
		MetaRangeID:  "deadbeef123",
		CreationDate: ts,
		Parents:      graveler.CommitParents{"deadbeef1", "deadbeef12"},
		Metadata:     graveler.Metadata{"foo": "bar"},
	}

	cid, err := r.AddCommit(ctx, repository, c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	const expectedCommitID = "2277b5abd2d3ba6b4d35c48a0e358b0c4bcf5cd6d891c67437fb4c4af0d2fd4b"
	if cid != expectedCommitID {
		t.Fatalf("Commit ID '%s', expected '%s'", cid, expectedCommitID)
	}

	commit, err := r.GetCommit(ctx, repository, cid)
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
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

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
		cid, err := r.AddCommit(ctx, repository, c)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		previous = cid
		ts = ts.Add(time.Second)
	}

	iter, err := r.Log(ctx, repository, previous, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer iter.Close()

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
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.MustDo(t, "Create repository", err)
	dag := map[string][]string{
		"c1": {},
		"c2": {"c1"},
		"c3": {"c1"},
		"c4": {"c2"},
		"c5": {"c3"},
		"c6": {"c5"},
		"c7": {"c4"},
		"c8": {"c6", "c7"},
	}
	tests := map[string]struct {
		firstParent bool
		seek        string
		start       string
		since       time.Time
		expected    []string
	}{
		/*
			---1----2----4----7
			    \	           \
				 3----5----6----8---
		*/
		"full_graph": {
			start:    "c8",
			expected: []string{"c8", "c7", "c6", "c5", "c4", "c3", "c2", "c1"},
		},
		"full_graph_first_parent": {
			start:       "c8",
			firstParent: true,
			expected:    []string{"c8", "c6", "c5", "c3", "c1"},
		},
		"with_seek": {
			start:    "c8",
			seek:     "c4",
			expected: []string{"c4", "c3", "c2", "c1"},
		},
		"with_seek_first_parent": {
			start:       "c8",
			seek:        "c5",
			firstParent: true,
			expected:    []string{"c5", "c3", "c1"},
		},
		"start_from": {
			start:    "c7",
			expected: []string{"c7", "c4", "c2", "c1"},
		},
		"since": {
			start:    "c8",
			since:    time.Date(2020, time.December, 1, 15, 5, 0, 0, time.UTC),
			expected: []string{"c8", "c7", "c6", "c5"},
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			nextCommitTS := time.Date(2020, time.December, 1, 15, 0, 0, 0, time.UTC)
			commitNameToID := map[string]graveler.CommitID{}
			addCommit := func(commitName string, parentNames ...string) graveler.CommitID {
				nextCommitTS = nextCommitTS.Add(time.Minute)
				parentIDs := make([]graveler.CommitID, 0, len(parentNames))
				for _, parentName := range parentNames {
					parentIDs = append(parentIDs, commitNameToID[parentName])
				}
				c := graveler.Commit{
					Committer:    "user1",
					Message:      commitName,
					MetaRangeID:  "fefe1221",
					CreationDate: nextCommitTS,
					Parents:      parentIDs,
					Metadata:     graveler.Metadata{"foo": "bar"},
				}
				cid, err := r.AddCommit(ctx, repository, c)
				commitNameToID[commitName] = cid
				testutil.MustDo(t, "Add commit "+commitName, err)
				return cid
			}
			commitNames := make([]string, 0, len(dag))
			for commitName := range dag {
				commitNames = append(commitNames, commitName)
			}
			sort.Strings(commitNames)
			for _, commitName := range commitNames {
				addCommit(commitName, dag[commitName]...)
			}

			// setup time since
			var since *time.Time
			if !tst.since.IsZero() {
				since = &tst.since
			}

			it, err := r.Log(ctx, repository, commitNameToID[tst.start], tst.firstParent, since)
			if err != nil {
				t.Fatal("Error during create Log iterator", err)
			}
			defer it.Close()
			if tst.seek != "" {
				it.SeekGE(commitNameToID[tst.seek])
			}
			var commits []string
			for it.Next() {
				c := it.Value()
				commits = append(commits, c.Message)
			}
			if err := it.Err(); err != nil {
				t.Fatal("Iteration ended with error", err)
			}
			if diff := deep.Equal(commits, tst.expected); diff != nil {
				t.Fatal("Found diff between expected commits:", diff)
			}
		})
	}
}

func TestConsistentCommitIdentity(t *testing.T) {
	addressProvider := ident.NewHexAddressProvider()
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
	// If this is failing, and you're tempted to change this value,
	// then you are probably introducing a breaking change to commits identity.
	// All previous references to commits (if not migrated) may be lost.
	const expected = "f1a106bbeb12d3eb54418d6000f4507501d289d0d0879dcce6f4d31425587df1"

	// Running many times to check that it's actually consistent (see issue #1291)
	const iterations = 50

	for i := 0; i < iterations; i++ {
		res := addressProvider.ContentAddress(commit)
		assert.Equalf(t, expected, res, "iteration %d content mismatch", i+1)
	}
}

func TestManager_GetCommitByPrefix(t *testing.T) {
	commitIDs := []string{"c1234", "d1", "b1", "c1245", "a1"}
	identityToFakeIdentity := make(map[string]string)

	provider := &fakeAddressProvider{identityToFakeIdentity: identityToFakeIdentity}
	r, _ := testRefManagerWithAddressProvider(t, provider)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.MustDo(t, "Create repository", err)
	for _, commitID := range commitIDs {
		c := graveler.Commit{
			Committer:    "user1",
			Message:      fmt.Sprintf("id_%s", commitID),
			MetaRangeID:  "deadbeef123",
			CreationDate: time.Now(),
			Parents:      graveler.CommitParents{"deadbeef1"},
			Metadata:     graveler.Metadata{"foo": "bar"},
		}
		identityToFakeIdentity[hex.EncodeToString(c.Identity())] = commitID
		_, err := r.AddCommit(ctx, repository, c)
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
			c, err := r.GetCommitByPrefix(ctx, repository, graveler.CommitID(tst.Prefix))
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

func TestManager_ListCommits(t *testing.T) {
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)
	nextCommitNumber := 0
	addNextCommit := func(parents ...graveler.CommitID) graveler.CommitID {
		nextCommitNumber++
		id := "c" + strconv.Itoa(nextCommitNumber)
		c := graveler.Commit{
			Message: id,
			Parents: parents,
		}
		cid, err := r.AddCommit(ctx, repository, c)
		testutil.MustDo(t, "Add commit "+id, err)
		return cid
	}
	c1 := addNextCommit()
	c2 := addNextCommit(c1)
	c3 := addNextCommit(c1)
	c4 := addNextCommit(c2)
	c5 := addNextCommit(c3)
	c6 := addNextCommit(c5)
	addNextCommit(c4)
	addNextCommit(c6, c1)
	/*
	 1----2----4---7
	 | \
	 |  3----5----6
	 |             \
	 ---------------8
	*/

	iter, err := r.ListCommits(ctx, repository)
	testutil.MustDo(t, "fill generations", err)
	defer iter.Close()
	var lastCommit string
	var i int
	for iter.Next() {
		commit := iter.Value()
		if i == 0 {
			gravelerCommitReflection := reflect.Indirect(reflect.ValueOf(graveler.Commit{}))
			listCommitReflection := reflect.Indirect(reflect.ValueOf(*commit.Commit))
			listCommitFields := make(map[string]struct{})
			for i := 0; i < listCommitReflection.NumField(); i++ {
				listCommitFields[listCommitReflection.Type().Field(i).Name] = struct{}{}
			}
			for i := 0; i < gravelerCommitReflection.NumField(); i++ {
				fieldName := gravelerCommitReflection.Type().Field(i).Name
				_, exists := listCommitFields[fieldName]
				if !exists {
					t.Errorf("missing field: %s in commit response from list commits.", fieldName)
				}
			}
		} else if string(commit.CommitID) < lastCommit {
			t.Errorf("wrong commitId order for commit number%d in ListCommits response. commitId: %s came after commitId: %s", i, string(commit.CommitID), lastCommit)
		}
		lastCommit = string(commit.CommitID)
		i++
	}
}

func TestManager_DeleteExpiredImports(t *testing.T) {
	r, store := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	imports := []*graveler.ImportStatusData{
		{
			Id:        "not_expired1",
			Completed: false,
			UpdatedAt: timestamppb.New(time.Now()),
			Error:     "An error",
		},
		{
			Id:        "not_expired2",
			Completed: false,
			UpdatedAt: timestamppb.New(time.Now().Add(-ref.ImportExpiryTime + time.Hour)),
			Error:     "An error",
		},
		{
			Id:        "expired",
			Completed: true,
			UpdatedAt: timestamppb.New(time.Now().Add(-ref.ImportExpiryTime - time.Hour)),
			Error:     "",
		},
		{
			Id:        "stale",
			Completed: false,
			UpdatedAt: timestamppb.New(time.Now().Add(-ref.ImportExpiryTime - time.Hour)),
			Error:     "",
		},
	}

	repoPartition := graveler.RepoPartition(repository)
	for _, i := range imports {
		data, err := proto.Marshal(i)
		require.NoError(t, err)
		err = store.Set(ctx, []byte(repoPartition), []byte(graveler.ImportsPath(i.Id)), data)
		require.NoError(t, err)
	}

	err = r.DeleteExpiredImports(context.Background(), repository)
	require.NoError(t, err)

	it, err := kv.NewPrimaryIterator(ctx, store, (&graveler.ImportStatusData{}).ProtoReflect().Type(), repoPartition, []byte(graveler.ImportsPath("")), kv.IteratorOptionsFrom([]byte("")))
	require.NoError(t, err)
	defer it.Close()

	count := 0
	for it.Next() {
		entry := it.Entry()
		count += 1
		id := string(entry.Key)
		require.True(t, strings.HasPrefix(id, "imports/not_expired"), id)
	}
	require.NoError(t, it.Err())
	require.Equal(t, 2, count)
}

func TestManager_GetRepositoryMetadata(t *testing.T) {
	ctx := context.Background()
	r, _ := testRefManager(t)
	const (
		repoID = "repo1"
	)
	repository, err := r.CreateRepository(context.Background(), repoID, graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	t.Run("get_on_non_existing_repo", func(t *testing.T) {
		_, err := r.GetRepositoryMetadata(ctx, "not_exist")
		require.ErrorIs(t, err, graveler.ErrNotFound)
	})

	t.Run("basic", func(t *testing.T) {
		metadata, err := r.GetRepositoryMetadata(ctx, repository.RepositoryID)
		require.NoError(t, err)
		require.Nil(t, metadata)
	})
}

func TestManager_SetRepositoryMetadata(t *testing.T) {
	ctx := context.Background()
	r, store := testRefManager(t)
	const (
		repoID = "repo1"
		key    = "test_key"
	)
	repository, err := r.CreateRepository(context.Background(), repoID, graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)
	tests := []struct {
		name             string
		f                graveler.RepoMetadataUpdateFunc
		err              error
		expectedMetadata graveler.RepositoryMetadata
	}{
		{
			name: "success_branch_update",
			f: func(metadata graveler.RepositoryMetadata) (graveler.RepositoryMetadata, error) {
				metadata[key] = "success"
				return metadata, nil
			},
			expectedMetadata: graveler.RepositoryMetadata{key: "success"},
		},
		{
			name: "failed_metadata_update_due_to_changes",
			f: func(metadata graveler.RepositoryMetadata) (graveler.RepositoryMetadata, error) {
				m := graveler.RepoMetadata{
					Metadata: graveler.RepositoryMetadata{
						key: "failed",
					},
				}
				_ = kv.SetMsg(ctx, store, graveler.RepoPartition(repository), []byte(graveler.RepoMetadataPath()), &m)
				metadata[key] = "not_expected"
				return metadata, nil
			},
			err:              kv.ErrPredicateFailed,
			expectedMetadata: graveler.RepositoryMetadata{key: "failed"},
		},
		{
			name: "failed_update_on_validation",
			f: func(metadata graveler.RepositoryMetadata) (graveler.RepositoryMetadata, error) {
				return nil, graveler.ErrInvalid
			},
			err:              graveler.ErrInvalid,
			expectedMetadata: graveler.RepositoryMetadata{key: "failed"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = r.SetRepositoryMetadata(ctx, repository, tt.f)
			require.ErrorIs(t, err, tt.err)

			metadata, err := r.GetRepositoryMetadata(ctx, repository.RepositoryID)
			require.NoError(t, err)
			require.Equal(t, tt.expectedMetadata, metadata)
		})
	}
}

func TestManager_GetPullRequest(t *testing.T) {
	r, store := testRefManager(t)
	repository, err := r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)
	ctx := context.Background()

	t.Run("get_pull_request_exists", func(t *testing.T) {
		mergeCommitID := "abc"
		expected := graveler.PullRequestRecord{
			ID: "pullID",
			PullRequest: graveler.PullRequest{
				CreationDate:   time.Now().UTC(),
				Status:         graveler.PullRequestStatus_CLOSED,
				Title:          "some title",
				Author:         "some author",
				Description:    "some description",
				Source:         "dev",
				Destination:    "main",
				MergedCommitID: &mergeCommitID,
			},
		}
		require.NoError(t, r.CreatePullRequest(ctx, repository, expected.ID, &expected.PullRequest))

		// Verify secondary index
		data, err := store.Get(ctx, []byte(ref.PullsPartitionKey), []byte(ref.PullBySrcDstPath(repository, expected.Source, expected.Destination)))
		require.NoError(t, err)
		sec := kv.SecondaryIndex{}
		require.NoError(t, proto.Unmarshal(data.Value, &sec))
		require.Equal(t, expected.ID.String(), string(sec.PrimaryKey))

		// Verify we can get pull from secondary index
		pull, err := r.GetPullRequest(ctx, repository, graveler.PullRequestID(sec.PrimaryKey))
		require.NoError(t, err)
		require.Equal(t, expected.PullRequest, *pull)
	})

	t.Run("get_pull_request_doesnt_exists", func(t *testing.T) {
		_, err := r.GetPullRequest(ctx, repository, "pull2")
		require.ErrorIs(t, err, graveler.ErrPullRequestNotFound)
	})
}

func TestManager_DeletePullRequest(t *testing.T) {
	r, store := testRefManager(t)
	repository, err := r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)
	ctx := context.Background()

	t.Run("delete_pull_request_exists", func(t *testing.T) {
		rec := graveler.PullRequestRecord{
			ID: "",
			PullRequest: graveler.PullRequest{
				CreationDate:   time.Now().UTC(),
				Status:         graveler.PullRequestStatus_CLOSED,
				Title:          "some title",
				Author:         "some author",
				Description:    "some description",
				Source:         "dev",
				Destination:    "main",
				MergedCommitID: nil,
			},
		}
		require.NoError(t, r.CreatePullRequest(ctx, repository, rec.ID, &rec.PullRequest))

		// Delete Pull request
		err := r.DeletePullRequest(ctx, repository, rec.ID)
		require.NoError(t, err)

		// Verify secondary index deleted
		_, err = store.Get(ctx, []byte(ref.PullsPartitionKey), []byte(ref.PullBySrcDstPath(repository, rec.Source, rec.Destination)))
		require.ErrorIs(t, err, kv.ErrNotFound)
	})

	t.Run("delete_pull_request_doesnt_exists", func(t *testing.T) {
		// No error expected if key not found
		require.NoError(t, r.DeletePullRequest(ctx, repository, "pull2"))
	})
}

func TestManager_UpdatePullRequest(t *testing.T) {
	r, _ := testRefManager(t)
	repository, err := r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)
	ctx := context.Background()
	expected := graveler.PullRequestRecord{
		ID: "",
		PullRequest: graveler.PullRequest{
			CreationDate:   time.Now().UTC(),
			Status:         graveler.PullRequestStatus_CLOSED,
			Title:          "some title",
			Author:         "some author",
			Description:    "some description",
			Source:         "dev",
			Destination:    "main",
			MergedCommitID: nil,
		},
	}

	t.Run("update_pull_request_exists", func(t *testing.T) {
		require.NoError(t, r.CreatePullRequest(ctx, repository, expected.ID, &expected.PullRequest))

		err := r.UpdatePullRequest(ctx, repository, expected.ID, func(request *graveler.PullRequest) (*graveler.PullRequest, error) {
			// Modify pull
			expected.Title = "foo"
			expected.Description = "bar"
			expected.Destination = "abc"
			return &expected.PullRequest, nil
		})
		require.NoError(t, err)
		pull, err := r.GetPullRequest(ctx, repository, expected.ID)
		require.NoError(t, err)
		require.Equal(t, expected.PullRequest, *pull)
	})

	t.Run("update_pull_request_doesnt_exists", func(t *testing.T) {
		// No error expected if key not found
		require.ErrorIs(t, r.UpdatePullRequest(ctx, repository, "pull2", func(request *graveler.PullRequest) (*graveler.PullRequest, error) {
			t.Fatalf("Should not reach")
			return nil, nil
		}), graveler.ErrPullRequestNotFound)
	})
}
