package ref_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	NumRepositories = 10
	numCommits      = 100
	numBranches     = 100
)

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	conn, _ := testutil.GetDB(t, databaseURI)
	store, err := kv.Open(context.Background(), kvpg.DriverName, kvparams.KV{Postgres: &kvparams.Postgres{ConnectionString: databaseURI}})
	testutil.MustDo(t, "Open KV Store", err)
	kvStore := kv.StoreMessage{Store: store}
	t.Cleanup(func() {
		_, err = conn.Pool().Exec(ctx, `TRUNCATE kv`)
		if err != nil {
			t.Fatalf("failed to delete KV table from postgres DB %s", err)
		}
		store.Close()
	})
	dbMgr := ref.NewPGRefManager(batch.NopExecutor(), conn, ident.NewHexAddressProvider())

	createMigrateTestData(t, ctx, dbMgr)

	buf := bytes.Buffer{}
	err = ref.Migrate(ctx, conn.Pool(), &buf)
	require.NoError(t, err)

	testutil.MustDo(t, "Import file", kv.Import(ctx, &buf, kvStore.Store))
	kvMgr := ref.NewKVRefManager(batch.NopExecutor(), kvStore, ident.NewHexAddressProvider())
	verifyMigrationResults(t, ctx, kvMgr, dbMgr)
}

func createTagInitialCommit(t *testing.T, ctx context.Context, mgr graveler.RefManager, repoID graveler.RepositoryID) {
	res := make([]graveler.CommitRecord, 0)
	commits, err := mgr.ListCommits(ctx, repoID)
	require.NoError(t, err)
	defer commits.Close()
	for commits.Next() {
		res = append(res, *commits.Value())
	}
	require.Equal(t, len(res), 1)

	if err := mgr.CreateTag(ctx, repoID, graveler.TagID("tag_"+string(res[0].CommitID)), res[0].CommitID); err != nil {
		t.Fatalf("Create Tag: %s", err)
	}
}

func createMigrateTestData(t *testing.T, ctx context.Context, mgr graveler.RefManager) {
	for i := 0; i < NumRepositories; i++ {
		repoId := graveler.RepositoryID("repo_" + strconv.Itoa(i))
		if err := mgr.CreateRepository(ctx, repoId,
			graveler.Repository{StorageNamespace: graveler.StorageNamespace(strconv.Itoa(i)), CreationDate: time.Now(), DefaultBranchID: "main"},
			graveler.StagingToken("test_token_"+strconv.Itoa(i))); err != nil {
			t.Fatalf("Create Repository: %s", err)
		}
		createTagInitialCommit(t, ctx, mgr, repoId)

		for b := 0; b < numBranches; b++ {
			if err := mgr.CreateBranch(ctx, repoId, graveler.BranchID("branch_"+strconv.Itoa(b)),
				graveler.Branch{CommitID: graveler.CommitID(strconv.Itoa(b)),
					StagingToken: graveler.StagingToken("test_token_" + strconv.Itoa(b)),
					SealedTokens: []graveler.StagingToken{"token"},
				}); err != nil {
				t.Fatalf("Create Branch: %s", err)
			}
		}
		for c := 0; c < numCommits; c++ {
			commitID, err := mgr.AddCommit(ctx, repoId,
				graveler.Commit{Version: graveler.CurrentCommitVersion,
					Committer:    "tester",
					Message:      strconv.Itoa(c),
					MetaRangeID:  "meta_range_id",
					CreationDate: time.Now().UTC(),
					Parents:      graveler.CommitParents{graveler.CommitID(strconv.Itoa(c))},
					Metadata:     map[string]string{"data": strconv.Itoa(c)},
					Generation:   1,
				})
			if err != nil {
				t.Fatalf("Create Commit: %s", err)
			}

			if err := mgr.CreateTag(ctx, graveler.RepositoryID("repo_"+strconv.Itoa(i)), graveler.TagID("tag_"+commitID), commitID); err != nil {
				t.Fatalf("Create Tag: %s", err)
			}
		}
	}
}

func verifyCommitTagResults(t *testing.T, ctx context.Context, kvMgr, dbMgr graveler.RefManager, repoID graveler.RepositoryID) {
	commits, err := kvMgr.ListCommits(ctx, repoID)
	require.NoError(t, err)
	defer commits.Close()
	commitNum := 0
	for commits.Next() {
		commit := commits.Value()
		kvc, err := kvMgr.GetCommit(ctx, repoID, commit.CommitID)
		require.NoError(t, err)
		dbm, err := dbMgr.GetCommit(ctx, repoID, commit.CommitID)
		require.NoError(t, err)
		dbm.CreationDate = dbm.CreationDate.UTC()
		require.Equal(t, kvc, dbm)
		commitNum += 1

		// verify tag
		tag, err := kvMgr.GetTag(ctx, repoID, graveler.TagID("tag_"+commit.CommitID))
		require.NoError(t, err)
		require.Equal(t, *tag, commit.CommitID)
	}
	require.Equal(t, commitNum, numCommits+1)

	tags, err := kvMgr.ListTags(ctx, repoID)
	require.NoError(t, err)
	defer tags.Close()
	numTags := 0
	for tags.Next() {
		numTags += 1
	}
	require.Equal(t, commitNum, numTags)
}

func verifyBranchResults(t *testing.T, ctx context.Context, kvMgr, dbMgr graveler.RefManager, repoID graveler.RepositoryID) {
	branches, err := kvMgr.ListBranches(ctx, repoID)
	require.NoError(t, err)
	defer branches.Close()
	branchesNum := 0
	for branches.Next() {
		branch := branches.Value()
		bkv, err := kvMgr.GetBranch(ctx, repoID, branch.BranchID)
		require.NoError(t, err)

		bdb, err := dbMgr.GetBranch(ctx, repoID, branch.BranchID)
		require.NoError(t, err)
		require.Equal(t, bkv, bdb)
		branchesNum += 1
	}
	require.Equal(t, branchesNum, numBranches+1)
}

func verifyMigrationResults(t *testing.T, ctx context.Context, kvMgr, dbMgr graveler.RefManager) {
	repos, err := kvMgr.ListRepositories(ctx)
	require.NoError(t, err)
	defer repos.Close()
	repoNum := 0
	for repos.Next() {
		repo := repos.Value()
		kvr, err := kvMgr.GetRepository(ctx, repo.RepositoryID)
		require.NoError(t, err)
		dbr, err := dbMgr.GetRepository(ctx, repo.RepositoryID)
		require.NoError(t, err)
		dbr.CreationDate = dbr.CreationDate.UTC()
		require.Equal(t, kvr, dbr)
		repoNum += 1

		// verify repository entities
		verifyCommitTagResults(t, ctx, kvMgr, dbMgr, repo.RepositoryID)
		verifyBranchResults(t, ctx, kvMgr, dbMgr, repo.RepositoryID)
	}
	require.Equal(t, repoNum, NumRepositories)
}
