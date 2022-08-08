package ref_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/branch"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
	"google.golang.org/protobuf/proto"
)

const (
	NumRepositories = 1
	numCommits      = 100
	numBranches     = 100

	blockstoreCommittedPrefix = "migrate_test"
)

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	conn, _ := testutil.GetDB(t, databaseURI)
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	blockstore := mem.New()

	store, err := kv.Open(context.Background(), kvparams.KV{Type: kvpg.DriverName, Postgres: &kvparams.Postgres{ConnectionString: databaseURI}})
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
	createMigrateTestData(t, ctx, dbMgr, blockstore)

	buf := bytes.Buffer{}
	err = ref.MigrateWithBlockstore(ctx, conn.Pool(), &buf, blockstore, blockstoreCommittedPrefix)
	require.NoError(t, err)

	testutil.MustDo(t, "Import file", kv.Import(ctx, &buf, kvStore.Store))
	kvMgr := ref.NewKVRefManager(batch.NopExecutor(), kvStore, ident.NewHexAddressProvider())
	settingsMgr := settings.NewManager(kvMgr, kv.StoreMessage{Store: kvStore.Store})
	verifyMigrationResults(t, ctx, kvMgr, dbMgr, settingsMgr)
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

func createMigrateTestData(t *testing.T, ctx context.Context, mgr graveler.RefManager, blockstore block.Adapter) {
	for i := 0; i < NumRepositories; i++ {
		repoID := graveler.RepositoryID("repo_" + strconv.Itoa(i))
		repo := graveler.Repository{StorageNamespace: graveler.StorageNamespace(strconv.Itoa(i)), CreationDate: time.Now(), DefaultBranchID: "main"}
		if err := mgr.CreateRepository(ctx, repoID, repo); err != nil {
			t.Fatalf("Create Repository: %s", err)
		}
		// Add repo settings for every second repo
		if i%2 == 0 {
			s := settings.ExampleSettings{
				ExampleInt: int32(i),
				ExampleStr: repoID.String(),
			}
			data, err := proto.Marshal(&s)
			testutil.Must(t, err)

			objectPointer := block.ObjectPointer{
				StorageNamespace: string(repo.StorageNamespace),
				Identifier:       fmt.Sprintf(graveler.SettingsRelativeKey, blockstoreCommittedPrefix, branch.ProtectionSettingKey),
				IdentifierType:   block.IdentifierTypeRelative,
			}
			testutil.Must(t, blockstore.Put(ctx, objectPointer, int64(len(data)), bytes.NewReader(data), block.PutOpts{}))
		}

		createTagInitialCommit(t, ctx, mgr, repoID)

		for b := 0; b < numBranches; b++ {
			if err := mgr.CreateBranch(ctx, repoID, graveler.BranchID("branch_"+strconv.Itoa(b)),
				graveler.Branch{CommitID: graveler.CommitID(strconv.Itoa(b)),
					StagingToken: graveler.StagingToken("test_token_" + strconv.Itoa(b)),
					SealedTokens: []graveler.StagingToken{"token"},
				}); err != nil {
				t.Fatalf("Create Branch: %s", err)
			}
		}
		for c := 0; c < numCommits; c++ {
			commitID, err := mgr.AddCommit(ctx, repoID,
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
		b := branches.Value()
		bkv, err := kvMgr.GetBranch(ctx, repoID, b.BranchID)
		require.NoError(t, err)

		bdb, err := dbMgr.GetBranch(ctx, repoID, b.BranchID)
		require.NoError(t, err)
		require.Equal(t, bkv, bdb)
		branchesNum += 1
	}
	require.Equal(t, branchesNum, numBranches+1)
}

func verifyMigrationResults(t *testing.T, ctx context.Context, kvMgr, dbMgr graveler.RefManager, settingsMgr settings.Manager) {
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
		require.Equal(t, graveler.RepositoryState_ACTIVE, kvr.State)
		require.True(t, len(kvr.InstanceUID) == 20) // valid xid length
		// Fill DB missing fields
		dbr.State = graveler.RepositoryState_ACTIVE
		dbr.InstanceUID = kvr.InstanceUID
		require.Equal(t, kvr, dbr)

		// verify settings
		testSettings := settings.ExampleSettings{}
		data, err := settingsMgr.Get(ctx, repo.RepositoryID, branch.ProtectionSettingKey, &testSettings)
		if repoNum%2 == 0 {
			require.Equal(t, data.(*settings.ExampleSettings).ExampleStr, repo.RepositoryID.String())
			require.Equal(t, data.(*settings.ExampleSettings).ExampleInt, int32(repoNum))
		} else {
			require.ErrorIs(t, err, graveler.ErrNotFound)
		}

		// verify repository entities
		verifyCommitTagResults(t, ctx, kvMgr, dbMgr, repo.RepositoryID)
		verifyBranchResults(t, ctx, kvMgr, dbMgr, repo.RepositoryID)
		repoNum += 1
	}
	require.Equal(t, repoNum, NumRepositories)
}
