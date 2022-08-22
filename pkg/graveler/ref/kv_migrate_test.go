package ref_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/branch"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/graveler/staging"
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
	numBranches     = 50
	numStaging      = 100

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
	t.Log("Create tests data")
	createMigrateTestData(t, ctx, dbMgr, conn, blockstore)

	buf := bytes.Buffer{}
	t.Log("Run Migrate")
	err = ref.MigrateWithBlockstore(ctx, conn.Pool(), &buf, blockstore, blockstoreCommittedPrefix)
	require.NoError(t, err)

	testutil.MustDo(t, "Import file", kv.Import(ctx, &buf, kvStore.Store))
	kvMgr := ref.NewKVRefManager(batch.NopExecutor(), kvStore, ident.NewHexAddressProvider())
	settingsMgr := settings.NewManager(kvMgr, kvStore)
	stagingMgr := staging.NewManager(ctx, kvStore)
	t.Log("Verify results")
	verifyMigrationResults(t, ctx, kvMgr, dbMgr, stagingMgr, settingsMgr)
}

func createTagInitialCommit(t *testing.T, ctx context.Context, mgr graveler.RefManager, repository *graveler.RepositoryRecord) {
	res := make([]graveler.CommitRecord, 0)
	commits, err := mgr.ListCommits(ctx, repository)
	require.NoError(t, err)
	defer commits.Close()
	for commits.Next() {
		res = append(res, *commits.Value())
	}
	require.Equal(t, len(res), 1)

	if err := mgr.CreateTag(ctx, repository, graveler.TagID("tag_"+string(res[0].CommitID)), res[0].CommitID); err != nil {
		t.Fatalf("Create Tag: %s", err)
	}
}

func createMigrateTestData(t *testing.T, ctx context.Context, mgr graveler.RefManager, dbPool db.Database, blockstore block.Adapter) {
	t.Helper()
	for i := 0; i < NumRepositories; i++ {
		repoID := graveler.RepositoryID("repo_" + strconv.Itoa(i))
		repo := graveler.Repository{StorageNamespace: graveler.StorageNamespace(strconv.Itoa(i)), CreationDate: time.Now(), DefaultBranchID: "main"}
		repository, err := mgr.CreateRepository(ctx, repoID, repo)
		if err != nil {
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

		createTagInitialCommit(t, ctx, mgr, repository)

		for b := 0; b < numBranches; b++ {
			name := "branch_" + strconv.Itoa(b)
			stagingToken := graveler.StagingToken(name + "_test_token_" + strconv.Itoa(b))
			if err := mgr.CreateBranch(ctx, repository, graveler.BranchID(name),
				graveler.Branch{CommitID: graveler.CommitID(strconv.Itoa(b)),
					StagingToken: stagingToken,
					SealedTokens: []graveler.StagingToken{"token"},
				}); err != nil {
				t.Fatalf("Create Branch: %s", err)
			}
			createStagingData(t, ctx, dbPool, name, stagingToken)
		}

		for c := 0; c < numCommits; c++ {
			commitID, err := mgr.AddCommit(ctx, repository,
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

			if err := mgr.CreateTag(ctx, repository, graveler.TagID("tag_"+commitID), commitID); err != nil {
				t.Fatalf("Create Tag: %s", err)
			}
		}
	}
}

func createStagingData(t *testing.T, ctx context.Context, dbPool db.Database, branch string, stagingToken graveler.StagingToken) {
	mgr := staging.NewDBManager(dbPool)
	for i := 0; i < numStaging; i++ {
		var data *graveler.Value
		if i%2 == 0 { // every second element is a tombstone
			data = &graveler.Value{
				Identity: []byte(fmt.Sprintf("%s_%s", branch, stagingToken)),
				Data:     []byte(fmt.Sprintf("%s_%s", branch, stagingToken))}
		}
		err := mgr.Set(ctx, stagingToken, graveler.Key(strconv.Itoa(i)), data, false)
		testutil.MustDo(t, "Create staging data", err)
	}

	// Create staging data which doesn't belong to any branch
	err := mgr.Set(ctx, graveler.StagingToken(branch+"_invalid"), graveler.Key("bad"), &graveler.Value{
		Identity: []byte("DEAD"),
		Data:     []byte("BEEF"),
	}, false)
	testutil.MustDo(t, "Create staging data", err)
}

func verifyCommitTagResults(t *testing.T, ctx context.Context, kvMgr, dbMgr graveler.RefManager, repository *graveler.RepositoryRecord) {
	commits, err := kvMgr.ListCommits(ctx, repository)
	require.NoError(t, err)
	defer commits.Close()
	commitNum := 0
	for commits.Next() {
		commit := commits.Value()
		kvc, err := kvMgr.GetCommit(ctx, repository, commit.CommitID)
		require.NoError(t, err)
		dbm, err := dbMgr.GetCommit(ctx, repository, commit.CommitID)
		require.NoError(t, err)
		dbm.CreationDate = dbm.CreationDate.UTC()
		require.Equal(t, kvc, dbm)
		commitNum += 1

		// verify tag
		tag, err := kvMgr.GetTag(ctx, repository, graveler.TagID("tag_"+commit.CommitID))
		require.NoError(t, err)
		require.Equal(t, *tag, commit.CommitID)
	}
	require.Equal(t, commitNum, numCommits+1)

	tags, err := kvMgr.ListTags(ctx, repository)
	require.NoError(t, err)
	defer tags.Close()
	numTags := 0
	for tags.Next() {
		numTags += 1
	}
	require.Equal(t, commitNum, numTags)
}

func verifyBranchResults(t *testing.T, ctx context.Context, kvMgr, dbMgr graveler.RefManager, stagingMgr graveler.StagingManager, repository *graveler.RepositoryRecord) {
	branches, err := kvMgr.ListBranches(ctx, repository)
	require.NoError(t, err)
	defer branches.Close()
	branchesNum := 0
	for branches.Next() {
		b := branches.Value()
		bkv, err := kvMgr.GetBranch(ctx, repository, b.BranchID)
		require.NoError(t, err)
		bdb, err := dbMgr.GetBranch(ctx, repository, b.BranchID)
		require.NoError(t, err)
		require.Equal(t, bkv, bdb)
		branchesNum += 1

		// Skip staging check for main
		if b.BranchID == "main" {
			continue
		}
		st, err := stagingMgr.List(ctx, b.StagingToken, 0)
		require.NoError(t, err)

		index := 0
		data := make([]*graveler.ValueRecord, numStaging)
		for st.Next() {
			data[index] = st.Value()
			index++
		}
		require.Equal(t, numStaging, index)
		require.NoError(t, st.Err())
		sort.Slice(data, func(i, j int) bool {
			ki, _ := strconv.Atoi(data[i].Key.String())
			kj, _ := strconv.Atoi(data[j].Key.String())
			return ki < kj
		})

		for i, v := range data {
			require.Equal(t, graveler.Key(strconv.Itoa(i)), v.Key)
			if i%2 == 0 {
				require.Equal(t, string(v.Value.Data), fmt.Sprintf("%s_%s", b.BranchID, b.StagingToken))
				require.Equal(t, string(v.Value.Identity), fmt.Sprintf("%s_%s", b.BranchID, b.StagingToken))
			} else { // verify tombstone
				require.Nil(t, v.Value)
			}
		}

		// Verify bad token doesn't exist
		st, err = stagingMgr.List(ctx, graveler.StagingToken(b.BranchID+"_invalid"), 0)
		require.NoError(t, err)
		require.False(t, st.Next())
		require.NoError(t, st.Err())
	}
	require.Equal(t, branchesNum, numBranches+1) // including main
}

func verifyMigrationResults(t *testing.T, ctx context.Context, kvMgr, dbMgr graveler.RefManager, stagingMgr graveler.StagingManager, settingsMgr settings.Manager) {
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
		data, err := settingsMgr.Get(ctx, kvr, branch.ProtectionSettingKey, &testSettings)
		if repoNum%2 == 0 {
			require.Equal(t, data.(*settings.ExampleSettings).ExampleStr, repo.RepositoryID.String())
			require.Equal(t, data.(*settings.ExampleSettings).ExampleInt, int32(repoNum))
		} else {
			require.ErrorIs(t, err, graveler.ErrNotFound)
		}

		// verify repository entities
		verifyCommitTagResults(t, ctx, kvMgr, dbMgr, kvr)
		verifyBranchResults(t, ctx, kvMgr, dbMgr, stagingMgr, kvr)
		repoNum += 1
	}
	require.Equal(t, repoNum, NumRepositories)
}
