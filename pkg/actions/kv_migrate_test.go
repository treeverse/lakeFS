package actions_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/actions/mock"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	migrateTestRepo   = "migrateTestRepo"
	actionsSampleSize = 100
)

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	database, _ := testutil.GetDB(t, databaseURI)

	ctrl := gomock.NewController(t)
	testSource := mock.NewMockSource(ctrl)
	testWriter := mock.NewMockOutputWriter(ctrl)
	mockStatsCollector := NewActionStatsMockCollector()
	dbService := actions.NewDBService(ctx, database, testSource, testWriter, &mockStatsCollector, true)
	testData := createMigrateTestData(t, ctx, dbService, actionsSampleSize)

	buf := bytes.Buffer{}
	err := actions.Migrate(ctx, database.Pool(), &buf)
	require.NoError(t, err)

	kvStore := kvtest.MakeStoreByName(postgres.DriverName, databaseURI)(t, ctx)
	mStore := kv.StoreMessage{Store: kvStore}
	testutil.MustDo(t, "Import file", kv.Import(ctx, &buf, kvStore))
	kvService := actions.NewKVService(ctx, mStore, testSource, testWriter, &mockStatsCollector, true)
	runs, err := kvService.ListRunResults(ctx, migrateTestRepo, "", "", "")
	defer runs.Close()
	require.NoError(t, err)
	for runs.Next() {
		run := runs.Value()
		runIdx, err := strconv.Atoi(run.SourceRef)

		// Check for secondary keys
		if run.BranchID != "" {
			secondary := kv.SecondaryIndex{}
			rk := actions.RunByBranchPath(migrateTestRepo, run.BranchID, run.RunID)
			_, err = mStore.GetMsg(ctx, actions.PartitionKey, rk, &secondary)
			require.NoError(t, err)
			r := actions.RunResultData{}
			_, err = mStore.GetMsg(ctx, actions.PartitionKey, string(secondary.PrimaryKey), &r)
			require.NoError(t, err)
			require.Equal(t, run, actions.RunResultFromProto(&r))
		}
		if run.CommitID != "" {
			secondary := kv.SecondaryIndex{}
			rk := actions.RunByCommitPath(migrateTestRepo, run.CommitID, run.RunID)
			_, err = mStore.GetMsg(ctx, actions.PartitionKey, rk, &secondary)
			require.NoError(t, err)
			r := actions.RunResultData{}
			_, err = mStore.GetMsg(ctx, actions.PartitionKey, string(secondary.PrimaryKey), &r)
			require.Equal(t, run, actions.RunResultFromProto(&r))
		}

		// Validate tasks
		tasks, err := kvService.ListRunTaskResults(ctx, migrateTestRepo, run.RunID, "")
		require.NoError(t, err)
		for i := 0; tasks.Next(); i++ {
			require.Equal(t, testData[runIdx].HooksRun[i], *tasks.Value())
		}
		tasks.Close()
		// Validate run data
		run.RunID = run.SourceRef
		require.NoError(t, err)
		expRun := testData[runIdx]
		require.Equal(t, expRun.Run, *run)
	}
}

func createMigrateTestData(t *testing.T, ctx context.Context, service *actions.DBService, size int) []actions.RunManifest {
	t.Helper()
	rand.Seed(time.Now().UnixNano())
	runs := make([]actions.RunManifest, 0)
	for i := 0; i < size; i++ {
		iStr := strconv.Itoa(i)
		repoID := migrateTestRepo
		now := time.Now().UTC().Truncate(time.Second)
		run := actions.RunManifest{
			Run: actions.RunResult{
				RunID:     iStr,
				BranchID:  "SomeBranch" + iStr,
				SourceRef: iStr, // use this to identify old runID in KV format
				EventType: "EventType" + iStr,
				CommitID:  "CommitID" + iStr,
				StartTime: now,
				EndTime:   now.Add(1 * time.Hour).UTC().Truncate(time.Second),
				Passed:    rand.Intn(2) == 1,
			},
			HooksRun: make([]actions.TaskResult, 0),
		}

		_, err := service.DB.Exec(ctx, `INSERT INTO actions_runs(repository_id, run_id, event_type, start_time, end_time, branch_id, source_ref, commit_id, passed)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			repoID, run.Run.RunID, run.Run.EventType, run.Run.StartTime, run.Run.EndTime, run.Run.BranchID, run.Run.SourceRef, run.Run.CommitID, run.Run.Passed)
		testutil.MustDo(t, "Create entry", err)

		for j := 0; j < size; j++ {
			jStr := strconv.Itoa(j)
			now = time.Now().UTC().Truncate(time.Second)
			h := actions.TaskResult{
				RunID:      run.Run.RunID,
				HookRunID:  fmt.Sprintf("%d_%d", i, j),
				HookID:     "hook_id_" + jStr,
				ActionName: "Some_Action_" + jStr,
				StartTime:  now,
				EndTime:    now.Add(5 * time.Minute).UTC().Truncate(time.Second),
				Passed:     rand.Intn(2) == 1,
			}
			run.HooksRun = append(run.HooksRun, h)
			_, err = service.DB.Exec(ctx, `INSERT INTO actions_run_hooks(repository_id, run_id, hook_run_id, action_name, hook_id, start_time, end_time, passed)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				repoID, h.RunID, h.HookRunID, h.ActionName, h.HookID, h.StartTime, h.EndTime, h.Passed)
			testutil.MustDo(t, "Create entry", err)
		}
		runs = append(runs, run)
	}
	return runs
}
