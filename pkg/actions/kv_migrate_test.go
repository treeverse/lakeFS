package actions_test

import (
	"bytes"
	"context"
	"math/rand"
	"os"
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
	migrateBenchRepo  = "migrateBenchRepo"
	migrateTestRepo   = "migrateTestRepo"
	actionsSampleSize = 10
)

// TestMigrateTime - use this test to benchmark the migration time of actions. Default dataset size is small as to not
// block github pipeline. Modify as needed when running locally. Note that dbContainerTimeoutSeconds might need to be modified as well
func TestMigrateTime(t *testing.T) {
	ctx := context.Background()
	database, _ := testutil.GetDB(t, databaseURI)

	ctrl := gomock.NewController(t)
	testSource := mock.NewMockSource(ctrl)
	testWriter := mock.NewMockOutputWriter(ctrl)
	mockStatsCollector := NewActionStatsMockCollector()
	dbService := actions.NewDBService(ctx, database, testSource, testWriter, &mockStatsCollector, true)
	createMigrateTestData(t, ctx, dbService, migrateBenchRepo, 250)
	kvStore := kvtest.MakeStoreByName(postgres.DriverName, databaseURI)(t, ctx)
	buf, _ := os.CreateTemp("", "migrate")
	start := time.Now()
	defer os.Remove(buf.Name())
	defer buf.Close()
	err := actions.Migrate(ctx, database.Pool(), buf)
	require.NoError(t, err)
	t.Log("Actions Migrate Time:", time.Since(start))
	_, _ = buf.Seek(0, 0)
	testutil.MustDo(t, "Import file", kv.Import(ctx, buf, kvStore))

	t.Log("Total Migrate Time:", time.Since(start))
}

func TestMigrate(t *testing.T) {
	var testData = make([][]actions.RunManifest, actionsSampleSize)
	ctx := context.Background()
	database, _ := testutil.GetDB(t, databaseURI)

	ctrl := gomock.NewController(t)
	testSource := mock.NewMockSource(ctrl)
	testWriter := mock.NewMockOutputWriter(ctrl)
	mockStatsCollector := NewActionStatsMockCollector()
	dbService := actions.NewDBService(ctx, database, testSource, testWriter, &mockStatsCollector, true)
	for i := 0; i < actionsSampleSize; i++ {
		testData[i] = createMigrateTestData(t, ctx, dbService, migrateTestRepo+strconv.Itoa(i), actionsSampleSize)
	}

	buf := bytes.Buffer{}
	err := actions.Migrate(ctx, database.Pool(), &buf)
	require.NoError(t, err)

	kvStore := kvtest.MakeStoreByName(postgres.DriverName, databaseURI)(t, ctx)
	mStore := kv.StoreMessage{Store: kvStore}
	testutil.MustDo(t, "Import file", kv.Import(ctx, &buf, kvStore))
	kvService := actions.NewKVService(ctx, mStore, testSource, testWriter, &mockStatsCollector, true)
	for i := 0; i < actionsSampleSize; i++ {
		validateTestData(t, ctx, kvService, mStore, testData[i], migrateTestRepo+strconv.Itoa(i))
	}
}

func validateTestData(t *testing.T, ctx context.Context, service actions.Service, store kv.StoreMessage, testData []actions.RunManifest, repoID string) {
	runs, err := service.ListRunResults(ctx, repoID, "", "", "")
	require.NoError(t, err)
	defer runs.Close()

	runCount := 0
	for runs.Next() {
		runCount++
		run := runs.Value()
		runIdx, err := strconv.Atoi(run.SourceRef)

		// Check for secondary keys
		if run.BranchID != "" {
			secondary := kv.SecondaryIndex{}
			rk := actions.RunByBranchPath(repoID, run.BranchID, run.RunID)
			_, err = store.GetMsg(ctx, actions.PartitionKey, rk, &secondary)
			require.NoError(t, err)
			r := actions.RunResultData{}
			_, err = store.GetMsg(ctx, actions.PartitionKey, string(secondary.PrimaryKey), &r)
			require.NoError(t, err)
			require.Equal(t, run, actions.RunResultFromProto(&r))
		}
		if run.CommitID != "" {
			secondary := kv.SecondaryIndex{}
			rk := actions.RunByCommitPath(repoID, run.CommitID, run.RunID)
			_, err = store.GetMsg(ctx, actions.PartitionKey, rk, &secondary)
			require.NoError(t, err)
			r := actions.RunResultData{}
			_, err = store.GetMsg(ctx, actions.PartitionKey, string(secondary.PrimaryKey), &r)
			require.Equal(t, run, actions.RunResultFromProto(&r))
		}

		// Validate tasks
		tasks, err := service.ListRunTaskResults(ctx, repoID, run.RunID, "")
		require.NoError(t, err)
		taskCount := 0
		for i := 0; tasks.Next(); i++ {
			taskCount++
			task := tasks.Value()
			taskIdx, err := strconv.Atoi(task.HookID)
			require.NoError(t, err)
			task.RunID = testData[runIdx].HooksRun[i].RunID
			require.Equal(t, testData[runIdx].HooksRun[taskIdx], *task)
		}
		tasks.Close()
		require.Equal(t, len(testData[runIdx].HooksRun), taskCount)
		// Validate run data
		run.RunID = testData[runIdx].Run.RunID
		require.NoError(t, err)
		expRun := testData[runIdx]
		require.Equal(t, expRun.Run, *run)
	}
	require.Equal(t, len(testData), runCount)
}

func createMigrateTestData(t testing.TB, ctx context.Context, service *actions.DBService, repoID string, size int) []actions.RunManifest {
	t.Helper()
	rand.Seed(time.Now().UnixNano())
	runs := make([]actions.RunManifest, 0)
	for i := 0; i < size; i++ {
		iStr := strconv.Itoa(i)
		now := time.Now().UTC().Truncate(time.Second)
		runID := service.NewRunID()
		run := actions.RunManifest{
			Run: actions.RunResult{
				RunID:     runID,
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

		for j := 0; j < 10; j++ {
			jStr := strconv.Itoa(j)
			now = time.Now().UTC().Truncate(time.Second)
			h := actions.TaskResult{
				RunID:      run.Run.RunID,
				HookRunID:  actions.NewHookRunID(i, j),
				HookID:     jStr, // used to identify task when iterating over task results
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
