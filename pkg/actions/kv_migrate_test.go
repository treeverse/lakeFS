package actions_test

import (
	"bytes"
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/actions/mock"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	migrateBenchRepo  = "migrateBenchRepo"
	migrateTestRepo   = "migrateTestRepo"
	actionsSampleSize = 10
)

func BenchmarkMigrate(b *testing.B) {
	b.Run("Benchmark-250", func(b *testing.B) {
		benchmarkMigrate(250, b)
	})
	b.Run("Benchmark-2500", func(b *testing.B) {
		benchmarkMigrate(2500, b)
	})
	b.Run("Benchmark-25000", func(b *testing.B) {
		benchmarkMigrate(25000, b)
	})
}

// benchmarkMigrate - use this test to benchmark the migration time of actions. Default dataset size is small as to not
func benchmarkMigrate(runCount int, b *testing.B) {
	ctx := context.Background()
	database, _ := testutil.GetDB(b, databaseURI)

	createMigrateTestData(b, ctx, database, migrateBenchRepo, runCount)
	kvStore := kvtest.MakeStoreByName(postgres.DriverName, kvparams.KV{Postgres: &kvparams.Postgres{ConnectionString: databaseURI}})(b, ctx)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var buffer bytes.Buffer
		err := actions.Migrate(ctx, database.Pool(), nil, &buffer)
		require.NoError(b, err)
		testutil.MustDo(b, "Import file", kv.Import(ctx, &buffer, kvStore))
	}
}

func TestMigrate(t *testing.T) {
	testData := make([][]actions.RunManifest, actionsSampleSize)
	ctx := context.Background()
	database, _ := testutil.GetDB(t, databaseURI)

	ctrl := gomock.NewController(t)
	testSource := mock.NewMockSource(ctrl)
	testWriter := mock.NewMockOutputWriter(ctrl)
	mockStatsCollector := NewActionStatsMockCollector()

	for i := 0; i < actionsSampleSize; i++ {
		testData[i] = createMigrateTestData(t, ctx, database, migrateTestRepo+strconv.Itoa(i), actionsSampleSize)
	}

	buf := bytes.Buffer{}
	err := actions.Migrate(ctx, database.Pool(), nil, &buf)
	require.NoError(t, err)

	kvStore := kvtest.MakeStoreByName(postgres.DriverName, kvparams.KV{Postgres: &kvparams.Postgres{ConnectionString: databaseURI}})(t, ctx)
	mStore := kv.StoreMessage{Store: kvStore}
	testutil.MustDo(t, "Import file", kv.Import(ctx, &buf, kvStore))
	kvService := actions.NewService(ctx, actions.NewActionsKVStore(mStore), testSource, testWriter, &actions.DecreasingIDGenerator{}, &mockStatsCollector, true)
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
		require.NoError(t, err)

		// Check for secondary keys
		if run.BranchID != "" {
			secondary := kv.SecondaryIndex{}
			rk := actions.RunByBranchPath(repoID, run.BranchID, run.RunID)
			_, err = store.GetMsg(ctx, actions.PartitionKey, rk, &secondary)
			require.NoError(t, err)
			r := actions.RunResultData{}
			_, err = store.GetMsg(ctx, actions.PartitionKey, secondary.PrimaryKey, &r)
			require.NoError(t, err)
			require.Equal(t, run, actions.RunResultFromProto(&r))
		}
		if run.CommitID != "" {
			secondary := kv.SecondaryIndex{}
			rk := actions.RunByCommitPath(repoID, run.CommitID, run.RunID)
			_, err = store.GetMsg(ctx, actions.PartitionKey, rk, &secondary)
			require.NoError(t, err)
			r := actions.RunResultData{}
			_, err = store.GetMsg(ctx, actions.PartitionKey, secondary.PrimaryKey, &r)
			require.NoError(t, err)
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

func createMigrateTestData(t testing.TB, ctx context.Context, database db.Database, repoID string, size int) []actions.RunManifest {
	t.Helper()
	rand.Seed(time.Now().UnixNano())
	runs := make([]actions.RunManifest, 0)
	runChan := make(chan *actions.RunManifest, 100)
	var g multierror.Group
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			return writeToDB(ctx, runChan, repoID, database)
		})
	}

	for i := 0; i < size; i++ {
		iStr := strconv.Itoa(i)
		now := time.Now().UTC().Truncate(time.Second)
		runID := (&actions.IncreasingIDGenerator{}).NewRunID()
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
		}
		runs = append(runs, run)
		runChan <- &run
	}
	close(runChan)
	testutil.MustDo(t, "Create entries", g.Wait().ErrorOrNil())
	return runs
}

func writeToDB(ctx context.Context, jobChan <-chan *actions.RunManifest, repoID string, db db.Database) error {
	for run := range jobChan {
		_, err := db.Exec(ctx, `INSERT INTO actions_runs(repository_id, run_id, event_type, start_time, end_time, branch_id, source_ref, commit_id, passed)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			repoID, run.Run.RunID, run.Run.EventType, run.Run.StartTime, run.Run.EndTime, run.Run.BranchID, run.Run.SourceRef, run.Run.CommitID, run.Run.Passed)
		if err != nil {
			return err
		}
		for _, h := range run.HooksRun {
			_, err = db.Exec(ctx, `INSERT INTO actions_run_hooks(repository_id, run_id, hook_run_id, action_name, hook_id, start_time, end_time, passed)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				repoID, h.RunID, h.HookRunID, h.ActionName, h.HookID, h.StartTime, h.EndTime, h.Passed)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
