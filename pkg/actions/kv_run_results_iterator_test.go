package actions_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/actions/mock"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

const (
	iteratorTestRepoID = "iterTestRepoID"
	testByBranch       = "testBranch"
	testByCommit       = "testCommit1"
	testMissingPrimary = "branchPartialPrimary"
	IndexOutOfRange    = "zzz"
)

var keyMap = make(map[string]int, 0)
var keyList = make([]string, 300)

func TestRunResultsIterator(t *testing.T) {
	t.Skip("flaky")
	ctx := context.Background()
	kvStore := kv.StoreMessage{Store: kvtest.GetStore(ctx, t)}
	createTestData(t, ctx, kvStore)

	tests := []struct {
		name     string
		branchID string
		commitID string
		after    string
		startIdx int
		endIdx   int
	}{
		{
			name:     "basic",
			branchID: "",
			commitID: "",
			after:    "",
			startIdx: 0,
			endIdx:   200,
		},
		{
			name:     "after key",
			branchID: "",
			commitID: "",
			after:    keyList[0],
			startIdx: 0,
			endIdx:   100,
		},
		{
			name:     "basic by branch",
			branchID: testByBranch,
			commitID: "",
			after:    "",
			startIdx: 100,
			endIdx:   150,
		},
		{
			name:     "basic by commit",
			branchID: "",
			commitID: testByCommit,
			after:    "",
			startIdx: 150,
			endIdx:   200,
		},
		{
			name:     "after by commit",
			branchID: "",
			commitID: testByCommit,
			after:    keyList[190],
			startIdx: 190,
			endIdx:   200,
		},
		{
			name:     "missing primary keys",
			branchID: testMissingPrimary,
			commitID: "",
			after:    "",
			startIdx: 0,
			endIdx:   50,
		},
		{
			name:     "after out of range",
			branchID: "",
			commitID: "",
			after:    IndexOutOfRange,
			startIdx: 0,
			endIdx:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			itr, err := actions.NewKVRunResultIterator(ctx, kvStore, iteratorTestRepoID, tt.branchID, tt.commitID, tt.after)
			require.NoError(t, err)
			defer itr.Close()

			numRead := 0
			for itr.Next() {
				run := itr.Value()
				require.NotNil(t, run)
				idx, ok := keyMap[run.RunID]
				require.True(t, ok)
				require.GreaterOrEqual(t, idx, tt.startIdx)
				require.Less(t, idx, tt.endIdx)
				numRead++
			}
			require.False(t, itr.Next())
			require.NoError(t, itr.Err())
			require.LessOrEqual(t, numRead, tt.endIdx-tt.startIdx)
		})
	}
}

func createTestData(t *testing.T, ctx context.Context, kvStore kv.StoreMessage) {
	ctrl := gomock.NewController(t)
	writer := mock.NewMockOutputWriter(ctrl)
	mockStatsCollector := NewActionStatsMockCollector()
	testSource := mock.NewMockSource(ctrl)
	actionService := actions.NewService(ctx, actions.NewActionsKVStore(kvStore), testSource, writer, &actions.DecreasingIDGenerator{}, &mockStatsCollector, false)
	msgIdx := 0
	run := actions.RunResultData{
		RunId:     "",
		BranchId:  "",
		CommitId:  "",
		SourceRef: "",
		EventType: "",
		StartTime: nil,
		EndTime:   nil,
		Passed:    false,
	}
	task := actions.TaskResultData{
		RunId:      "",
		HookRunId:  "",
		HookId:     "",
		ActionName: "",
		StartTime:  nil,
		EndTime:    nil,
		Passed:     false,
	}

	// Basic runs
	for ; msgIdx < 100; msgIdx++ {
		runID := actionService.NewRunID()
		keyMap[runID] = msgIdx
		keyList[msgIdx] = runID
		key := actions.RunPath(iteratorTestRepoID, runID)
		run.RunId = runID
		require.NoError(t, kvStore.SetMsg(ctx, actions.PartitionKey, key, &run))
		for j := 0; j < 100; j++ {
			HookRunId := actions.NewHookRunID(msgIdx, j)
			taskKey := kv.FormatPath(actions.TasksPath(iteratorTestRepoID, runID), HookRunId)
			task.HookRunId = HookRunId
			task.HookId = strconv.Itoa(j)
			task.RunId = runID
			require.NoError(t, kvStore.SetMsg(ctx, actions.PartitionKey, taskKey, &task))
		}
	}

	time.Sleep(2 * time.Second) // For 'after' test
	s := kv.SecondaryIndex{}
	// By branch
	for ; msgIdx < 150; msgIdx++ {
		runID := actionService.NewRunID()
		keyMap[runID] = msgIdx
		keyList[msgIdx] = runID
		key := actions.RunPath(iteratorTestRepoID, runID)
		run.RunId = runID
		require.NoError(t, kvStore.SetMsg(ctx, actions.PartitionKey, key, &run))
		keyByBranch := actions.RunByBranchPath(iteratorTestRepoID, testByBranch, runID)
		s.PrimaryKey = []byte(key)
		require.NoError(t, kvStore.SetMsg(ctx, actions.PartitionKey, keyByBranch, &s))
	}

	// By commit
	for ; msgIdx < 200; msgIdx++ {
		runID := actionService.NewRunID()
		keyMap[runID] = msgIdx
		keyList[msgIdx] = runID
		key := actions.RunPath(iteratorTestRepoID, runID)
		run.RunId = runID
		require.NoError(t, kvStore.SetMsg(ctx, actions.PartitionKey, key, &run))
		keyByCommit := actions.RunByCommitPath(iteratorTestRepoID, testByCommit, runID)
		s.PrimaryKey = []byte(key)
		require.NoError(t, kvStore.SetMsg(ctx, actions.PartitionKey, keyByCommit, &s))
	}

	// Missing Primary
	for ; msgIdx < 250; msgIdx++ {
		runID := actionService.NewRunID()

		// Add key with bad primary
		primaryKey := actions.RunPath(iteratorTestRepoID, runID)
		keyNoPrimary := actions.RunByBranchPath(iteratorTestRepoID, testMissingPrimary, runID)
		s.PrimaryKey = []byte(primaryKey)
		require.NoError(t, kvStore.SetMsg(ctx, actions.PartitionKey, keyNoPrimary, &s))

		// Key with primary
		primaryKey = actions.RunPath(iteratorTestRepoID, keyList[msgIdx%100])
		keyWithPrimary := actions.RunByBranchPath(iteratorTestRepoID, testMissingPrimary, keyList[msgIdx%100])
		s.PrimaryKey = []byte(primaryKey)
		require.NoError(t, kvStore.SetMsg(ctx, actions.PartitionKey, keyWithPrimary, &s))
	}

	// Out of range
	badValue := "BadValue"
	key := "aaa"
	require.NoError(t, kvStore.Set(ctx, []byte(actions.PartitionKey), []byte(key), []byte(badValue)))
	msgIdx++
	keyMap[key] = msgIdx
	keyList[msgIdx] = key

	require.NoError(t, kvStore.Set(ctx, []byte(actions.PartitionKey), []byte(IndexOutOfRange), []byte(badValue)))
	msgIdx++
	keyMap[key] = msgIdx
	keyList[msgIdx] = key
}
