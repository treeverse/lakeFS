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
	iteratorTestRepoID = "itTestRepoID"
	testByBranch       = "testBranch"
	testByCommit       = "commit1"
)

var keyMap = make(map[int]string, 0)

func TestRunResultsIterator(t *testing.T) {
	ctx := context.Background()
	kvStore := kv.StoreMessage{Store: kvtest.GetStore(ctx, t)}
	createTestData(t, ctx, kvStore)

	tests := []struct {
		name     string
		branchID string
		commitID string
		after    string
		startIdx int
		count    int
	}{
		{
			name:     "basic",
			branchID: "",
			commitID: "",
			after:    "",
			startIdx: 199,
			count:    200,
		},
		{
			name:     "after key",
			branchID: "",
			commitID: "",
			after:    keyMap[40],
			startIdx: 40,
			count:    41,
		},
		{
			name:     "basic by branch",
			branchID: testByBranch,
			commitID: "",
			after:    "",
			startIdx: 149,
			count:    50,
		},
		{
			name:     "basic by commit",
			branchID: "",
			commitID: testByCommit,
			after:    "",
			startIdx: 199,
			count:    50,
		},
		{
			name:     "after by commit",
			branchID: "",
			commitID: testByCommit,
			after:    keyMap[190],
			startIdx: 190,
			count:    41,
		},
		{
			name:     "after out of range",
			branchID: "",
			commitID: "",
			after:    keyMap[201],
			startIdx: 0,
			count:    0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			itr, err := actions.NewKVRunResultIterator(ctx, kvStore, iteratorTestRepoID, tt.branchID, tt.commitID, tt.after)
			require.NoError(t, err)
			defer itr.Close()

			numRead := 0
			runID := tt.startIdx
			for itr.Next() {
				run := itr.Value()
				require.Equal(t, strconv.Itoa(runID), run.RunID)
				runID--
				numRead++
			}
			require.False(t, itr.Next())
			require.NoError(t, itr.Err())
			require.Equal(t, tt.count, numRead)
		})
	}
}

func createTestData(t *testing.T, ctx context.Context, kvStore kv.StoreMessage) {
	ctrl := gomock.NewController(t)
	writer := mock.NewMockOutputWriter(ctrl)
	mockStatsCollector := NewActionStatsMockCollector()
	testSource := mock.NewMockSource(ctrl)
	actionService := actions.NewKVService(ctx, kvStore, testSource, writer, &mockStatsCollector, false)
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
		keyMap[msgIdx] = runID
		key := actions.GetRunPath(iteratorTestRepoID, runID)
		run.RunId = strconv.Itoa(msgIdx)
		require.NoError(t, kvStore.SetMsg(ctx, key, &run))
		time.Sleep(2 * time.Millisecond)
		for j := 0; j < 100; j++ {
			HookRunId := actions.NewHookRunID(msgIdx, j)
			taskKey := kv.FormatPath(actions.GetTasksPath(iteratorTestRepoID, runID), HookRunId)
			task.HookRunId = HookRunId
			task.HookId = strconv.Itoa(j)
			task.RunId = runID
			require.NoError(t, kvStore.SetMsg(ctx, taskKey, &task))
		}
	}

	// By branch
	for ; msgIdx < 150; msgIdx++ {
		runID := actionService.NewRunID()
		keyMap[msgIdx] = runID
		key := actions.GetRunPath(iteratorTestRepoID, runID)
		run.RunId = strconv.Itoa(msgIdx)
		require.NoError(t, kvStore.SetMsg(ctx, key, &run))
		keyByCommit := actions.GetRunByBranchPath(iteratorTestRepoID, testByBranch, runID)
		require.NoError(t, kvStore.Set(ctx, []byte(keyByCommit), []byte(key)))
		time.Sleep(2 * time.Millisecond)
	}

	// By commit
	for ; msgIdx < 200; msgIdx++ {
		runID := actionService.NewRunID()
		keyMap[msgIdx] = runID
		key := actions.GetRunPath(iteratorTestRepoID, runID)
		run.RunId = strconv.Itoa(msgIdx)
		require.NoError(t, kvStore.SetMsg(ctx, key, &run))
		keyByCommit := actions.GetRunByCommitPath(iteratorTestRepoID, testByCommit, runID)
		require.NoError(t, kvStore.Set(ctx, []byte(keyByCommit), []byte(key)))
		time.Sleep(2 * time.Millisecond)
	}

	// Out of range
	badValue := "BadValue"
	key := "aaa"
	require.NoError(t, kvStore.Set(ctx, []byte(key), []byte(badValue)))
	msgIdx++
	keyMap[msgIdx] = key

	key = "zzz"
	require.NoError(t, kvStore.Set(ctx, []byte(key), []byte(badValue)))
	msgIdx++
	keyMap[msgIdx] = key
}
