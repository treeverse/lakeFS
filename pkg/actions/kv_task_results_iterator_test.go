package actions_test

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestTaskResultsIterator(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	_, keyList := createTestData(t, ctx, kvStore)

	tests := []struct {
		name  string
		runID int
		after int
		count int
	}{
		{
			name:  "basic",
			runID: len(keyList) - actionsWithHooks/4,
			after: -1,
			count: 100,
		},
		{
			name:  "after key",
			runID: len(keyList) - actionsWithHooks/3,
			after: 19,
			count: 80,
		},
		{
			name:  "after out of range",
			runID: rand.Intn(100),
			after: 100,
			count: 0,
		},
		{
			name:  "run no tasks",
			runID: 100,
			after: 0,
			count: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runID := keyList[tt.runID]
			taskID := 0
			after := ""
			idx := len(keyList) - 1 - tt.runID
			if tt.after >= 0 {
				after = actions.NewHookRunID(idx, tt.after)
				taskID = tt.after + 1
			}
			itr, err := actions.NewKVTaskResultIterator(ctx, kvStore, iteratorTestRepoID, runID, after)
			require.NoError(t, err)
			defer itr.Close()

			numRead := 0
			for itr.Next() {
				task := itr.Value()
				require.Equal(t, runID, task.RunID)
				require.Equal(t, actions.NewHookRunID(idx, taskID), task.HookRunID)
				require.Equal(t, strconv.Itoa(taskID), task.HookID)
				taskID++
				numRead++
			}
			require.False(t, itr.Next())
			require.NoError(t, itr.Err())
			require.Equal(t, tt.count, numRead)
		})
	}
}
