package actions_test

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestTaskResultsIterator(t *testing.T) {
	ctx := context.Background()
	kvStore := kv.StoreMessage{Store: kvtest.GetStore(ctx, t)}
	createTestData(t, ctx, kvStore)

	tests := []struct {
		name  string
		runID int
		after int
		count int
	}{
		{
			name:  "basic",
			runID: rand.Intn(100),
			after: -1,
			count: 100,
		},
		{
			name:  "after key",
			runID: rand.Intn(100),
			after: 20,
			count: 80,
		},
		{
			name:  "after out of range",
			runID: rand.Intn(100),
			after: 100,
			count: 0,
		},
		{
			name:  "Run no tasks",
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
			if tt.after >= 0 {
				after = actions.NewHookRunID(tt.runID, tt.after)
				taskID = tt.after
			}
			itr, err := actions.NewKVTaskResultIterator(ctx, kvStore, iteratorTestRepoID, runID, after)
			require.NoError(t, err)
			defer itr.Close()

			numRead := 0
			for itr.Next() {
				task := itr.Value()
				require.Equal(t, runID, task.RunID)
				require.Equal(t, actions.NewHookRunID(tt.runID, taskID), task.HookRunID)
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
