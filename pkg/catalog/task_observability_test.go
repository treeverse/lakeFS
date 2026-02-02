package catalog

import (
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestStartTaskSpanAndEnd(t *testing.T) {
	log := logging.ContextUnavailable()

	t.Run("successful completion increments and decrements gauge, records success", func(t *testing.T) {
		task := &Task{Id: "test-123", Operation: OpCommit}

		runningBefore := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpCommit))
		totalBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpCommit, statusSuccess))

		span := StartTaskSpan(log, task)
		require.Equal(t, runningBefore+1, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpCommit)),
			"gauge should increment on StartTaskSpan")

		task.Done = true
		span.End()

		require.Equal(t, runningBefore, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpCommit)),
			"gauge should decrement on End")
		require.Equal(t, totalBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpCommit, statusSuccess)),
			"success counter should increment")
	})

	t.Run("failed completion records failure status", func(t *testing.T) {
		task := &Task{Id: "test-456", Operation: OpMerge}

		runningBefore := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpMerge))
		totalBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpMerge, statusFailure))

		span := StartTaskSpan(log, task)
		require.Equal(t, runningBefore+1, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpMerge)))

		task.Done = true
		task.ErrorMsg = "something went wrong"
		span.End()

		require.Equal(t, runningBefore, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpMerge)))
		require.Equal(t, totalBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpMerge, statusFailure)),
			"failure counter should increment")
	})

	t.Run("expired completion records expired status", func(t *testing.T) {
		task := &Task{Id: "test-789", Operation: OpDumpRefs}

		runningBefore := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpDumpRefs))
		totalBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpDumpRefs, statusExpired))

		span := StartTaskSpan(log, task)
		require.Equal(t, runningBefore+1, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpDumpRefs)))

		task.Done = true
		task.StatusCode = http.StatusRequestTimeout
		span.End()

		require.Equal(t, runningBefore, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpDumpRefs)))
		require.Equal(t, totalBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpDumpRefs, statusExpired)),
			"expired counter should increment")
	})

	t.Run("task without operation field uses unknown", func(t *testing.T) {
		task := &Task{Id: "legacy-task-123"}

		runningBefore := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opUnknown))
		totalBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opUnknown, statusSuccess))

		span := StartTaskSpan(log, task)
		require.Equal(t, runningBefore+1, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opUnknown)),
			"gauge should increment with unknown operation")

		task.Done = true
		span.End()

		require.Equal(t, runningBefore, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opUnknown)))
		require.Equal(t, totalBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opUnknown, statusSuccess)),
			"success counter should increment with unknown operation")
	})

	t.Run("nil task does not affect metrics", func(t *testing.T) {
		commitRunning := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpCommit))
		unknownRunning := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opUnknown))

		span := StartTaskSpan(log, nil)
		span.End()

		require.Equal(t, commitRunning, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpCommit)))
		require.Equal(t, unknownRunning, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(opUnknown)))
	})

	t.Run("multiple End calls only affect metrics once", func(t *testing.T) {
		task := &Task{Id: "test-double-end", Operation: OpRestoreRefs}

		runningBefore := testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpRestoreRefs))
		totalBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpRestoreRefs, statusSuccess))

		span := StartTaskSpan(log, task)
		require.Equal(t, runningBefore+1, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpRestoreRefs)))

		task.Done = true
		span.End()
		span.End() // second call should be no-op
		span.End() // third call should be no-op

		require.Equal(t, runningBefore, testutil.ToFloat64(asyncOperationsRunning.WithLabelValues(OpRestoreRefs)),
			"gauge should only decrement once")
		require.Equal(t, totalBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpRestoreRefs, statusSuccess)),
			"counter should only increment once")
	})
}

func TestRecordOrphanedTask(t *testing.T) {
	log := logging.ContextUnavailable()

	t.Run("records orphaned status for tasks with operation field", func(t *testing.T) {
		commitBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpCommit, statusOrphaned))
		mergeBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpMerge, statusOrphaned))

		RecordOrphanedTask(log, &Task{Id: "orphaned-1", Operation: OpCommit})
		RecordOrphanedTask(log, &Task{Id: "orphaned-2", Operation: OpMerge})

		require.Equal(t, commitBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpCommit, statusOrphaned)))
		require.Equal(t, mergeBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(OpMerge, statusOrphaned)))
	})

	t.Run("records orphaned status with unknown for legacy tasks", func(t *testing.T) {
		unknownBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opUnknown, statusOrphaned))

		RecordOrphanedTask(log, &Task{Id: "legacy-orphaned"})

		require.Equal(t, unknownBefore+1, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opUnknown, statusOrphaned)))
	})

	t.Run("nil task does not affect metrics", func(t *testing.T) {
		unknownBefore := testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opUnknown, statusOrphaned))

		RecordOrphanedTask(log, nil)

		require.Equal(t, unknownBefore, testutil.ToFloat64(asyncOperationsTotal.WithLabelValues(opUnknown, statusOrphaned)))
	})
}
