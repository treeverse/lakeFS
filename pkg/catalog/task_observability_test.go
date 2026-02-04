package catalog

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
)

func getHistogramCount(t *testing.T, observer prometheus.Observer) float64 {
	t.Helper()

	// An Observer (returned by WithLabelValues) can be type-asserted
	// to a prometheus.Metric to access the Write method.
	metric, ok := observer.(prometheus.Metric)
	if !ok {
		t.Fatalf("observer does not implement prometheus.Metric")
	}

	dtoMetric := &dto.Metric{}
	err := metric.Write(dtoMetric)
	require.NoError(t, err)

	return float64(dtoMetric.GetHistogram().GetSampleCount())
}

func TestStartTaskSpanAndEnd(t *testing.T) {
	log := logging.Dummy()

	t.Run("successful completion increments and decrements gauge, records success", func(t *testing.T) {
		task := &Task{Id: "test-123", Operation: OpDumpRefs}

		runningGauge := asyncOperationsRunning.WithLabelValues(OpDumpRefs)
		successHist := asyncOperationDuration.WithLabelValues(OpDumpRefs, statusSuccess)

		runningBefore := testutil.ToFloat64(runningGauge)
		totalBefore := getHistogramCount(t, successHist)

		span := StartTaskSpan(log, task)
		require.Equal(t, runningBefore+1, testutil.ToFloat64(runningGauge), "gauge should increment")

		task.Done = true
		span.End()

		require.Equal(t, runningBefore, testutil.ToFloat64(runningGauge), "gauge should decrement")
		require.Equal(t, totalBefore+1, getHistogramCount(t, successHist), "histogram count should increment")
	})

	t.Run("failed completion records failure status", func(t *testing.T) {
		task := &Task{Id: "test-456", Operation: OpRestoreRefs}

		runningGauge := asyncOperationsRunning.WithLabelValues(OpRestoreRefs)
		failureHist := asyncOperationDuration.WithLabelValues(OpRestoreRefs, statusFailure)

		runningBefore := testutil.ToFloat64(runningGauge)
		totalBefore := getHistogramCount(t, failureHist)

		span := StartTaskSpan(log, task)
		require.Equal(t, runningBefore+1, testutil.ToFloat64(runningGauge))

		task.Done = true
		task.ErrorMsg = "something went wrong"
		span.End()

		require.Equal(t, runningBefore, testutil.ToFloat64(runningGauge))
		require.Equal(t, totalBefore+1, getHistogramCount(t, failureHist), "failure counter should increment")
	})

	t.Run("task without operation field uses unknown", func(t *testing.T) {
		task := &Task{Id: "legacy-task-123"}

		runningGauge := asyncOperationsRunning.WithLabelValues(opUnknown)
		successHist := asyncOperationDuration.WithLabelValues(opUnknown, statusSuccess)

		runningBefore := testutil.ToFloat64(runningGauge)
		totalBefore := getHistogramCount(t, successHist)

		span := StartTaskSpan(log, task)
		require.Equal(t, runningBefore+1, testutil.ToFloat64(runningGauge), "gauge should increment with unknown operation")

		task.Done = true
		span.End()

		require.Equal(t, runningBefore, testutil.ToFloat64(runningGauge))
		require.Equal(t, totalBefore+1, getHistogramCount(t, successHist), "success counter should increment with unknown operation")
	})

	t.Run("multiple End calls only affect metrics once", func(t *testing.T) {
		task := &Task{Id: "test-double-end", Operation: OpRestoreRefs}

		runningGauge := asyncOperationsRunning.WithLabelValues(OpRestoreRefs)
		successHist := asyncOperationDuration.WithLabelValues(OpRestoreRefs, statusSuccess)

		runningBefore := testutil.ToFloat64(runningGauge)
		totalBefore := getHistogramCount(t, successHist)

		span := StartTaskSpan(log, task)
		require.Equal(t, runningBefore+1, testutil.ToFloat64(runningGauge))

		task.Done = true
		span.End()
		span.End() // second call should be no-op
		span.End() // third call should be no-op

		require.Equal(t, runningBefore, testutil.ToFloat64(runningGauge), "gauge should only decrement once")
		require.Equal(t, totalBefore+1, getHistogramCount(t, successHist), "counter should only increment once")
	})
}

func TestRecordExpiredTask(t *testing.T) {
	log := logging.Dummy()

	t.Run("records expired status for tasks with operation field", func(t *testing.T) {
		drBefore := testutil.ToFloat64(asyncOperationsExpired.WithLabelValues(OpDumpRefs))
		rrBefore := testutil.ToFloat64(asyncOperationsExpired.WithLabelValues(OpRestoreRefs))

		RecordExpiredTask(log, &Task{Id: "expired-1", Operation: OpDumpRefs})
		RecordExpiredTask(log, &Task{Id: "expired-2", Operation: OpRestoreRefs})

		require.Equal(t, drBefore+1, testutil.ToFloat64(asyncOperationsExpired.WithLabelValues(OpDumpRefs)))
		require.Equal(t, rrBefore+1, testutil.ToFloat64(asyncOperationsExpired.WithLabelValues(OpRestoreRefs)))
	})

	t.Run("records expired status with unknown for legacy tasks", func(t *testing.T) {
		unknownBefore := testutil.ToFloat64(asyncOperationsExpired.WithLabelValues(opUnknown))

		RecordExpiredTask(log, &Task{Id: "legacy-expired-no-operation"})

		require.Equal(t, unknownBefore+1, testutil.ToFloat64(asyncOperationsExpired.WithLabelValues(opUnknown)))
	})
}
