package catalog

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestTaskIDToOperation_ValidPrefixes(t *testing.T) {
	tests := []struct {
		name     string
		taskID   string
		expected string
	}{
		{
			name:     "commit prefix",
			taskID:   "CA123456789",
			expected: "commit",
		},
		{
			name:     "merge prefix",
			taskID:   "MA987654321",
			expected: "merge",
		},
		{
			name:     "commit with minimum length",
			taskID:   "CA",
			expected: "commit",
		},
		{
			name:     "merge with minimum length",
			taskID:   "MA",
			expected: "merge",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := taskIDToOperation(tt.taskID)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestTaskIDToOperation_InvalidPrefixes(t *testing.T) {
	tests := []struct {
		name   string
		taskID string
	}{
		{
			name:   "empty string",
			taskID: "",
		},
		{
			name:   "single character",
			taskID: "C",
		},
		{
			name:   "unknown prefix",
			taskID: "XX12345",
		},
		{
			name:   "lowercase commit",
			taskID: "ca12345",
		},
		{
			name:   "lowercase merge",
			taskID: "ma12345",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := taskIDToOperation(tt.taskID)
			require.Empty(t, result, "invalid prefix should return empty operation string")
		})
	}
}

func TestAsyncOperationMetricsObserver_FullLifecycle_Success(t *testing.T) {
	resetMetrics(t)

	observer := NewAsyncOperationMetricsObserver()
	taskID := "CA12345"

	observer.OnTaskSubmitted(taskID)
	assertGaugeValue(t, asyncOperationsPending, "commit", 1)
	assertGaugeValue(t, asyncOperationsRunning, "commit", 0)

	observer.OnTaskStarted(taskID)
	assertGaugeValue(t, asyncOperationsPending, "commit", 0)
	assertGaugeValue(t, asyncOperationsRunning, "commit", 1)

	observer.OnTaskCompleted(taskID, nil)
	assertGaugeValue(t, asyncOperationsPending, "commit", 0)
	assertGaugeValue(t, asyncOperationsRunning, "commit", 0)
	assertCounterValue(t, asyncOperationsTotal, "commit", "success", 1)
	assertHistogramHasObservations(t, asyncOperationPendingDuration, "commit", 1)
	assertHistogramHasObservations(t, asyncOperationRunningDuration, "commit", 1)
}

func TestAsyncOperationMetricsObserver_FullLifecycle_Failure(t *testing.T) {
	resetMetrics(t)

	observer := NewAsyncOperationMetricsObserver()
	taskID := "MA12345"
	taskErr := errors.New("merge conflict")

	observer.OnTaskSubmitted(taskID)
	observer.OnTaskStarted(taskID)
	observer.OnTaskCompleted(taskID, taskErr)

	assertGaugeValue(t, asyncOperationsPending, "merge", 0)
	assertGaugeValue(t, asyncOperationsRunning, "merge", 0)
	assertCounterValue(t, asyncOperationsTotal, "merge", "failure", 1)
}

func TestAsyncOperationMetricsObserver_Expired_WhilePending(t *testing.T) {
	resetMetrics(t)

	observer := NewAsyncOperationMetricsObserver()
	taskID := "CA12345"

	observer.OnTaskSubmitted(taskID)
	assertGaugeValue(t, asyncOperationsPending, "commit", 1)

	observer.OnTaskExpired(taskID)
	assertGaugeValue(t, asyncOperationsPending, "commit", 0)
	assertGaugeValue(t, asyncOperationsRunning, "commit", 0)
	assertCounterValue(t, asyncOperationsTotal, "commit", "expired", 1)
	assertHistogramHasObservations(t, asyncOperationPendingDuration, "commit", 1)
}

func TestAsyncOperationMetricsObserver_Expired_WhileRunning(t *testing.T) {
	resetMetrics(t)

	observer := NewAsyncOperationMetricsObserver()
	taskID := "MA12345"

	observer.OnTaskSubmitted(taskID)
	observer.OnTaskStarted(taskID)
	assertGaugeValue(t, asyncOperationsRunning, "merge", 1)

	observer.OnTaskExpired(taskID)
	assertGaugeValue(t, asyncOperationsPending, "merge", 0)
	assertGaugeValue(t, asyncOperationsRunning, "merge", 0)
	assertCounterValue(t, asyncOperationsTotal, "merge", "expired", 1)
	assertHistogramHasObservations(t, asyncOperationPendingDuration, "merge", 1)
	assertHistogramHasObservations(t, asyncOperationRunningDuration, "merge", 1)
}

func TestAsyncOperationMetricsObserver_UnknownTaskPrefix_Ignored(t *testing.T) {
	resetMetrics(t)

	observer := NewAsyncOperationMetricsObserver()
	taskID := "XX12345"

	observer.OnTaskSubmitted(taskID)
	observer.OnTaskStarted(taskID)
	observer.OnTaskCompleted(taskID, nil)
	observer.OnTaskExpired(taskID)

	observer.mu.Lock()
	require.Empty(t, observer.tasks)
	observer.mu.Unlock()
}

func TestAsyncOperationMetricsObserver_MultipleTasks_Concurrent(t *testing.T) {
	resetMetrics(t)

	observer := NewAsyncOperationMetricsObserver()
	numTasks := 100

	var wg sync.WaitGroup
	wg.Add(numTasks)

	for i := range numTasks {
		go func(i int) {
			defer wg.Done()
			prefix := "CA"
			if i%2 == 1 {
				prefix = "MA"
			}
			observer.OnTaskSubmitted(fmt.Sprintf("%s%04d", prefix, i))
		}(i)
	}
	wg.Wait()

	observer.mu.Lock()
	require.Len(t, observer.tasks, numTasks)
	observer.mu.Unlock()
}

func TestAsyncOperationMetricsObserver_StartWithoutSubmit(t *testing.T) {
	// Task starts without prior submit (observer added after task was submitted)
	resetMetrics(t)

	observer := NewAsyncOperationMetricsObserver()
	taskID := "CA12345"

	observer.OnTaskStarted(taskID)

	observer.mu.Lock()
	require.Contains(t, observer.tasks, taskID)
	require.Equal(t, taskStateRunning, observer.tasks[taskID].state)
	observer.mu.Unlock()

	assertGaugeValue(t, asyncOperationsRunning, "commit", 1)
	assertGaugeValue(t, asyncOperationsPending, "commit", 0)
}

func TestAsyncOperationMetricsObserver_ExpireWithoutTrack(t *testing.T) {
	// Untracked task expires - counter increments, gauges stay at zero
	resetMetrics(t)

	observer := NewAsyncOperationMetricsObserver()

	observer.OnTaskExpired("MA12345")

	assertCounterValue(t, asyncOperationsTotal, "merge", "expired", 1)
	assertGaugeValue(t, asyncOperationsPending, "merge", 0)
	assertGaugeValue(t, asyncOperationsRunning, "merge", 0)
}

func TestAsyncOperationMetricsObserver_CompleteWithoutTrack(t *testing.T) {
	// Untracked task completes - counter increments, gauges stay at zero
	resetMetrics(t)

	observer := NewAsyncOperationMetricsObserver()

	observer.OnTaskCompleted("CA12345", nil)

	assertCounterValue(t, asyncOperationsTotal, "commit", "success", 1)
	assertGaugeValue(t, asyncOperationsPending, "commit", 0)
	assertGaugeValue(t, asyncOperationsRunning, "commit", 0)
}

func TestAsyncOperationMetricsObserver_ThreadSafety(t *testing.T) {
	resetMetrics(t)

	observer := NewAsyncOperationMetricsObserver()
	numGoroutines := 50
	operationsPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := range numGoroutines {
		go func(g int) {
			defer wg.Done()
			for i := range operationsPerGoroutine {
				taskID := fmt.Sprintf("CA%03d%03d", g, i)
				observer.OnTaskSubmitted(taskID)
				observer.OnTaskStarted(taskID)
				observer.OnTaskCompleted(taskID, nil)
			}
		}(g)
	}
	wg.Wait()

	observer.mu.Lock()
	require.Empty(t, observer.tasks)
	observer.mu.Unlock()

	assertGaugeValue(t, asyncOperationsPending, "commit", 0)
	assertGaugeValue(t, asyncOperationsRunning, "commit", 0)
}

func resetMetrics(t *testing.T) {
	t.Helper()
	asyncOperationsPending.Reset()
	asyncOperationsRunning.Reset()
}

func assertGaugeValue(t *testing.T, gauge *prometheus.GaugeVec, operation string, expected float64) {
	t.Helper()
	metric, err := gauge.GetMetricWithLabelValues(operation)
	require.NoError(t, err)
	actual := testutil.ToFloat64(metric)
	require.Equal(t, expected, actual, "gauge{operation=%q} expected %v, got %v",
		operation, expected, actual)
}

func assertCounterValue(t *testing.T, counter *prometheus.CounterVec, operation, status string, expected float64) {
	t.Helper()
	metric, err := counter.GetMetricWithLabelValues(operation, status)
	require.NoError(t, err)
	actual := testutil.ToFloat64(metric)
	// Use GreaterOrEqual because counters accumulate across tests and cannot be reset
	require.GreaterOrEqual(t, actual, expected, "counter{operation=%q,status=%q} expected at least %v, got %v",
		operation, status, expected, actual)
}

func assertHistogramHasObservations(t *testing.T, histogram *prometheus.HistogramVec, operation string, minCount uint64) {
	t.Helper()
	observer, err := histogram.GetMetricWithLabelValues(operation)
	require.NoError(t, err)

	var m dto.Metric
	err = observer.(prometheus.Metric).Write(&m)
	require.NoError(t, err)

	h := m.GetHistogram()
	require.NotNil(t, h)
	require.GreaterOrEqual(t, h.GetSampleCount(), minCount)
}
