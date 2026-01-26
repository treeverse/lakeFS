package catalog

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/treeverse/lakefs/pkg/logging"
)

type taskState int

const (
	taskStatePending taskState = iota
	taskStateRunning
)

const (
	opCommit      = "commit"
	opMerge       = "merge"
	statusSuccess = "success"
	statusFailure = "failure"
	statusExpired = "expired"
)

type taskInfo struct {
	state       taskState
	operation   string
	submittedAt time.Time // when task was submitted
	startedAt   time.Time // zero if still pending
}

var (
	asyncOperationsPending = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lakefs_async_operations_pending",
			Help: "Number of async operations submitted but not yet started (may drift after restart)",
		},
		[]string{"operation"},
	)

	asyncOperationsRunning = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lakefs_async_operations_running",
			Help: "Number of async operations currently executing (may drift after restart)",
		},
		[]string{"operation"},
	)

	asyncOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lakefs_async_operations_total",
			Help: "Total number of completed async operations",
		},
		[]string{"operation", "status"},
	)

	asyncOperationPendingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lakefs_async_operation_pending_duration_seconds",
			Help:    "Time spent in pending state before starting or expiring",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		},
		[]string{"operation"},
	)

	asyncOperationRunningDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lakefs_async_operation_running_duration_seconds",
			Help:    "Time spent executing before completion or expiration",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		},
		[]string{"operation"},
	)
)

// AsyncOperationMetricsObserver implements TaskObserver to collect
// Prometheus metrics for async commit/merge operations.
//
// Thread-safety: All methods are safe for concurrent use.
//
// Limitations:
//   - Gauge metrics (pending/running) may drift after server restarts since
//     in-flight task state is not persisted. They will self-correct over time.
//   - If a task expires before OnTaskStarted is called, the pending gauge is
//     decremented (not running), based on tracked state.
type AsyncOperationMetricsObserver struct {
	mu    sync.Mutex
	tasks map[string]*taskInfo
}

func NewAsyncOperationMetricsObserver() *AsyncOperationMetricsObserver {
	return &AsyncOperationMetricsObserver{
		tasks: make(map[string]*taskInfo),
	}
}

var _ TaskObserver = (*AsyncOperationMetricsObserver)(nil)

func (o *AsyncOperationMetricsObserver) OnTaskSubmitted(taskID string) {
	operation := taskIDToOperation(taskID)
	if operation == "" {
		return
	}

	o.mu.Lock()
	o.tasks[taskID] = &taskInfo{
		state:       taskStatePending,
		operation:   operation,
		submittedAt: time.Now(),
	}
	o.mu.Unlock()

	asyncOperationsPending.WithLabelValues(operation).Inc()
}

func (o *AsyncOperationMetricsObserver) OnTaskStarted(taskID string) {
	operation := taskIDToOperation(taskID)
	if operation == "" {
		return
	}

	now := time.Now()

	o.mu.Lock()
	info, wasTracked := o.tasks[taskID]
	var pendingDuration float64
	if wasTracked {
		pendingDuration = now.Sub(info.submittedAt).Seconds()
		info.state = taskStateRunning
		info.startedAt = now
	} else {
		// Task wasn't tracked (submitted before observer was set up)
		o.tasks[taskID] = &taskInfo{
			state:     taskStateRunning,
			operation: operation,
			startedAt: now,
		}
	}
	o.mu.Unlock()

	if wasTracked {
		asyncOperationsPending.WithLabelValues(operation).Dec()
		asyncOperationPendingDuration.WithLabelValues(operation).Observe(pendingDuration)
	}
	asyncOperationsRunning.WithLabelValues(operation).Inc()
}

func (o *AsyncOperationMetricsObserver) OnTaskCompleted(taskID string, err error) {
	operation := taskIDToOperation(taskID)
	if operation == "" {
		return
	}

	now := time.Now()

	o.mu.Lock()
	info, wasTracked := o.tasks[taskID]
	var startedAt time.Time
	if wasTracked {
		startedAt = info.startedAt
	}
	delete(o.tasks, taskID)
	o.mu.Unlock()

	if wasTracked {
		asyncOperationsRunning.WithLabelValues(operation).Dec()
	}

	status := statusSuccess
	if err != nil {
		status = statusFailure
	}
	asyncOperationsTotal.WithLabelValues(operation, status).Inc()

	if wasTracked && !startedAt.IsZero() {
		runningDuration := now.Sub(startedAt).Seconds()
		asyncOperationRunningDuration.WithLabelValues(operation).Observe(runningDuration)
	}
}

func (o *AsyncOperationMetricsObserver) OnTaskExpired(taskID string) {
	operation := taskIDToOperation(taskID)
	if operation == "" {
		return
	}

	now := time.Now()

	o.mu.Lock()
	info, wasTracked := o.tasks[taskID]
	var wasPending bool
	var startedAt time.Time
	var submittedAt time.Time
	if wasTracked {
		wasPending = info.state == taskStatePending
		startedAt = info.startedAt
		submittedAt = info.submittedAt
	}
	delete(o.tasks, taskID)
	o.mu.Unlock()

	if wasTracked {
		if wasPending {
			asyncOperationsPending.WithLabelValues(operation).Dec()
			asyncOperationPendingDuration.WithLabelValues(operation).Observe(now.Sub(submittedAt).Seconds())
		} else {
			asyncOperationsRunning.WithLabelValues(operation).Dec()
		}
	}

	asyncOperationsTotal.WithLabelValues(operation, statusExpired).Inc()

	if wasTracked && !startedAt.IsZero() {
		runningDuration := now.Sub(startedAt).Seconds()
		asyncOperationRunningDuration.WithLabelValues(operation).Observe(runningDuration)
	}
}

func taskIDToOperation(taskID string) string {
	switch {
	case strings.HasPrefix(taskID, "CA"):
		return opCommit
	case strings.HasPrefix(taskID, "MA"):
		return opMerge
	default:
		logging.ContextUnavailable().
			WithField("task_id", taskID).
			Warn("async metrics: unrecognized operation")
		return ""
	}
}
