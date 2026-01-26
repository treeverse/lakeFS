package catalog

import (
	"github.com/treeverse/lakefs/pkg/logging"
)

// TaskObserver receives notifications about task lifecycle events.
// Implementations can use these for metrics, logging, or audit trails.
// All callbacks are synchronous and should be fast.
type TaskObserver interface {
	// OnTaskSubmitted is called after a task is created and written to KV store,
	// before RunBackgroundTaskSteps returns.
	OnTaskSubmitted(taskID string)

	// OnTaskStarted is called when a worker picks up the task and begins execution.
	OnTaskStarted(taskID string)

	// OnTaskCompleted is called when task execution is done (success or error).
	OnTaskCompleted(taskID string, err error)

	// OnTaskExpired is called exactly once when the cleanup job deletes an expired task.
	OnTaskExpired(taskID string)
}

// NoOpTaskObserver is the default implementation that does nothing.
type NoOpTaskObserver struct{}

// Compile-time check that NoOpTaskObserver implements TaskObserver
var _ TaskObserver = (*NoOpTaskObserver)(nil)

func (n *NoOpTaskObserver) OnTaskSubmitted(taskID string) {}

func (n *NoOpTaskObserver) OnTaskStarted(taskID string) {}

func (n *NoOpTaskObserver) OnTaskCompleted(taskID string, err error) {}

func (n *NoOpTaskObserver) OnTaskExpired(taskID string) {}

// notifyObserver safely calls an observer callback, recovering from any panic.
// This protects task execution from buggy observer implementations.
func notifyObserver(taskID string, callback func()) {
	defer func() {
		if r := recover(); r != nil {
			logging.ContextUnavailable().
				WithField("task_id", taskID).
				WithField("panic", r).
				Error("Task observer panic recovered")
		}
	}()
	callback()
}
