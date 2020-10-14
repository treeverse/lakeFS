package action

import (
	"github.com/treeverse/lakefs/parade"
	"sync/atomic"
	"testing"
	"time"
)

type ownTaskResult struct {
	tasks  int
	action string
}

type mockParade struct {
	parade.Parade
	ownCalled    *int32
	returnCalled *int32
	tasks        []ownTaskResult
}

func newMockParade(tasks []ownTaskResult) mockParade {
	ownCalled := int32(0)
	returnCalled := int32(0)
	return mockParade{
		ownCalled:    &ownCalled,
		returnCalled: &returnCalled,
		tasks:        tasks,
	}
}

func getKTasks(k int, action string) []parade.OwnedTaskData {
	tasks := make([]parade.OwnedTaskData, 0, k)
	for i := 0; i < k; i++ {
		task := parade.OwnedTaskData{
			ID:     parade.TaskID(i),
			Token:  parade.PerformanceToken{},
			Action: action,
			Body:   nil,
		}
		tasks = append(tasks, task)
	}
	return tasks
}

func (m mockParade) OwnTasks(_ parade.ActorID, _ int, _ []string, _ *time.Duration) ([]parade.OwnedTaskData, error) {
	cur := atomic.AddInt32(m.ownCalled, 1)
	if cur <= int32(len(m.tasks)) {
		return getKTasks(m.tasks[cur-1].tasks, m.tasks[cur-1].action), nil
	}
	return []parade.OwnedTaskData{}, nil
}

func (m mockParade) ReturnTask(_ parade.TaskID, _ parade.PerformanceToken, _ string, _ parade.TaskStatusCodeValue) error {
	atomic.AddInt32(m.returnCalled, 1)
	return nil
}

type mockHandler struct {
	handleCalled *int32
}

func newMockHandler() mockHandler {
	handleCalled := int32(0)
	return mockHandler{
		handleCalled: &handleCalled,
	}
}

func (m mockHandler) Handle(_ string, _ *string) HandlerResult {
	atomic.AddInt32(m.handleCalled, 1)
	return HandlerResult{}
}

func (m mockHandler) Actions() []string {
	return []string{"one", "two"}
}

func (m mockHandler) Actor() parade.ActorID {
	return "mock"
}

func durationPointer(d time.Duration) *time.Duration {
	return &d
}

func TestManager(t *testing.T) {
	tests := []struct {
		name                    string
		mp                      mockParade
		mh                      mockHandler
		sleepTime               time.Duration
		expectedReturnTaskCalls int32
		expectedHandleCalled    int32
		properties              *Properties
	}{
		{
			name:                    "no tasks",
			mp:                      newMockParade([]ownTaskResult{}),
			mh:                      newMockHandler(),
			sleepTime:               100 * time.Millisecond,
			expectedReturnTaskCalls: int32(0),
			expectedHandleCalled:    int32(0),
			properties:              nil,
		},
		{
			name: "50 tasks in one call",
			mp: newMockParade([]ownTaskResult{
				{
					tasks:  50,
					action: "first action",
				},
			}),
			mh:                      newMockHandler(),
			sleepTime:               100 * time.Millisecond,
			expectedReturnTaskCalls: int32(50),
			expectedHandleCalled:    int32(50),
			properties:              nil,
		},
		{
			name: "80 tasks in two calls",
			mp: newMockParade([]ownTaskResult{
				{
					tasks:  50,
					action: "first action",
				},
				{
					tasks:  30,
					action: "second action",
				},
			}),
			mh:                      newMockHandler(),
			sleepTime:               100 * time.Millisecond,
			expectedReturnTaskCalls: int32(80),
			expectedHandleCalled:    int32(80),
			properties:              nil,
		},
		{
			name: "exit before second call",
			mp: newMockParade([]ownTaskResult{
				{
					tasks:  50,
					action: "first action",
				},
				{
					tasks:  0,
					action: "force sleep",
				},
				{
					tasks:  30,
					action: "second action",
				},
			}),
			mh:                      newMockHandler(),
			sleepTime:               50 * time.Millisecond,
			expectedReturnTaskCalls: int32(50),
			expectedHandleCalled:    int32(50),
			properties: &Properties{
				waitTime: durationPointer(time.Millisecond * 100),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAction(tt.mh, tt.mp, tt.properties)
			time.Sleep(tt.sleepTime)
			a.Close()
			returnCalled := atomic.LoadInt32(tt.mp.returnCalled)
			if returnCalled != tt.expectedReturnTaskCalls {
				t.Fatalf("expected ownedTasks to be called: %d times, called %d\n", tt.expectedReturnTaskCalls, returnCalled)
			}
			handleCalled := atomic.LoadInt32(tt.mh.handleCalled)
			if handleCalled != tt.expectedHandleCalled {
				t.Fatalf("expected ownedTasks to be called: %d times, called %d\n", tt.expectedHandleCalled, handleCalled)
			}
		})
	}
}
