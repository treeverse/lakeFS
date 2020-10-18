package parade_test

import (
	"github.com/treeverse/lakefs/parade"
	"strconv"
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
	ownCalled    int32
	returnCalled int32
	Tasks        []ownTaskResult
}

func getKTasks(k int, action string) []parade.OwnedTaskData {
	tasks := make([]parade.OwnedTaskData, 0, k)
	for i := 0; i < k; i++ {
		task := parade.OwnedTaskData{
			ID:     parade.TaskID(strconv.Itoa(i)),
			Token:  parade.PerformanceToken{},
			Action: action,
			Body:   nil,
		}
		tasks = append(tasks, task)
	}
	return tasks
}

func (m *mockParade) OwnTasks(_ parade.ActorID, _ int, _ []string, _ *time.Duration) ([]parade.OwnedTaskData, error) {
	cur := atomic.AddInt32(&m.ownCalled, 1)
	if cur <= int32(len(m.Tasks)) {
		return getKTasks(m.Tasks[cur-1].tasks, m.Tasks[cur-1].action), nil
	}
	return []parade.OwnedTaskData{}, nil
}

func (m *mockParade) ReturnTask(_ parade.TaskID, _ parade.PerformanceToken, _ string, _ parade.TaskStatusCodeValue) error {
	atomic.AddInt32(&m.returnCalled, 1)
	return nil
}

type mockHandler struct {
	handleCalled int32
}

func (m *mockHandler) Handle(_ string, _ *string) parade.HandlerResult {
	atomic.AddInt32(&m.handleCalled, 1)
	return parade.HandlerResult{}
}

func (m *mockHandler) Actions() []string {
	return []string{"one", "two"}
}

func (m *mockHandler) Actor() parade.ActorID {
	return "mock"
}

func durationPointer(d time.Duration) *time.Duration {
	return &d
}

func TestActionManager(t *testing.T) {
	tests := []struct {
		name                    string
		Tasks                   []ownTaskResult
		sleepTime               time.Duration
		expectedReturnTaskCalls int32
		expectedHandleCalled    int32
		properties              *parade.ManagerProperties
	}{
		{
			name:                    "no tasks",
			sleepTime:               50 * time.Millisecond,
			expectedReturnTaskCalls: int32(0),
			expectedHandleCalled:    int32(0),
		},
		{
			name: "50 tasks in one call",
			Tasks: []ownTaskResult{
				{
					tasks:  50,
					action: "first action",
				},
			},
			sleepTime:               50 * time.Millisecond,
			expectedReturnTaskCalls: int32(50),
			expectedHandleCalled:    int32(50),
		},
		{
			name: "80 Tasks in two calls",
			Tasks: []ownTaskResult{
				{
					tasks:  50,
					action: "first action",
				},
				{
					tasks:  30,
					action: "second action",
				},
			},
			sleepTime:               50 * time.Millisecond,
			expectedReturnTaskCalls: int32(80),
			expectedHandleCalled:    int32(80),
		},
		{
			name: "exit before second call",
			Tasks: []ownTaskResult{
				{
					tasks:  50,
					action: "first action",
				},
				{
					tasks:  0,
					action: "force sleep",
				},
				{
					tasks:  0,
					action: "force sleep",
				},
				{
					tasks:  30,
					action: "second action",
				},
			},
			sleepTime:               50 * time.Millisecond,
			expectedReturnTaskCalls: int32(50),
			expectedHandleCalled:    int32(50),
			properties: &parade.ManagerProperties{
				WaitTime: durationPointer(time.Millisecond * 30),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mh := &mockHandler{}
			mp := &mockParade{
				Tasks: tt.Tasks,
			}
			a := parade.NewActionManager(mh, mp, tt.properties)
			time.Sleep(tt.sleepTime)
			a.Close()
			if mp.returnCalled != tt.expectedReturnTaskCalls {
				t.Errorf("expected ownedTasks to be called: %d times, called %d\n", tt.expectedReturnTaskCalls, mp.returnCalled)
			}
			if mh.handleCalled != tt.expectedHandleCalled {
				t.Errorf("expected ownedTasks to be called: %d times, called %d\n", tt.expectedHandleCalled, mh.handleCalled)
			}
		})
	}
}
