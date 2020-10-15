package parade

import (
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/logging"
	"sync"
	"time"
)

const (
	defaultWorkers     = 5
	defaultChannelSize = 1000
	defaultMaxTasks    = 500
	defaultWaitTime    = time.Millisecond * 300
	defaultErrWaitTime = time.Millisecond * 300
	defaultMaxDuration = time.Minute * 30 // Todo(guys): change this
)

// ManagerProperties defines the configuration properties of an ActionManager
type ManagerProperties struct {
	Workers     int            // number of goroutines handling tasks
	ChannelSize int            // size of the channel containing tasks for workers
	MaxTasks    int            // max tasks requested in every ownTasks request
	WaitTime    *time.Duration // time to wait if OwnTasks returned no tasks.
	ErrWaitTime *time.Duration // time to wait if OwnTasks returned err.
	MaxDuration *time.Duration // maxDuration passed to parade.OwnTasks
}

// A ActionManager manages the process of requesting and returning tasks for a specific TaskHandler
// The manager requests tasks, sends the tasks to workers through a channel, the workers then handle the task and return it
type ActionManager struct {
	properties *ManagerProperties
	handler    TaskHandler
	parade     Parade
	quit       chan struct{}
	wp         *workerPool
}

func setDefaultProperties(properties *ManagerProperties) *ManagerProperties {
	if properties == nil {
		properties = &ManagerProperties{}
	}
	if properties.Workers == 0 {
		properties.Workers = defaultWorkers
	}
	if properties.ChannelSize == 0 {
		properties.ChannelSize = defaultChannelSize
	}
	if properties.MaxTasks == 0 {
		properties.MaxTasks = defaultMaxTasks
	}
	if properties.WaitTime == nil {
		waitTime := defaultWaitTime
		properties.WaitTime = &waitTime
	}
	if properties.ErrWaitTime == nil {
		errWaitTime := defaultErrWaitTime
		properties.ErrWaitTime = &errWaitTime
	}
	if properties.MaxDuration == nil {
		maxDuration := defaultMaxDuration
		properties.MaxDuration = &maxDuration
	}
	return properties
}

// NewActionManager initiates an ActionManager with workers and returns a
func NewActionManager(handler TaskHandler, parade Parade, properties *ManagerProperties) *ActionManager {
	a := &ActionManager{
		handler:    handler,
		parade:     parade,
		properties: setDefaultProperties(properties),
		quit:       nil,
	}
	a.start()
	return a
}

func (a *ActionManager) Close() {
	close(a.quit)
	a.wp.Close()
}

func (a *ActionManager) start() {
	taskChannel := make(chan OwnedTaskData, a.properties.ChannelSize)
	a.quit = make(chan struct{})
	a.wp = newWorkerPool(a.handler, taskChannel, a.parade, a.properties.Workers)
	go func() {
		for {
			select {
			case <-a.quit:
				return
			default:
				ownedTasks, err := a.parade.OwnTasks(a.handler.Actor(), a.properties.MaxTasks, a.handler.Actions(), a.properties.MaxDuration)
				if err != nil {
					logging.Default().WithField("actor", a.handler.Actor()).Errorf("manager failed to receive tasks: %s", err)
					time.Sleep(*a.properties.WaitTime)
				}
				for _, ot := range ownedTasks {
					a.wp.ch <- ot
				}
				if len(ownedTasks) == 0 {
					time.Sleep(*a.properties.WaitTime)
				}
			}
		}
	}()
}

type workerPool struct {
	handler TaskHandler
	ch      chan OwnedTaskData
	workers int
	wg      sync.WaitGroup
	parade  Parade
}

func newWorkerPool(handler TaskHandler, ch chan OwnedTaskData, parade Parade, workers int) *workerPool {
	a := &workerPool{
		handler: handler,
		ch:      ch,
		workers: workers,
		wg:      sync.WaitGroup{},
		parade:  parade,
	}
	a.start()
	return a
}

func (a *workerPool) Close() {
	close(a.ch)
	a.wg.Wait()
}

func (a *workerPool) start() {
	a.wg.Add(a.workers)
	for i := 0; i < a.workers; i++ {
		go func() {
			workerID := uuid.New()
			defer a.wg.Done()
			for task := range a.ch {
				res := a.handler.Handle(task.Action, task.Body)
				err := a.parade.ReturnTask(task.ID, task.Token, res.Status, res.StatusCode)
				if err != nil {
					logging.Default().WithFields(logging.Fields{
						"action":          task.Action,
						"task workerID":   task.ID,
						"status":          res.Status,
						"status code:":    res.StatusCode,
						"worker workerID": workerID,
					}).Errorf("failed to return task: %w", err)
				}
			}
		}()
	}
}
