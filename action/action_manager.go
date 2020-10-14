package action

import (
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/parade"
	"sync"
	"time"
)

const (
	defaultWorkers     = 5
	defaultChannelSize = 1000
	defaultMaxTasks    = 500
	defaultWaitTime    = time.Millisecond * 10
	defaultMaxDuration = time.Minute * 30 // Todo(guys): change this
)

type Properties struct {
	workers     int
	channelSize int
	maxTasks    int
	waitTime    *time.Duration
	maxDuration *time.Duration
}

type Action struct {
	properties  *Properties
	handler     TaskHandler
	parade      parade.Parade
	quit        chan bool
	actionGroup *workerPool
}

func setDefaultProperties(properties *Properties) *Properties {
	if properties == nil {
		properties = &Properties{}
	}
	if properties.workers == 0 {
		properties.workers = defaultWorkers
	}
	if properties.channelSize == 0 {
		properties.channelSize = defaultChannelSize
	}
	if properties.maxTasks == 0 {
		properties.maxTasks = defaultMaxTasks
	}
	if properties.waitTime == nil {
		waitTime := defaultWaitTime
		properties.waitTime = &waitTime
	}
	if properties.maxDuration == nil {
		maxDuration := defaultMaxDuration
		properties.maxDuration = &maxDuration
	}
	return properties
}

func NewAction(handler TaskHandler, parade parade.Parade, properties *Properties) *Action {
	a := &Action{
		handler:    handler,
		parade:     parade,
		properties: setDefaultProperties(properties),
		quit:       nil,
	}
	a.start()
	return a
}

func (a *Action) Close() {
	a.quit <- true
	a.actionGroup.Close()
}

func (a *Action) start() {
	taskChannel := make(chan parade.OwnedTaskData, a.properties.channelSize)
	a.quit = make(chan bool)
	a.actionGroup = newWorkerPool(a.handler, taskChannel, a.parade, a.properties.workers)
	go func() {
		for {
			select {
			case <-a.quit:
				return
			default:
				ownedTasks, err := a.parade.OwnTasks(a.handler.Actor(), a.properties.maxTasks, a.handler.Actions(), a.properties.maxDuration)
				if err != nil {
					logging.Default().WithField("actor", a.handler.Actor()).Errorf("manager failed to receive tasks: %s", err)
					// Todo(guys): handle error case better ( with growing sleep periods and returning eventually
					time.Sleep(*a.properties.waitTime)
				}
				for _, ot := range ownedTasks {
					a.actionGroup.ch <- ot
				}
				if len(ownedTasks) == 0 {
					time.Sleep(*a.properties.waitTime)
				}
			}
		}
	}()
}

type workerPool struct {
	handler TaskHandler
	ch      chan parade.OwnedTaskData
	exit    chan int
	workers int
	wg      sync.WaitGroup
	parade  parade.Parade
}

func newWorkerPool(handler TaskHandler, ch chan parade.OwnedTaskData, parade parade.Parade, workers int) *workerPool {
	a := &workerPool{
		handler: handler,
		ch:      ch,
		exit:    make(chan int),
		workers: workers,
		wg:      sync.WaitGroup{},
		parade:  parade,
	}
	a.start()
	return a
}

func (a *workerPool) Close() {
	close(a.exit)
	close(a.ch)
	a.wg.Wait()
}

func (a *workerPool) start() {
	a.wg.Add(a.workers)
	for i := 0; i < a.workers; i++ {
		go func() {
			defer a.wg.Done()
			for task := range a.ch {
				res := a.handler.Handle(task.Action, task.Body)
				err := a.parade.ReturnTask(task.ID, task.Token, res.Status, res.StatusCode)
				if err != nil {
					logging.Default().WithField("action", task.Action).Errorf("failed to return task: %w", err)
				}
			}
		}()
	}
}
