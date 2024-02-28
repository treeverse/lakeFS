package graveler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	callbackChannelSize = 1000
	graceDuration       = 30 * time.Second
)

type DeleteSensorCB func(repositoryID RepositoryID, branchID BranchID, stagingTokenID StagingToken, inGrace bool)

type StagingTokenCounter struct {
	StagingTokenID StagingToken
	Counter        int
}

type stagingTokenData struct {
	repositoryID   RepositoryID
	branchID       BranchID
	stagingTokenID StagingToken
}

func (s *stagingTokenData) CombinedKey() string {
	return fmt.Sprintf("%s:%s", s.repositoryID, s.branchID)
}

type DeleteSensor struct {
	ctx       context.Context
	cb        DeleteSensorCB
	triggerAt int
	callbacks chan stagingTokenData
	wg        sync.WaitGroup
	mutex     *sync.RWMutex
	// stopped used as flag that the sensor has stopped. stop processing CountDelete.
	stopped                int32
	graceDuration          time.Duration
	branchTombstoneCounter map[string]*StagingTokenCounter
}

type DeleteSensorOpts func(s *DeleteSensor)

func WithCBBufferSize(bufferSize int) DeleteSensorOpts {
	return func(s *DeleteSensor) {
		s.callbacks = make(chan stagingTokenData, bufferSize)
	}
}

func WithGraceDuration(d time.Duration) DeleteSensorOpts {
	return func(s *DeleteSensor) {
		s.graceDuration = d
	}
}
func NewDeleteSensor(ctx context.Context, triggerAt int, cb DeleteSensorCB, opts ...DeleteSensorOpts) *DeleteSensor {
	ds := &DeleteSensor{
		ctx:                    ctx,
		cb:                     cb,
		triggerAt:              triggerAt,
		stopped:                0,
		graceDuration:          graceDuration,
		mutex:                  &sync.RWMutex{},
		branchTombstoneCounter: make(map[string]*StagingTokenCounter),
		callbacks:              make(chan stagingTokenData, callbackChannelSize),
	}
	for _, opt := range opts {
		opt(ds)
	}
	ds.wg.Add(1)
	go ds.processCallbacks()
	return ds
}

func (s *DeleteSensor) isStopped() bool {
	return atomic.LoadInt32(&s.stopped) == 1
}

func (s *DeleteSensor) triggerTombstone(ctx context.Context, st stagingTokenData) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	combinedKey := st.CombinedKey()
	stCounter, ok := s.branchTombstoneCounter[combinedKey]
	if !ok {
		stCounter = &StagingTokenCounter{
			StagingTokenID: st.stagingTokenID,
			Counter:        1,
		}
		s.branchTombstoneCounter[combinedKey] = stCounter
		return
	}
	if stCounter.StagingTokenID != st.stagingTokenID {
		stCounter.StagingTokenID = st.stagingTokenID
		stCounter.Counter = 1
		return
	}
	if stCounter.Counter >= s.triggerAt-1 {
		select {
		case s.callbacks <- st:
			stCounter.Counter = 0
		default:
			logging.FromContext(ctx).WithFields(logging.Fields{"repositoryID": st.repositoryID, "branchID": st.branchID, "stagingTokenID": st.stagingTokenID}).Info("delete sensor callback channel is full, dropping delete event")
		}
		return
	}
	stCounter.Counter++
}

func (s *DeleteSensor) processCallbacks() {
	defer s.wg.Done()
	for cb := range s.callbacks {
		s.cb(cb.repositoryID, cb.branchID, cb.stagingTokenID, s.isStopped())
	}
}

func (s *DeleteSensor) CountDelete(ctx context.Context, repositoryID RepositoryID, branchID BranchID, stagingTokenID StagingToken) {
	if s.isStopped() {
		return
	}
	st := stagingTokenData{
		repositoryID:   repositoryID,
		branchID:       branchID,
		stagingTokenID: stagingTokenID,
	}
	s.triggerTombstone(ctx, st)
}

func (s *DeleteSensor) stop() {
	done := make(chan struct{})
	go func(wg *sync.WaitGroup, doneChan chan struct{}) {
		close(s.callbacks)
		wg.Wait()
		close(doneChan)
	}(&s.wg, done)
	select {
	case <-done:
		return
	case <-time.After(s.graceDuration):
		return
	}
}

func (s *DeleteSensor) Close() {
	// stop, return if already marked as stopped
	if !atomic.CompareAndSwapInt32(&s.stopped, 0, 1) {
		return
	}
	s.stop()
}
