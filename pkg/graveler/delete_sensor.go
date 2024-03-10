package graveler

import (
	"context"
	"sync"

	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	callbackChannelSize = 1000
)

type DeleteSensorCB func(repositoryID RepositoryID, branchID BranchID, stagingTokenID StagingToken, inGrace bool)

// StagingTokenCounter holds a counter for a specific staging token.
type StagingTokenCounter struct {
	StagingTokenID StagingToken
	Counter        int
}

// stagingTokenData holds data regarding a staging token.
type stagingTokenData struct {
	repositoryID   RepositoryID
	branchID       BranchID
	stagingTokenID StagingToken
}

type DeleteSensor struct {
	cb        DeleteSensorCB
	triggerAt int
	callbacks chan stagingTokenData
	wg        sync.WaitGroup
	mutex     sync.Mutex
	// stopped used as flag that the sensor has stopped. stop processing CountDelete.
	stopped                bool
	branchTombstoneCounter map[RepositoryID]map[BranchID]*StagingTokenCounter
}

type DeleteSensorOpts func(s *DeleteSensor)

func WithCBBufferSize(bufferSize int) DeleteSensorOpts {
	return func(s *DeleteSensor) {
		s.callbacks = make(chan stagingTokenData, bufferSize)
	}
}

func NewDeleteSensor(triggerAt int, cb DeleteSensorCB, opts ...DeleteSensorOpts) *DeleteSensor {
	ds := &DeleteSensor{
		cb:                     cb,
		triggerAt:              triggerAt,
		stopped:                false,
		mutex:                  sync.Mutex{},
		branchTombstoneCounter: make(map[RepositoryID]map[BranchID]*StagingTokenCounter),
		callbacks:              make(chan stagingTokenData, callbackChannelSize),
	}
	for _, opt := range opts {
		opt(ds)
	}
	ds.wg.Add(1)
	go ds.processCallbacks()
	return ds
}

// triggerTombstone triggers a tombstone event for a specific staging token. if stopped, the event is not triggered.
// if the staging token has changed, the counter is reset. if the counter reaches the triggerAt value, the event is triggered.
// in case the callback channel is full, the event is dropped and will be retried in the next call.
func (s *DeleteSensor) triggerTombstone(ctx context.Context, st stagingTokenData) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.stopped {
		return
	}
	if _, ok := s.branchTombstoneCounter[st.repositoryID]; !ok {
		s.branchTombstoneCounter[st.repositoryID] = make(map[BranchID]*StagingTokenCounter)
	}
	stCounter, ok := s.branchTombstoneCounter[st.repositoryID][st.branchID]
	if !ok {
		stCounter = &StagingTokenCounter{
			StagingTokenID: st.stagingTokenID,
			Counter:        1,
		}
		s.branchTombstoneCounter[st.repositoryID][st.branchID] = stCounter
		return
	}
	// Reset the counter if the staging token has changed, under the assumption that staging tokens are updated only during sealing processes, which occur during a commit or compaction.
	if stCounter.StagingTokenID != st.stagingTokenID {
		stCounter.StagingTokenID = st.stagingTokenID
		stCounter.Counter = 1
		return
	}
	if stCounter.Counter < s.triggerAt {
		stCounter.Counter++
	}
	if stCounter.Counter >= s.triggerAt {
		select {
		case s.callbacks <- st:
			stCounter.Counter = 0
		default:
			logging.FromContext(ctx).WithFields(logging.Fields{"repositoryID": st.repositoryID, "branchID": st.branchID, "stagingTokenID": st.stagingTokenID}).Info("delete sensor callback channel is full, dropping delete event")
		}
		return
	}
}

func (s *DeleteSensor) processCallbacks() {
	s.mutex.Lock()
	isStopped := s.stopped
	s.mutex.Unlock()
	defer s.wg.Done()
	for cb := range s.callbacks {
		s.cb(cb.repositoryID, cb.branchID, cb.stagingTokenID, isStopped)
	}
}

func (s *DeleteSensor) CountDelete(ctx context.Context, repositoryID RepositoryID, branchID BranchID, stagingTokenID StagingToken) {
	st := stagingTokenData{
		repositoryID:   repositoryID,
		branchID:       branchID,
		stagingTokenID: stagingTokenID,
	}
	s.triggerTombstone(ctx, st)
}

func (s *DeleteSensor) Close() {
	s.mutex.Lock()
	if s.stopped {
		s.mutex.Unlock()
		return
	}
	s.stopped = true
	s.mutex.Unlock()

	close(s.callbacks)
	s.wg.Wait()
}
