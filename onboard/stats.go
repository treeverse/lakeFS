package onboard

import (
	"sync/atomic"
	"time"
)

type Stats struct {
	AddedOrChanged       *int64
	Deleted              *int64
	DryRun               bool
	PreviousInventoryURL string
	CommitRef            string
	PreviousImportDate   time.Time
	cb                   ProgressCallback
}

type ProgressEvent Stats

type ProgressCallback func(event *ProgressEvent)

func NewStats(cb ProgressCallback) *Stats {
	s := &Stats{
		AddedOrChanged: new(int64),
		Deleted:        new(int64),
		cb:             cb,
	}
	s.reportProgress()
	return s
}

func (s *Stats) reportProgress() {
	if s.cb != nil {
		s.cb(&ProgressEvent{
			AddedOrChanged: s.AddedOrChanged,
			Deleted:        s.Deleted,
		})
	}
}

func (s *Stats) AddCreated(n int64) {
	s.reportProgress()
	atomic.AddInt64(s.AddedOrChanged, n)
}

func (s *Stats) AddDeleted(n int64) {
	s.reportProgress()
	atomic.AddInt64(s.Deleted, n)
}
