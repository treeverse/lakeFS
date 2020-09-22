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
}

func NewStats() *Stats {
	s := &Stats{
		AddedOrChanged: new(int64),
		Deleted:        new(int64),
	}
	return s
}

func (s *Stats) AddCreated(n int64) {
	atomic.AddInt64(s.AddedOrChanged, n)
}

func (s *Stats) AddDeleted(n int64) {
	atomic.AddInt64(s.Deleted, n)
}
