package testutil

import (
	"fmt"
	"time"
)

type CollectorRequest int

const (
	CollectorRequestStats CollectorRequest = iota
	CollectorRequestHistogram
)

type Stats struct {
	TotalCompleted   int64
	TotalErrors      int64
	CurrentCompleted int64
	CurrentInterval  time.Duration
}

func (s *Stats) String() string {
	return fmt.Sprintf("completed: %d, errors: %d, current rate: %.2f done/second",
		s.TotalCompleted, s.TotalErrors,
		float64(s.CurrentCompleted)/s.CurrentInterval.Seconds())
}

type ResultCollector struct {
	// Have workers write to here
	Results chan Result

	// for getting data out using methods
	requests   chan CollectorRequest
	stats      chan *Stats
	histograms chan *Histogram

	// collected stats
	lastFlush        time.Time
	histogram        *Histogram
	totalCompleted   int64
	totalErrors      int64
	currentCompleted int64
}

func (rc *ResultCollector) flushCurrent() *Stats {
	return &Stats{
		TotalCompleted:   rc.totalCompleted,
		TotalErrors:      rc.totalErrors,
		CurrentCompleted: rc.currentCompleted,
		CurrentInterval:  time.Since(rc.lastFlush),
	}
}

func (rc *ResultCollector) Stats() *Stats {
	rc.requests <- CollectorRequestStats
	return <-rc.stats
}

func (rc *ResultCollector) Histogram() *Histogram {
	rc.requests <- CollectorRequestHistogram
	return <-rc.histograms
}

func (rc *ResultCollector) Collect() {
	for {
		select {
		case result := <-rc.Results:
			rc.totalCompleted++
			rc.currentCompleted++
			if result.Error != nil {
				rc.totalErrors++
			} else {
				rc.histogram.Add(result.Took.Milliseconds())
			}
		case request := <-rc.requests:
			switch request {
			case CollectorRequestHistogram:
				rc.histograms <- rc.histogram.Clone()
			case CollectorRequestStats:
				rc.stats <- rc.flushCurrent()
				rc.currentCompleted = 0
				rc.lastFlush = time.Now()
			}
		}
	}
}

// TODO(ozkatz): make this configurable
var defaultHistogramBuckets = []int64{1, 2, 5, 7, 10, 15, 25, 50, 75, 100, 250, 350, 500, 750, 1000, 5000}

func NewResultCollector(workerResults chan Result) *ResultCollector {
	return &ResultCollector{
		Results:    workerResults,
		requests:   make(chan CollectorRequest),
		stats:      make(chan *Stats),
		histograms: make(chan *Histogram),
		lastFlush:  time.Now(),
		histogram:  NewHistogram(defaultHistogramBuckets),
	}
}
