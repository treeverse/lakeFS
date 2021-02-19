package stress

import (
	"fmt"
	"time"
)

type collectorRequest int

const (
	collectorRequestStats collectorRequest = iota
	collectorRequestHistogram
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
	// Results is the channel workers should write their output to
	Results chan Result

	// for getting data out using methods
	requests   chan collectorRequest
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
	rc.requests <- collectorRequestStats
	return <-rc.stats
}

func (rc *ResultCollector) Histogram() *Histogram {
	rc.requests <- collectorRequestHistogram
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
			case collectorRequestHistogram:
				rc.histograms <- rc.histogram.Clone()
			case collectorRequestStats:
				rc.stats <- rc.flushCurrent()
				rc.currentCompleted = 0
				rc.lastFlush = time.Now()
			}
		}
	}
}

var DefaultHistogramBuckets = []int64{1, 2, 5, 7, 10, 15, 25, 50, 75, 100, 250, 350, 500, 750, 1000, 5000}

func NewResultCollector(workerResults chan Result) *ResultCollector {
	return &ResultCollector{
		Results:    workerResults,
		requests:   make(chan collectorRequest),
		stats:      make(chan *Stats),
		histograms: make(chan *Histogram),
		lastFlush:  time.Now(),
		histogram:  NewHistogram(DefaultHistogramBuckets),
	}
}
