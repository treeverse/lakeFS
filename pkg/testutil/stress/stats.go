package stress

import (
	"fmt"
	"time"
)

type collectorRequest int

const (
	collectorRequestStats collectorRequest = iota
	collectorRequestHistogram
	collectorRequestTotals
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

type TotalStats struct {
	Duration   time.Duration
	NumTotal   int64
	NumErrors  int64
	FirstError error
}

func (t *TotalStats) String() string {
	var ret string
	if t.FirstError != nil {
		ret = fmt.Sprintf("First error: %s\n", t.FirstError)
	}
	ret += fmt.Sprintf("%d total with %d failures in %s\n%f successes/s\n%f failures/s",
		t.NumTotal, t.NumErrors, t.Duration,
		float64(t.NumTotal)/t.Duration.Seconds(),
		float64(t.NumErrors)/t.Duration.Seconds(),
	)
	return ret
}

type ResultCollector struct {
	// Results is the channel workers should write their output to
	Results chan Result

	// for getting data out using methods
	requests   chan collectorRequest
	stats      chan *Stats
	histograms chan *Histogram
	totals     chan *TotalStats

	// collected stats
	firstError       error
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

func (rc *ResultCollector) Total() *TotalStats {
	rc.requests <- collectorRequestTotals
	return <-rc.totals
}

func (rc *ResultCollector) Histogram() *Histogram {
	rc.requests <- collectorRequestHistogram
	return <-rc.histograms
}

func (rc *ResultCollector) Collect() {
	start := time.Now()
	for {
		select {
		case result := <-rc.Results:
			rc.totalCompleted++
			rc.currentCompleted++
			if result.Error != nil {
				rc.totalErrors++
				if rc.firstError == nil {
					rc.firstError = result.Error
				}
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
			case collectorRequestTotals:
				rc.totals <- &TotalStats{
					Duration:   time.Since(start),
					NumTotal:   rc.totalCompleted,
					NumErrors:  rc.totalErrors,
					FirstError: rc.firstError,
				}
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
		totals:     make(chan *TotalStats),
		lastFlush:  time.Now(),
		histogram:  NewHistogram(DefaultHistogramBuckets),
	}
}
