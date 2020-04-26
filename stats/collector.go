package stats

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/logging"
)

const (
	DefaultCollectorEventBufferSize = 1024 * 1024
	DefaultFlushInterval            = time.Second * 600
)

type Collector interface {
	Collect(class, action string)
}

type Metric struct {
	Class string `json:"class"`
	Name  string `json:"name"`
	Value uint64 `json:"value"`
}

type InputEvent struct {
	Email     string   `json:"email"`
	ProcessId string   `json:"process_id"`
	Time      string   `json:"time"`
	Metrics   []Metric `json:"metrics"`
}

type primaryKey struct {
	class  string
	action string
}

func (p primaryKey) String() string {
	return fmt.Sprintf("%s/%s", p.class, p.action)
}

type keyIndex map[primaryKey]uint64

type FlushTicker interface {
	Stop()
	Tick() <-chan time.Time
}

type TimeTicker struct {
	ticker *time.Ticker
}

func (t *TimeTicker) Stop() {
	t.ticker.Stop()
}

func (t *TimeTicker) Tick() <-chan time.Time {
	return t.ticker.C
}

type BufferedCollector struct {
	cache       keyIndex
	writes      chan primaryKey
	sender      Sender
	flushTicker FlushTicker
	done        chan bool
}

type BufferedCollectorOpts func(s *BufferedCollector)

func WithWriteBufferSize(bufferSize int) BufferedCollectorOpts {
	return func(s *BufferedCollector) {
		s.writes = make(chan primaryKey, bufferSize)
	}
}

func WithSender(sender Sender) BufferedCollectorOpts {
	return func(s *BufferedCollector) {
		s.sender = sender
	}
}

func WithTicker(t FlushTicker) BufferedCollectorOpts {
	return func(s *BufferedCollector) {
		s.flushTicker = t
	}
}

func NewBufferedCollector(opts ...BufferedCollectorOpts) *BufferedCollector {
	s := &BufferedCollector{
		cache:       make(keyIndex),
		writes:      make(chan primaryKey, DefaultCollectorEventBufferSize),
		done:        make(chan bool),
		sender:      NewDummySender(),
		flushTicker: &TimeTicker{ticker: time.NewTicker(DefaultFlushInterval)},
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *BufferedCollector) incr(k primaryKey) {
	if current, exists := s.cache[k]; !exists {
		s.cache[k] = 1
	} else {
		s.cache[k] = current + 1
	}
}

func (s *BufferedCollector) drain() {
	for {
		select {
		case w := <-s.writes:
			s.incr(w)
		default:
			return
		}
	}
}

func (s *BufferedCollector) send() {
	metrics := makeMetrics(s.cache)
	if len(metrics) == 0 {
		return
	}
	err := s.sender.Send(metrics)
	if err != nil {
		logging.Default().
			WithError(err).
			WithField("service", "stats_collector").
			Debug("could not send stats")
	}
	s.cache = make(keyIndex)
}

func (s *BufferedCollector) Collect(class, action string) {
	s.writes <- primaryKey{
		class:  class,
		action: action,
	}
}

func (s *BufferedCollector) Done() <-chan bool {
	return s.done
}

func (s *BufferedCollector) Run(ctx context.Context) {
	for {
		select {
		case w := <-s.writes: // collect events
			s.incr(w)
		case <-s.flushTicker.Tick(): // every N seconds, send the collected events
			s.send()
		case <-ctx.Done(): // we're done
			s.drain()
			s.send()
			s.done <- true
			return
		}
	}
}

func makeMetrics(counters keyIndex) []Metric {
	metrics := make([]Metric, len(counters))
	i := 0
	for k, v := range counters {
		metrics[i] = Metric{
			Class: k.class,
			Name:  k.action,
			Value: v,
		}
		i++
	}
	return metrics
}
