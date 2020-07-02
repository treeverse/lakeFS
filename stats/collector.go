package stats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/treeverse/lakefs/logging"
)

const (
	DefaultCollectorEventBufferSize = 1024 * 1024
	DefaultFlushInterval            = time.Second * 600
	DefaultSendTimeout              = time.Second * 5
)

type Collector interface {
	SetInstallationID(installationID string)
	CollectEvent(class, action string)
	CollectMetadata(accountMetadata map[string]string)
}

type Metric struct {
	Class string `json:"class"`
	Name  string `json:"name"`
	Value uint64 `json:"value"`
}

type InputEvent struct {
	InstallationID string   `json:"installation_id"`
	ProcessID      string   `json:"process_id"`
	Time           string   `json:"time"`
	Metrics        []Metric `json:"metrics"`
}

type MetadataEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Metadata struct {
	InstallationID string          `json:"installation_id"`
	Entries        []MetadataEntry `json:"entries"`
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
	cache          keyIndex
	writes         chan primaryKey
	sender         Sender
	sendTimeout    time.Duration
	flushTicker    FlushTicker
	done           chan bool
	mutex          *sync.RWMutex
	installationID string
	processID      string
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

func WithFlushInterval(d time.Duration) BufferedCollectorOpts {
	return func(s *BufferedCollector) {
		s.flushTicker = &TimeTicker{ticker: time.NewTicker(d)}
	}
}

func WithSendTimeout(d time.Duration) BufferedCollectorOpts {
	return func(s *BufferedCollector) {
		s.sendTimeout = d
	}
}

func NewBufferedCollector(installationID, processID string, opts ...BufferedCollectorOpts) *BufferedCollector {
	s := &BufferedCollector{
		cache:          make(keyIndex),
		writes:         make(chan primaryKey, DefaultCollectorEventBufferSize),
		done:           make(chan bool),
		sender:         NewDummySender(),
		sendTimeout:    DefaultSendTimeout,
		flushTicker:    &TimeTicker{ticker: time.NewTicker(DefaultFlushInterval)},
		installationID: installationID,
		mutex:          &sync.RWMutex{},
		processID:      processID,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}
func (s *BufferedCollector) getInstallationID() string {
	s.mutex.RLock()
	s.mutex.RUnlock()
	return s.installationID
}

func (s *BufferedCollector) incr(k primaryKey) {
	if current, exists := s.cache[k]; !exists {
		s.cache[k] = 1
	} else {
		s.cache[k] = current + 1
	}
}

func (s *BufferedCollector) send(metrics []Metric) {
	if len(metrics) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()
	err := s.sender.Send(ctx, s.getInstallationID(), s.processID, metrics)
	if err != nil {
		logging.Default().
			WithError(err).
			WithField("service", "stats_collector").
			Debug("could not send stats")
	}

}

func (s *BufferedCollector) CollectEvent(class, action string) {
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
			metrics := makeMetrics(s.cache)
			s.cache = make(keyIndex)
			go s.send(metrics) // no need to block on this
		case <-ctx.Done(): // we're done
			metrics := makeMetrics(s.cache)
			s.send(metrics)
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

func (s *BufferedCollector) SetInstallationID(installationID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.installationID = installationID
}

func (s *BufferedCollector) CollectMetadata(accountMetadata map[string]string) {
	entries := make([]MetadataEntry, len(accountMetadata))
	i := 0
	for k, v := range accountMetadata {
		entries[i] = MetadataEntry{Name: k, Value: v}
		i++
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()
	err := s.sender.UpdateMetadata(ctx, Metadata{
		InstallationID: s.getInstallationID(),
		Entries:        entries,
	})
	if err != nil {
		logging.Default().
			WithError(err).
			WithField("service", "stats_collector").
			Debug("could not update metadata")
	}
}
