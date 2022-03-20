package stats

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	collectorEventBufferSize = 1024 * 1024
	flushInterval            = time.Second * 600
	sendTimeout              = time.Second * 5

	// heartbeatInterval is the interval between 2 heartbeat events.
	heartbeatInterval = 60 * time.Minute
)

type Collector interface {
	CollectEvent(class, action string)
	CollectMetadata(accountMetadata *Metadata)
	SetInstallationID(installationID string)
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
	cache            keyIndex
	writes           chan primaryKey
	sender           Sender
	sendTimeout      time.Duration
	flushTicker      FlushTicker
	installationID   string
	processID        string
	runtimeCollector func() map[string]string
	runtimeStats     map[string]string
	// wg used as a semaphore when 'writes' channel is used. on close, we can wait until the channel is free.
	wg sync.WaitGroup
	// closed set to 1 in case 'writes' channel is marked as closed
	closed int32
	// wgRuns used as a semaphore when run loop is used
	wgRun sync.WaitGroup
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

func NewBufferedCollector(installationID string, c *config.Config, opts ...BufferedCollectorOpts) *BufferedCollector {
	processID, moreOpts := getBufferedCollectorArgs(c)
	opts = append(opts, moreOpts...)
	s := &BufferedCollector{
		cache:          make(keyIndex),
		writes:         make(chan primaryKey, collectorEventBufferSize),
		sender:         NewDummySender(),
		sendTimeout:    sendTimeout,
		runtimeStats:   map[string]string{},
		flushTicker:    &TimeTicker{ticker: time.NewTicker(flushInterval)},
		installationID: installationID,
		processID:      processID,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *BufferedCollector) getInstallationID() string {
	return s.installationID
}

func (s *BufferedCollector) incr(k primaryKey) {
	s.cache[k]++
}

func (s *BufferedCollector) send(metrics []Metric) {
	if len(metrics) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()
	err := s.sender.SendEvent(ctx, s.getInstallationID(), s.processID, metrics)
	if err != nil {
		logging.Default().
			WithError(err).
			WithField("service", "stats_collector").
			Debug("could not send stats")
	}
}

func (s *BufferedCollector) isClosed() bool {
	return atomic.LoadInt32(&s.closed) == 1
}

func (s *BufferedCollector) CollectEvent(class, action string) {
	if s.isClosed() {
		return
	}
	s.wg.Add(1)
	s.writes <- primaryKey{
		class:  class,
		action: action,
	}
	s.wg.Done()
}

func (s *BufferedCollector) Close() {
	// mark as closed and return in case it was closed
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return
	}
	// wait for all events to be processed and close the channel
	// done in the background because our main loop cloud be canceled
	go func() {
		s.wg.Wait()
		close(s.writes)
	}()
	// wait for run loop to end
	s.wgRun.Wait()
	// drain channel and send cache for the last time
	for w := range s.writes {
		s.incr(w)
	}
	s.sendCache()
}

func (s *BufferedCollector) Run(ctx context.Context) {
	s.wgRun.Add(1)
	go func() {
		defer func() {
			s.wgRun.Done()
			// we trigger close after we mark that run ended
			s.Close()
		}()
		for {
			select {
			case w, ok := <-s.writes: // collect events
				if !ok {
					return
				}
				s.incr(w)
			case <-s.flushTicker.Tick(): // every N seconds, send the collected events
				s.handleRuntimeStats()
				s.sendCache()
			case <-time.After(heartbeatInterval):
				s.incr(primaryKey{class: "global", action: "heartbeat"})
			case <-ctx.Done(): // we're done
				return
			}
		}
	}()
}

func (s *BufferedCollector) sendCache() {
	if len(s.cache) == 0 {
		return
	}
	metrics := makeMetrics(s.cache)
	for k := range s.cache {
		delete(s.cache, k)
	}
	s.send(metrics)
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

func (s *BufferedCollector) CollectMetadata(accountMetadata *Metadata) {
	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()
	err := s.sender.UpdateMetadata(ctx, *accountMetadata)
	if err != nil {
		logging.Default().
			WithError(err).
			WithField("service", "stats_collector").
			Debug("could not update metadata")
	}
}

func (s *BufferedCollector) SetInstallationID(installationID string) {
	s.installationID = installationID
}

func (s *BufferedCollector) SetRuntimeCollector(runtimeCollector func() map[string]string) {
	s.runtimeCollector = runtimeCollector
}

func (s *BufferedCollector) handleRuntimeStats() {
	if s.runtimeCollector == nil {
		// nothing to do
		return
	}

	currStats := s.runtimeCollector()
	if len(currStats) == 0 {
		return
	}

	anyChange := false
	if len(s.runtimeStats) == 0 {
		// first time runtime stats are reported
		anyChange = true
		s.runtimeStats = currStats
	} else {
		for currK, currV := range currStats {
			if prevV, ok := s.runtimeStats[currK]; !ok || prevV != currV {
				// some reported metric changed, need to report it
				s.runtimeStats[currK] = currV
				anyChange = true
			}
		}
	}

	if anyChange {
		go s.sendRuntimeStats()
	}
}

func (s *BufferedCollector) sendRuntimeStats() {
	m := Metadata{InstallationID: s.installationID}
	for k, v := range s.runtimeStats {
		m.Entries = append(m.Entries, MetadataEntry{Name: k, Value: v})
	}

	s.CollectMetadata(&m)
}

func getBufferedCollectorArgs(c *config.Config) (processID string, opts []BufferedCollectorOpts) {
	if c == nil {
		return "", nil
	}
	var sender Sender
	if c.GetStatsEnabled() && !strings.HasPrefix(version.Version, version.UnreleasedVersion) {
		sender = NewHTTPSender(c.GetStatsAddress(), time.Now)
	} else {
		sender = NewDummySender()
	}
	return uuid.Must(uuid.NewUUID()).String(),
		[]BufferedCollectorOpts{
			WithSender(sender),
			WithFlushInterval(c.GetStatsFlushInterval()),
		}
}
