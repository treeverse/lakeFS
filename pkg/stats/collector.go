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

	// Close must be called to ensure the delivery of pending stats
	Close()
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
	heartbeatTicker  FlushTicker
	installationID   string
	processID        string
	runtimeCollector func() map[string]string
	runtimeStats     map[string]string

	pendingWrites   sync.WaitGroup
	pendingRequests sync.WaitGroup
	ctxCancelled    int32
	done            chan bool
	runCalled       int32
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
		cache:           make(keyIndex),
		writes:          make(chan primaryKey, collectorEventBufferSize),
		sender:          NewDummySender(),
		sendTimeout:     sendTimeout,
		runtimeStats:    map[string]string{},
		flushTicker:     &TimeTicker{ticker: time.NewTicker(flushInterval)},
		heartbeatTicker: &TimeTicker{ticker: time.NewTicker(heartbeatInterval)},
		installationID:  installationID,
		processID:       processID,
		pendingWrites:   sync.WaitGroup{},
		pendingRequests: sync.WaitGroup{},
		ctxCancelled:    0,
		done:            make(chan bool),
		runCalled:       0,
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
	s.pendingRequests.Add(1)
	go func() {
		defer s.pendingRequests.Done()
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
	}()
}

func (s *BufferedCollector) CollectEvent(class, action string) {
	if s.isCtxCancelled() {
		return
	}
	s.pendingWrites.Add(1)
	defer s.pendingWrites.Done()

	s.writes <- primaryKey{
		class:  class,
		action: action,
	}
}

func (s *BufferedCollector) isCtxCancelled() bool {
	return atomic.LoadInt32(&s.ctxCancelled) == 1
}

func (s *BufferedCollector) Run(ctx context.Context) {
	atomic.StoreInt32(&s.runCalled, 1)
	go func() {
		for {
			select {
			case w := <-s.writes: // collect events
				s.incr(w)
			case <-s.heartbeatTicker.Tick():
				s.incr(primaryKey{
					class:  "global",
					action: "heartbeat",
				})
			case <-s.flushTicker.Tick(): // every N seconds, send the collected events
				s.handleRuntimeStats()
				metrics := makeMetrics(s.cache)
				s.cache = make(keyIndex)
				s.send(metrics)
			case <-ctx.Done(): // we're done
				close(s.done)
				return
			}
		}
	}()
}

func (s *BufferedCollector) Close() {
	if atomic.LoadInt32(&s.runCalled) == 0 {
		// nothing to do
		return
	}

	// wait for main loop to exit
	<-s.done

	// block any new write
	atomic.StoreInt32(&s.ctxCancelled, 1)

	// wait until all writes were added
	s.pendingWrites.Wait()

	// drain writes
	close(s.writes)
	for w := range s.writes {
		s.incr(w)
	}
	metrics := makeMetrics(s.cache)
	s.send(metrics)

	// wait for http requests to complete
	s.pendingRequests.Wait()
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
		s.sendRuntimeStats()
	}
}

func (s *BufferedCollector) sendRuntimeStats() {
	s.pendingRequests.Add(1)
	go func() {
		defer s.pendingRequests.Done()

		m := Metadata{InstallationID: s.installationID}
		for k, v := range s.runtimeStats {
			m.Entries = append(m.Entries, MetadataEntry{Name: k, Value: v})
		}

		s.CollectMetadata(&m)
	}()
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
