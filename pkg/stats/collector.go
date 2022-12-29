package stats

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	collectorEventBufferSize = 8 * 1024
	flushInterval            = 10 * time.Minute
	sendTimeout              = 5 * time.Second

	// heartbeatInterval is the interval between 2 heartbeat events.
	heartbeatInterval = 60 * time.Minute
)

type Collector interface {
	CollectEvent(ev Event)
	CollectEvents(ev Event, count uint64)
	CollectMetadata(accountMetadata *Metadata)
	CollectCommPrefs(email, installationID string, featureUpdates, securityUpdates bool)
	SetInstallationID(installationID string)

	// Close must be called to ensure the delivery of pending stats
	Close()
}

type Event struct {
	Class      string `json:"class"`
	Name       string `json:"name"`
	Repository string `json:"repository,omitempty"`
	Ref        string `json:"ref,omitempty"`
	SourceRef  string `json:"source_ref,omitempty"`
	UserID     string `json:"user_id,omitempty"`
	Client     string `json:"client,omitempty"`
}

type EventWithCount struct {
	ev    Event
	count uint64
}

// ClearExtended clear values of *all* extended fields
func (e Event) ClearExtended() Event {
	e.Repository = ""
	e.Ref = ""
	e.SourceRef = ""
	e.UserID = ""
	e.Client = ""
	return e
}

// HashExtended hash the values of extended fields with sensitive information
func (e Event) HashExtended() Event {
	e.Repository = HashMetricValue(e.Repository)
	e.Ref = HashMetricValue(e.Ref)
	e.SourceRef = HashMetricValue(e.SourceRef)
	e.UserID = HashMetricValue(e.UserID)
	// e.Client - no need to hash the client value
	return e
}

type Metric struct {
	Event
	Value uint64 `json:"value"`
}

type InputEvent struct {
	InstallationID string   `json:"installation_id"`
	ProcessID      string   `json:"process_id"`
	Time           string   `json:"time"`
	Metrics        []Metric `json:"metrics"`
}

type CommPrefsData struct {
	InstallationID  string `json:"installation_id"`
	Email           string `json:"email"`
	FeatureUpdates  bool   `json:"featureUpdates"`
	SecurityUpdates bool   `json:"securityUpdates"`
}

type keyIndex map[Event]uint64

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
	writes           chan EventWithCount
	sender           Sender
	sendTimeout      time.Duration
	flushTicker      FlushTicker
	flushSize        int
	heartbeatTicker  FlushTicker
	installationID   string
	processID        string
	runtimeCollector func() map[string]string
	runtimeStats     map[string]string
	pendingWrites    sync.WaitGroup
	pendingRequests  sync.WaitGroup
	ctxCancelled     int32
	done             chan bool
	runCalled        int32
	extended         bool
	log              logging.Logger
}

type BufferedCollectorOpts func(s *BufferedCollector)

func WithWriteBufferSize(bufferSize int) BufferedCollectorOpts {
	return func(s *BufferedCollector) {
		s.writes = make(chan EventWithCount, bufferSize)
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

func WithExtended(b bool) BufferedCollectorOpts {
	return func(s *BufferedCollector) {
		s.extended = b
	}
}

func WithLogger(l logging.Logger) BufferedCollectorOpts {
	return func(s *BufferedCollector) {
		s.log = l
	}
}

func NewBufferedCollector(installationID string, c *config.Config, opts ...BufferedCollectorOpts) *BufferedCollector {
	statsEnabled := true
	flushDuration := flushInterval
	extended := false
	flushSize := config.DefaultStatsFlushSize
	if c != nil {
		statsEnabled = c.GetStatsEnabled() && !strings.HasPrefix(version.Version, version.UnreleasedVersion)
		flushDuration = c.GetStatsFlushInterval()
		flushSize = c.GetStatsFlushSize()
		extended = c.GetStatsExtended()
	}
	s := &BufferedCollector{
		cache:           make(keyIndex),
		writes:          make(chan EventWithCount, collectorEventBufferSize),
		runtimeStats:    map[string]string{},
		flushTicker:     &TimeTicker{ticker: time.NewTicker(flushDuration)},
		flushSize:       flushSize,
		heartbeatTicker: &TimeTicker{ticker: time.NewTicker(heartbeatInterval)},
		installationID:  installationID,
		processID:       uuid.Must(uuid.NewUUID()).String(),
		pendingWrites:   sync.WaitGroup{},
		pendingRequests: sync.WaitGroup{},
		ctxCancelled:    0,
		done:            make(chan bool),
		sendTimeout:     sendTimeout,
		extended:        extended,
		log:             logging.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}

	// re-assign logger with service name
	s.log = s.log.WithField("service", "stats_collector")

	// assign sender
	if s.sender == nil {
		if statsEnabled {
			s.sender = NewHTTPSender(c.GetStatsAddress(), s.sendTimeout, time.Now)
		} else {
			s.sender = &dummySender{Log: s.log}
		}
	}
	return s
}

func (s *BufferedCollector) getInstallationID() string {
	return s.installationID
}

func (s *BufferedCollector) incr(k EventWithCount) {
	s.cache[k.ev] += k.count
}

func (s *BufferedCollector) send(metrics []Metric) {
	s.pendingRequests.Add(1)
	go func() {
		defer s.pendingRequests.Done()
		if len(metrics) == 0 {
			return
		}
		err := s.sender.SendEvent(context.Background(), s.getInstallationID(), s.processID, metrics)
		if err != nil {
			s.log.WithError(err).WithField("service", "stats_collector").Debug("could not send stats")
		}
	}()
}

func (s *BufferedCollector) CollectEvents(ev Event, count uint64) {
	if s.isCtxCancelled() {
		return
	}
	s.pendingWrites.Add(1)
	defer s.pendingWrites.Done()

	// hash or clear extended fields on the event
	if s.extended {
		ev = ev.HashExtended()
	} else {
		ev = ev.ClearExtended()
	}
	s.writes <- EventWithCount{ev, count}
}

func (s *BufferedCollector) CollectEvent(ev Event) {
	s.CollectEvents(ev, 1)
}

func (s *BufferedCollector) isCtxCancelled() bool {
	return atomic.LoadInt32(&s.ctxCancelled) == 1
}

func (s *BufferedCollector) Run(ctx context.Context) {
	if atomic.LoadInt32(&s.runCalled) != 0 {
		return
	}
	atomic.StoreInt32(&s.runCalled, 1)
	go func() {
		for {
			select {
			case w := <-s.writes:
				// collect events, and flush if needed by size
				s.incr(w)
				if len(s.cache) >= s.flushSize {
					s.flush()
				}
			case <-s.heartbeatTicker.Tick():
				// collect heartbeat
				s.incr(EventWithCount{Event{
					Class: "global",
					Name:  "heartbeat",
				}, 1})
			case <-s.flushTicker.Tick():
				// every N seconds, send the collected events
				s.flush()
			case <-ctx.Done(): // we're done
				close(s.done)
				return
			}
		}
	}()
}

func (s *BufferedCollector) flush() {
	s.handleRuntimeStats()
	if len(s.cache) == 0 {
		// nothing to flush
		return
	}
	metrics := makeMetrics(s.cache)
	s.cache = make(keyIndex)
	s.send(metrics)
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
			Event: k,
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
		s.log.WithError(err).WithField("service", "stats_collector").Debug("could not update metadata")
	}
}

func (s *BufferedCollector) CollectCommPrefs(email, installationID string, featureUpdates, securityUpdates bool) {
	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()
	commPrefs := &CommPrefsData{
		Email:           email,
		InstallationID:  installationID,
		FeatureUpdates:  featureUpdates,
		SecurityUpdates: securityUpdates,
	}
	err := s.sender.UpdateCommPrefs(ctx, commPrefs)
	if err != nil {
		s.log.WithError(err).WithField("service", "stats_collector").Info("could not update comm prefs")
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

func HashMetricValue(s string) string {
	if s == "" {
		return s
	}
	v := xxhash.Sum64String(s)
	return fmt.Sprintf("%02x", v)
}
