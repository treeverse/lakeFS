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
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	collectorEventBufferSize = 8 * 1024
	flushInterval            = 10 * time.Minute

	// heartbeatInterval is the interval between 2 heartbeat events.
	heartbeatInterval = time.Hour
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
	// cache used to count event occurrences until we flush and send the data
	cache keyIndex
	// updatesCh holds reported metrics by the application and update the cache
	updatesCh chan Metric
	// sender used to post the collected metrics collected in cache and metadata
	sender Sender
	// sendCh used to post collected metrics. enables to let the sender post data without blocking updates from the application
	sendCh chan *InputEvent
	// flushTicker triggers flush to send the collected events in cache so far
	flushTicker FlushTicker
	// flushSize when cache size gets to flushSize we flush data
	flushSize int
	// heartbeatTicker trigger sending heartbeat event
	heartbeatTicker *time.Ticker
	// installationID posted as part of each report
	installationID string
	// processID unique identifier to distinguish between processes from the same install
	processID string
	// runtimeCollector callback func (optional) enable reporting metadata collected using runtime. triggered each flush.
	runtimeCollector func() map[string]string
	// runtimeStats holds last values from runtimeCollector
	runtimeStats map[string]string
	// inFlight monitor active calls to CollectEvents. Enables waiting until application code is no longer uses updatesCh.
	inFlight sync.WaitGroup
	// stopped used as flag that the collector is stopped. stop processing CollectEvents.
	stopped int32
	// extended stats indicate if we post extra stats on each BI or
	extended bool
	// log is the logger
	log logging.Logger
	// wg used to wait until two main goroutines are done
	wg sync.WaitGroup
}

type BufferedCollectorOpts func(s *BufferedCollector)

func WithWriteBufferSize(bufferSize int) BufferedCollectorOpts {
	return func(s *BufferedCollector) {
		s.updatesCh = make(chan Metric, bufferSize)
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

type Config struct {
	Enabled       bool
	Address       string
	FlushInterval time.Duration
	FlushSize     int
	Extended      bool
}

func NewBufferedCollector(installationID string, cfg Config, opts ...BufferedCollectorOpts) *BufferedCollector {
	if cfg.Enabled && strings.HasPrefix(version.Version, version.UnreleasedVersion) {
		cfg.Enabled = false
	}
	// must have flush interval setup
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = flushInterval
	}
	s := &BufferedCollector{
		cache:           make(keyIndex),
		updatesCh:       make(chan Metric, collectorEventBufferSize),
		runtimeStats:    map[string]string{},
		flushTicker:     &TimeTicker{ticker: time.NewTicker(cfg.FlushInterval)},
		flushSize:       cfg.FlushSize,
		heartbeatTicker: time.NewTicker(heartbeatInterval),
		installationID:  installationID,
		processID:       uuid.Must(uuid.NewUUID()).String(),
		inFlight:        sync.WaitGroup{},
		stopped:         1,
		extended:        cfg.Extended,
		log:             logging.Dummy(),
		sendCh:          make(chan *InputEvent, 1), // buffered as like to check if sender is free
	}
	for _, opt := range opts {
		opt(s)
	}

	// if no sender, assign default based on configuration
	if s.sender == nil {
		if cfg.Enabled {
			s.sender = NewHTTPSender(cfg.Address, s.log)
		} else {
			s.sender = &dummySender{Logger: s.log}
		}
	}
	return s
}

func (s *BufferedCollector) getInstallationID() string {
	return s.installationID
}

func (s *BufferedCollector) incr(k Event) {
	s.cache[k]++
}

func (s *BufferedCollector) update(k Metric) {
	s.cache[k.Event] += k.Value
}

func (s *BufferedCollector) newInputEvent() *InputEvent {
	metrics := makeMetrics(s.cache)
	if len(metrics) == 0 {
		return nil
	}
	return &InputEvent{
		InstallationID: s.getInstallationID(),
		ProcessID:      s.processID,
		Time:           time.Now().Format(time.RFC3339),
		Metrics:        metrics,
	}
}

func (s *BufferedCollector) CollectEvents(ev Event, count uint64) {
	if s.isStopped() {
		return
	}
	s.inFlight.Add(1)
	defer s.inFlight.Done()

	// hash or clear extended fields on the event
	if s.extended {
		ev = ev.HashExtended()
	} else {
		ev = ev.ClearExtended()
	}
	s.updatesCh <- Metric{Event: ev, Value: count}
}

func (s *BufferedCollector) CollectEvent(ev Event) {
	s.CollectEvents(ev, 1)
}

func (s *BufferedCollector) isStopped() bool {
	return atomic.LoadInt32(&s.stopped) == 1
}

func (s *BufferedCollector) Start(ctx context.Context) {
	if !s.isStopped() {
		return
	}

	const backgroundLoops = 2
	s.wg.Add(backgroundLoops)
	// update metrics sent on updatesCh
	go s.updateMetricsLoop(ctx)
	// post metrics waiting in sendCh
	// we use background context as we do not want to cancel posting based on context
	go s.sendMetricsLoop(context.Background())

	atomic.StoreInt32(&s.stopped, 0)
}

func (s *BufferedCollector) sendMetricsLoop(ctx context.Context) {
	defer s.wg.Done()
	s.processSendCh(ctx)
}

func (s *BufferedCollector) updateMetricsLoop(ctx context.Context) {
	defer func() {
		close(s.sendCh)
		s.wg.Done()
	}()
	for {
		select {
		case w, ok := <-s.updatesCh:
			// process updates and flush if flush (send) if needed
			if !ok {
				return
			}
			s.update(w)
			if len(s.cache) >= s.flushSize {
				s.flush()
			}

		case <-s.heartbeatTicker.C:
			s.incr(Event{
				Class: "global",
				Name:  "heartbeat",
			})

		case <-s.flushTicker.Tick():
			// every N seconds, flush (send) the collected events and runtime stats (if needed)
			s.flush()
			s.handleRuntimeStats()

		case <-ctx.Done():
			return
		}
	}
}

// flush send the current cache information using SendEvents
func (s *BufferedCollector) flush() {
	if len(s.sendCh) > 0 {
		return
	}
	event := s.newInputEvent()
	if event == nil {
		return
	}
	// try passing the current metrics to the sender, if we fail
	// we keep the current cache state and retry on the next flush
	select {
	case s.sendCh <- event:
		// reset the cache if we managed to send the event
		s.cache = make(keyIndex)
	default:
		// keep data in cache for the next time we try to send
	}
}

func (s *BufferedCollector) Stop() {
	// stop, return if already marked as stopped
	if !atomic.CompareAndSwapInt32(&s.stopped, 0, 1) {
		return
	}

	// wait until no new  in-flight requests
	s.inFlight.Wait()

	// close update channel as no more updates will arrive,
	// update loop will exit and close the sendCh.
	// closing the sendCh will cause the sendLoop to exit.
	close(s.updatesCh)
	s.wg.Wait()
}

func (s *BufferedCollector) Close() {
	s.Stop()
	// drain updates and send cache state anything that left
	ctx := context.Background()
	for upd := range s.updatesCh {
		s.update(upd)
	}
	if evt := s.newInputEvent(); evt != nil {
		s.sendEventHelper(ctx, evt)
	}
}

// processSendCh post metrics waiting on sendCh. Stops when 'sendCh' is stopped.
func (s *BufferedCollector) processSendCh(ctx context.Context) {
	for evt := range s.sendCh {
		s.sendEventHelper(ctx, evt)
	}
}

func (s *BufferedCollector) sendEventHelper(ctx context.Context, event *InputEvent) {
	err := s.sender.SendEvent(ctx, event)
	if err != nil {
		s.log.WithError(err).Debug("Failed sending stats event")
	}
}

func makeMetrics(counters keyIndex) []Metric {
	metrics := make([]Metric, 0, len(counters))
	for k, v := range counters {
		metrics = append(metrics, Metric{Event: k, Value: v})
	}
	return metrics
}

func (s *BufferedCollector) CollectMetadata(accountMetadata *Metadata) {
	ctx := context.Background()
	err := s.sender.UpdateMetadata(ctx, *accountMetadata)
	if err != nil {
		s.log.WithError(err).Debug("could not update metadata")
	}
}

func (s *BufferedCollector) CollectCommPrefs(email, installationID string, featureUpdates, securityUpdates bool) {
	commPrefs := &CommPrefsData{
		Email:           email,
		InstallationID:  installationID,
		FeatureUpdates:  featureUpdates,
		SecurityUpdates: securityUpdates,
	}
	ctx := context.Background()
	err := s.sender.UpdateCommPrefs(ctx, commPrefs)
	if err != nil {
		s.log.WithError(err).Info("could not update comm prefs")
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
	m := Metadata{InstallationID: s.installationID}
	for k, v := range s.runtimeStats {
		m.Entries = append(m.Entries, MetadataEntry{Name: k, Value: v})
	}
	s.CollectMetadata(&m)
}

func HashMetricValue(s string) string {
	if s == "" {
		return s
	}
	v := xxhash.Sum64String(s)
	return fmt.Sprintf("%02x", v)
}
