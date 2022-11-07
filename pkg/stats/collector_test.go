package stats_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/stats"
)

type mockSender struct {
	metrics  chan []stats.Metric
	metadata chan stats.Metadata
}

func (s *mockSender) SendEvent(ctx context.Context, installationID, processID string, m []stats.Metric) error {
	s.metrics <- m
	return nil
}

func (s *mockSender) UpdateMetadata(ctx context.Context, m stats.Metadata) error {
	s.metadata <- m
	return nil
}

func (s *mockSender) UpdateCommPrefs(ctx context.Context, email string, featureUpdates, securityUpdates bool) error {
	return nil
}

type mockTicker struct {
	tc chan time.Time
}

func (m *mockTicker) Stop() {
}

func (m *mockTicker) makeItTick() {
	m.tc <- time.Now()
}

func (m *mockTicker) Tick() <-chan time.Time {
	return m.tc
}

func setupTest(buffer int, opts ...stats.BufferedCollectorOpts) (*mockSender, *mockTicker, *stats.BufferedCollector) {
	sender := &mockSender{metrics: make(chan []stats.Metric, 10), metadata: make(chan stats.Metadata, 10)}
	ticker := &mockTicker{tc: make(chan time.Time)}
	opts = append(opts, stats.WithSender(sender), stats.WithTicker(ticker), stats.WithWriteBufferSize(buffer))
	collector := stats.NewBufferedCollector("installation_id", nil, opts...)
	collector.SetRuntimeCollector(func() map[string]string {
		return map[string]string{"runtime": "stat"}
	})

	return sender, ticker, collector
}

func TestCallHomeCollector_QuickNoTick(t *testing.T) {
	sender, _, collector := setupTest(10)
	ctx, cancelFn := context.WithCancel(context.Background())

	// Forcing collector to run last so that we make sure a race between context and collector
	// isn't impacting what's being sent
	collector.CollectEvent(stats.Event{Class: "foo", Name: "bar"})
	cancelFn()

	collector.Run(ctx)
	collector.Close()

	counters, ok := <-sender.metrics

	require.True(t, ok)
	require.Len(t, counters, 1)
	require.Equal(t, counters[0].Class, "foo")
	require.Equal(t, counters[0].Name, "bar")
	require.Equal(t, counters[0].Value, uint64(1))
}

func TestCallHomeCollector_Collect(t *testing.T) {
	sender, ticker, collector := setupTest(0)
	ctx, cancelFn := context.WithCancel(context.Background())

	collector.Run(ctx)

	// add metrics
	collector.CollectEvent(stats.Event{Class: "foo", Name: "bar"})
	collector.CollectEvent(stats.Event{Class: "foo", Name: "bar"})
	collector.CollectEvent(stats.Event{Class: "foo", Name: "bar"})
	collector.CollectEvent(stats.Event{Class: "foo", Name: "bazzz"})
	collector.CollectEvent(stats.Event{Class: "foo", Name: "bazzz"})
	collector.CollectEvent(stats.Event{Class: "other", Name: "bar"})

	// ensure we flush at the given interval
	ticker.makeItTick()

	counters := <-sender.metrics

	keys := 0
	for _, counter := range counters {
		if counter.Class == "foo" && counter.Name == "bar" {
			keys++
			if counter.Value != 3 {
				t.Fatalf("expected count %d for foo/bar, got %d", 3, counter.Value)
			}
		}
		if counter.Class == "foo" && counter.Name == "bazzz" {
			keys++
			if counter.Value != 2 {
				t.Fatalf("expected count %d for foo/bazzz, got %d", 2, counter.Value)
			}
		}
		if counter.Class == "other" && counter.Name == "bar" {
			keys++
			if counter.Value != 1 {
				t.Fatalf("expected count %d for foo/bazzz, got %d", 1, counter.Value)
			}
		}
	}
	if keys != 3 {
		t.Fatalf("expected all %d keys, got %d", 3, keys)
	}

	collector.CollectEvent(stats.Event{Class: "foo", Name: "bar"})

	cancelFn()
	collector.Close()

	counters, ok := <-sender.metrics // ensure we get another "payload"
	require.True(t, ok)
	require.Len(t, counters, 1)
	require.Equal(t, counters[0].Class, "foo")
	require.Equal(t, counters[0].Name, "bar")
	require.Equal(t, counters[0].Value, uint64(1))

	m := <-sender.metadata
	require.Equal(t, "installation_id", m.InstallationID)
	require.Len(t, m.Entries, 1)
	require.Equal(t, m.Entries[0].Name, "runtime")
	require.Equal(t, m.Entries[0].Value, "stat")

	select {
	case <-sender.metadata:
		require.Fail(t, "should not send the same metadata runtime stats more than once")
	default:
	}
}

func TestCallHomeCollector_ExtendedHashValues(t *testing.T) {
	sender, ticker, collector := setupTest(0, stats.WithExtended(true))
	ctx, cancelFn := context.WithCancel(context.Background())
	collector.Run(ctx)

	const events = 2
	for i := 0; i < events; i++ {
		collector.CollectEvent(stats.Event{
			Class:      "foo",
			Name:       "bar",
			Repository: "repository1",
			Ref:        "ref1",
			SourceRef:  "source_ref1",
			Client:     "client1",
			UserID:     "user_id1",
		})
	}

	// ensure we flush at the given interval
	ticker.makeItTick()

	counters, ok := <-sender.metrics
	if !ok {
		t.Fatal("Failed to pull counters from sender's metrics")
	}

	diff := deep.Equal(counters, []stats.Metric{
		{
			Event: stats.Event{
				Class:      "foo",
				Name:       "bar",
				Repository: stats.HashMetricValue("repository1"),
				Ref:        stats.HashMetricValue("ref1"),
				SourceRef:  stats.HashMetricValue("source_ref1"),
				UserID:     stats.HashMetricValue("user_id1"),
				Client:     "client1",
			},
			Value: 2,
		},
	})
	if diff != nil {
		t.Fatalf("Metric events should match: %s", diff)
	}

	cancelFn()
	collector.Close()
}

func TestCallHomeCollector_NoExtendedValues(t *testing.T) {
	sender, ticker, collector := setupTest(0, stats.WithExtended(false))
	ctx, cancelFn := context.WithCancel(context.Background())
	collector.Run(ctx)

	const events = 2
	for i := 0; i < events; i++ {
		collector.CollectEvent(stats.Event{
			Class:      "foo",
			Name:       "bar",
			Repository: "repository1",
			Ref:        "ref1",
			SourceRef:  "source_ref1",
			Client:     "client1",
			UserID:     "user_id1",
		})
	}

	// ensure we flush at the given interval
	ticker.makeItTick()

	counters, ok := <-sender.metrics
	if !ok {
		t.Fatal("Failed to pull counters from sender's metrics")
	}

	diff := deep.Equal(counters, []stats.Metric{
		{
			Event: stats.Event{
				Class:      "foo",
				Name:       "bar",
				Repository: "",
				Ref:        "",
				SourceRef:  "",
				UserID:     "",
				Client:     "",
			},
			Value: 2,
		},
	})
	if diff != nil {
		t.Fatalf("Metric events should match: %s", diff)
	}

	cancelFn()
	collector.Close()
}
