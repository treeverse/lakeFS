package stats_test

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

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

func TestCallHomeCollector_Collect(t *testing.T) {
	sender := &mockSender{metrics: make(chan []stats.Metric, 10), metadata: make(chan stats.Metadata, 10)}
	ticker := &mockTicker{tc: make(chan time.Time)}
	ctx, cancelFn := context.WithCancel(context.Background())
	collector := stats.NewBufferedCollector("installation_id",
		func() map[string]string {
			return map[string]string{"runtime": "stat"}
		}, nil,
		stats.WithSender(sender),
		stats.WithTicker(ticker),
		stats.WithWriteBufferSize(0))
	go collector.Run(ctx)

	// add metrics
	collector.CollectEvent("foo", "bar")
	collector.CollectEvent("foo", "bar")
	collector.CollectEvent("foo", "bar")
	collector.CollectEvent("foo", "bazzz")
	collector.CollectEvent("foo", "bazzz")
	collector.CollectEvent("other", "bar")

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

	collector.CollectEvent("foo", "bar")

	cancelFn()
	<-collector.Done()
	<-sender.metrics // ensure we get another "payload"

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
