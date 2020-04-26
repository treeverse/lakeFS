package stats_test

import (
	"context"
	"testing"
	"time"

	"github.com/treeverse/lakefs/stats"
)

type mockSender struct {
	metrics chan []stats.Metric
}

func (s *mockSender) Send(m []stats.Metric) error {
	s.metrics <- m
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
	sender := &mockSender{metrics: make(chan []stats.Metric, 1)}
	ticker := &mockTicker{tc: make(chan time.Time)}
	ctx, cancelFn := context.WithCancel(context.Background())
	collector := stats.NewBufferedCollector(stats.WithSender(sender), stats.WithTicker(ticker), stats.WithWriteBufferSize(0))
	go collector.Run(ctx)

	// add metrics
	collector.Collect("foo", "bar")
	collector.Collect("foo", "bar")
	collector.Collect("foo", "bar")
	collector.Collect("foo", "bazzz")
	collector.Collect("foo", "bazzz")
	collector.Collect("other", "bar")

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

	collector.Collect("foo", "bar")

	cancelFn()
	<-collector.Done()
	<-sender.metrics // ensure we get another "payload"
}
