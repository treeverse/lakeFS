package stats_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/treeverse/lakefs/stats"
)

type mockSender struct {
	mx        *sync.RWMutex
	lastEvent *stats.InputEvent
}

func (m *mockSender) Send(event stats.InputEvent) error {
	m.mx.Lock()
	m.lastEvent = &event
	m.mx.Unlock()
	return nil
}

func (m *mockSender) LastEvent() *stats.InputEvent {
	m.mx.RLock()
	defer m.mx.RUnlock()
	return m.lastEvent
}

func TestDispatcher_Collect(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	m := &mockSender{mx: &sync.RWMutex{}}
	d := stats.NewDispatcher(ctx, time.Millisecond*10, m, "procid", "userid")

	done := make(chan bool)
	go d.Run(done)

	d.Collect("foo", "bar")
	d.Collect("foo", "bar")
	d.Collect("foo", "bar")

	if m.LastEvent() != nil {
		t.Fatalf("didnt expect a send to happen yet")
	}

	time.Sleep(time.Millisecond * 100)

	if m.LastEvent() == nil {
		t.Fatalf("expected a send to happen")
	}

	if len(m.LastEvent().Metrics) != 1 {
		t.Fatalf("expected 1 metric in aggregate, got %d", len(m.LastEvent().Metrics))
	}

	if m.LastEvent().Metrics[0].Name != "bar" {
		t.Fatalf("expected metric name bar got %s", m.LastEvent().Metrics[0].Name)
	}

	if m.LastEvent().Metrics[0].Class != "foo" {
		t.Fatalf("expected metric class foo got %s", m.LastEvent().Metrics[0].Class)
	}
	if m.LastEvent().Metrics[0].Value != 3 {
		t.Fatalf("expected metric value 3 got %d", m.LastEvent().Metrics[0].Value)
	}

	cancelFn()
	<-done
}
