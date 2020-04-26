package stats_test

import (
	"testing"

	"github.com/treeverse/lakefs/stats"
)

func TestCallHomeCollector_Collect(t *testing.T) {
	collector := stats.NewCallHomeCollector(stats.WriteBufferSize(0))
	go collector.Run()

	// add metrics
	collector.Collect("foo", "bar")
	collector.Collect("foo", "bar")
	collector.Collect("foo", "bar")
	collector.Collect("foo", "bazzz")
	collector.Collect("foo", "bazzz")
	collector.Collect("other", "bar")

	// ensure we flush at the given interval
	counters := collector.Drain()
	keys := 0
	for k, v := range counters {
		if k.String() == "foo/bar" {
			keys++
			if v != 3 {
				t.Fatalf("expected count %d for foo/bar, got %d", 3, v)
			}
		}
		if k.String() == "foo/bazzz" {
			keys++
			if v != 2 {
				t.Fatalf("expected count %d for foo/bazzz, got %d", 2, v)
			}
		}
		if k.String() == "other/bar" {
			keys++
			if v != 1 {
				t.Fatalf("expected count %d for other/bar, got %d", 1, v)
			}
		}
	}
	if keys != 3 {
		t.Fatalf("expected all %d keys, got %d", 3, keys)
	}
}

func TestCallHomeCollector_Flush(t *testing.T) {
	collector := stats.NewCallHomeCollector(stats.WriteBufferSize(0))
	go collector.Run()

	// add metrics
	collector.Collect("foo", "bar")
	collector.Collect("foo", "bar")
	collector.Collect("foo", "bar")
	collector.Collect("foo", "bazzz")
	collector.Collect("foo", "bazzz")
	collector.Collect("other", "bar")

	// ensure we flush at the given interval
	counters := collector.Flush()
	keys := 0
	for k, v := range counters {
		if k.String() == "foo/bar" {
			keys++
			if v != 3 {
				t.Fatalf("expected count %d for foo/bar, got %d", 3, v)
			}
		}
		if k.String() == "foo/bazzz" {
			keys++
			if v != 2 {
				t.Fatalf("expected count %d for foo/bazzz, got %d", 2, v)
			}
		}
		if k.String() == "other/bar" {
			keys++
			if v != 1 {
				t.Fatalf("expected count %d for other/bar, got %d", 1, v)
			}
		}
	}
	if keys != 3 {
		t.Fatalf("expected all %d keys, got %d", 3, keys)
	}

	counters = collector.Drain()
	if len(counters) > 0 {
		t.Fatalf("expected %d keys left to drain, got %d", 0, len(counters))
	}
}
