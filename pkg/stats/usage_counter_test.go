package stats_test

import (
	"context"
	"math/rand"
	"testing"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	"github.com/treeverse/lakefs/pkg/stats"
)

func TestNewUsageCounter(t *testing.T) {
	const totalCounters = 3

	// keep track on counters
	counters := make([]*stats.UsageCounter, 0, totalCounters)
	defer func() {
		for _, c := range counters {
			c.Unregister()
		}
	}()

	for i := 0; i < totalCounters; i++ {
		c := stats.NewUsageCounter()
		counters = append(counters, c)
	}

	if len(stats.UsageCounters()) != totalCounters {
		t.Fatalf("expected %d counters, got %d", totalCounters, len(stats.UsageCounters()))
	}
}

func TestUsageCounter(t *testing.T) {
	c := stats.NewUsageCounter()
	defer c.Unregister()

	// start with 0
	if n := c.Load(); n != 0 {
		t.Fatalf("expected counter to be 0, got %d", n)
	}

	// test add works
	c.Add(10)
	if n := c.Load(); n != 10 {
		t.Fatalf("expected counter to be 10, got %d", n)
	}

	c.Add(-2)
	if n := c.Load(); n != 8 {
		t.Fatalf("expected counter to be 8, got %d", n)
	}

	// test reset works
	if n := c.Reset(); n != 8 {
		t.Fatalf("expected counter to be 8, got %d", n)
	}
	if n := c.Load(); n != 0 {
		t.Fatalf("expected counter to be 0, got %d", n)
	}
	if n := c.Reset(); n != 0 {
		t.Fatalf("expected counter to be 0, got %d", n)
	}
}

func TestUsageReporter(t *testing.T) {
	// create a store
	ctx := context.Background()
	store, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	installationID := gonanoid.Must(8)
	report := stats.NewUsageReporter(installationID, store)
	if report == nil {
		t.Fatal("expected an instance")
	}

	if report.InstallationID() != installationID {
		t.Fatalf("expected installation id to be '%s', got '%s'", installationID, report.InstallationID())
	}

	c1 := stats.NewUsageCounter()
	defer c1.Unregister()
	c1.Add(5)

	c2 := stats.NewUsageCounter()
	defer c2.Unregister()
	c2.Add(10)

	total := c1.Load() + c2.Load()

	const rounds = 3 // test multiple rounds, each time incrementing the counters
	for i := 0; i < rounds; i++ {
		flushTime, err := report.Flush(ctx)
		if err != nil {
			t.Fatal("Flush() expected no error")
		}

		records, err := report.Records(ctx)
		if err != nil {
			t.Fatal("Records() expected no error")
		}
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}
		record := records[0]

		// verify record includes year, month and count
		year, month, _ := flushTime.Date()
		if record.Month != int(month) {
			t.Fatalf("expected month to be %d, got %d", month, record.Month)
		}
		if record.Year != year {
			t.Fatalf("expected year to be %d, got %d", year, record.Year)
		}
		if record.Count != total {
			t.Fatalf("expected count to be %d, got %d", total, record.Count)
		}

		newUsage := rand.Int63n(50) // add a random number to each counter
		c1.Add(newUsage)
		total += newUsage
	}
}

func TestDefaultUsageReporter(t *testing.T) {
	reporter := stats.DefaultUsageReporter
	if reporter == nil {
		t.Fatal("expected an instance")
	}

	if reporter.InstallationID() != "" {
		t.Fatalf("expected installation id to be empty, got '%s'", reporter.InstallationID())
	}

	ctx := context.Background()
	tm, err := reporter.Flush(ctx)
	if err != nil {
		t.Fatal("Flush() expected no error")
	}
	if !tm.IsZero() {
		t.Fatalf("expected time to be zero, got %s", tm)
	}

	records, err := reporter.Records(ctx)
	if err != nil {
		t.Fatal("Records() expected no error")
	}
	if len(records) != 0 {
		t.Fatalf("expected no records, got %d", len(records))
	}
}
