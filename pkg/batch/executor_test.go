package batch_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestExecutor_BatchFor(t *testing.T) {
	// Setup executor
	exec := batch.NewExecutor(logging.Dummy())
	go exec.Run(context.Background())
	// Prove the executor does not violate read-after-write consistency.
	// First, let's define read-after-write consistency:
	// 	Any read that started after a successful write has returned, must return the updated value.
	// to test this, let's simulate the following scenario:
	// 1. reader (r1) starts (Current version: v0)
	// 2. writer (w1) writes v1
	// 3. writer (w1) returns (Current version: v1)
	// 4. reader (r2) starts
	// 5. both readers (r1,r2) return with v1 as their response.
	var db = sync.Map{}
	db.Store("v", "v0")

	read1Done := make(chan bool)
	write1Done := make(chan bool)
	read2Done := make(chan bool)

	// we pass a custom delay func that ensures we make the write only after
	//  reader1 started
	waitWrite := make(chan bool)
	delays := int32(0)
	delayFn := func(dur time.Duration) {
		delaysDone := atomic.AddInt32(&delays, 1)
		if delaysDone == 1 {
			close(waitWrite)
		}
		time.Sleep(dur)
	}
	exec.Delayer = delayFn

	// reader1 starts
	go func() {
		r1, _ := exec.BatchFor("k", time.Millisecond*50, func() (interface{}, error) {
			version, _ := db.Load("v")
			return version, nil
		})
		r1v := r1.(string)
		if r1v != "v1" {
			// reader1, while it could have returned either v0 or v1 without violating read-after-write conisistency,
			// is expected to return v1 with this batching logic
			t.Fatalf("expected r1 to get v1, got %s instead", r1v)
		}
		close(read1Done)
	}()

	// Writer1 writes
	go func() {
		<-waitWrite
		db.Store("v", "v1")
		close(write1Done)
	}()

	// following that write, another reader starts, and must read the updated value
	go func() {
		<-write1Done // ensure we start AFTER write1 has completed
		r2, _ := exec.BatchFor("k", time.Millisecond*50, func() (interface{}, error) {
			version, _ := db.Load("v")
			return version, nil
		})
		r2v := r2.(string)
		if r2v != "v1" {
			t.Fatalf("expected r2 to get v1, got %s instead", r2v)
		}
		close(read2Done)
	}()

	<-read1Done
	<-read2Done
}
