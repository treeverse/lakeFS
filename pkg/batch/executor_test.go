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

type trackableExecuter struct {
	batchTracker chan struct{}
	execTracker  chan struct{}
}

func (te *trackableExecuter) WasExecuted() bool {
	chanOpen := true
	select {
	case _, chanOpen = <-te.execTracker:
	default:
	}
	return !chanOpen
}

func (te *trackableExecuter) Execute() (interface{}, error) {
	close(te.execTracker)
	return nil, nil
}

func (te *trackableExecuter) Batched() {
	close(te.batchTracker)
}

type db struct {
	kvStore     sync.Map
	accessCount int32
}

func (d *db) Insert(key string, val string) {
	d.kvStore.Store(key, val)
}

func (d *db) Get(key string) (interface{}, bool) {
	atomic.AddInt32(&d.accessCount, 1)
	return d.kvStore.Load(key)
}

func (d *db) GetAccessCount() int32 {
	return d.accessCount
}

func testReadAfterWrite(t *testing.T) {
	// Setup executor
	exec := batch.NewExecutor(logging.Default())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go exec.Run(ctx)

	// Prove the executor does not violate read-after-write consistency.
	// First, let's define read-after-write consistency:
	// 	Any read that started after a successful write has returned, must return the updated value.
	// To test this, let's simulate the following scenario:
	// 1. reader (r1) starts (Current version: v0)
	// 2. writer (w1) writes v1
	// 3. writer (w1) returns (Current version: v1)
	// 4. reader (r2) starts
	// 5. reader (r1) returns
	// 6. reader (r2) returns
	// 7. both readers (r1,r2) return with v1 as their response.
	db := &db{}
	db.Insert("v", "v0")

	read1Done := make(chan bool)
	write1Done := make(chan bool)
	read2Done := make(chan bool)
	read2Batched := make(chan struct{})

	// we pass a custom delay func that ensures we make the write only after
	//  reader1 started
	waitWrite := make(chan bool)
	delays := int32(0)
	delayFn := func(dur time.Duration) {
		delaysDone := atomic.AddInt32(&delays, 1)
		if delaysDone == 1 {
			close(waitWrite)
		}
		// We want to make sure that r2 is in the same batch with r1
		<-read2Batched
	}
	exec.Delay = delayFn

	// reader1 starts
	go func() {
		r1, _ := exec.BatchFor(context.Background(), "k", time.Millisecond*50, batch.BatchFn(func() (interface{}, error) {
			version, _ := db.Get("v")
			return version, nil
		}))
		r1v := r1.(string)
		if r1v != "v1" {
			// reader1, while it could have returned either v0 or v1 without violating read-after-write consistency,
			// is expected to return v1 with this batching logic
			t.Errorf("expected r1 to get v1, got %s instead", r1v)
		}
		close(read1Done)
	}()

	// Writer1 writes
	go func() {
		<-waitWrite
		db.Insert("v", "v1")
		close(write1Done)
	}()

	// following that write, another reader starts, and must read the updated value
	go func() {
		<-write1Done // ensure we start AFTER write1 has completed
		te := trackableExecuter{batchTracker: read2Batched, execTracker: make(chan struct{})}
		r2, _ := exec.BatchFor(context.Background(), "k", 50*time.Millisecond, &te)
		r2v := r2.(string)
		if r2v != "v1" {
			t.Errorf("expected r2 to get v1, got %s instead", r2v)
		}

		// We expect r2's exec function to not execute because it should join r1's batch
		if te.WasExecuted() {
			t.Errorf("r2's exec function should not be called, only r1's")
		}

		if accessCount := db.GetAccessCount(); accessCount != 1 {
			t.Errorf("db should only be accessed once, but was accessed %d times", accessCount)
		}
		close(read2Done)
	}()

	<-read1Done
	<-read2Done
}

func testBatchExpiration(t *testing.T) {
	// Setup executor
	exec := batch.NewExecutor(logging.Default())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go exec.Run(ctx)

	// Confirm that batches expire after a delay period, and hence requests don't hang
	// To test this, let's simulate the following scenario:
	// 1. reader (r1) makes request with key 'k'
	// 2. reader 1 returns v1
	// 3. reader (r2) makes request with key 'k'
	// 4. a new batch is used to making r2's request and the returned value is v2
	read1Done := make(chan bool)
	read2Done := make(chan bool)

	// reader1 starts
	go func() {
		r1, _ := exec.BatchFor(context.Background(), "k", time.Millisecond*50, batch.BatchFn(func() (interface{}, error) {
			return "v1", nil
		}))
		if r1 != "v1" {
			t.Errorf("expected r1 to get v1 but got %s instead", r1)
		}
		close(read1Done)
	}()

	go func() {
		<-read1Done // ensure r2 starts after r1 has returned
		r2, _ := exec.BatchFor(context.Background(), "k", time.Millisecond*50, batch.BatchFn(func() (interface{}, error) {
			return "v2", nil
		}))
		if r2 != "v2" {
			t.Errorf("expected r2 to get v2 but got %s instead", r2)
		}
		close(read2Done)
	}()

	<-read2Done
}

func testBatchByKey(t *testing.T) {
	// Setup executor
	exec := batch.NewExecutor(logging.Default())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go exec.Run(ctx)

	// Confirm that requests are batched by key.
	// To test this, let's simulate the following scenario:
	// 1. reader (r1) makes a request with key 'k1'
	// 2. reader (r2) makes a request with key 'k2' before te1's batch has been expired
	// 3. Two batches are created and two requests are executed
	read1Done := make(chan bool)
	read2Done := make(chan bool)

	// we pass a custom delay func that ensures r2 starts only after r1 started, and that r1 executes only
	// after r2 made a request
	waitRead2 := make(chan bool)
	delays := int32(0)
	delayFn := func(dur time.Duration) {
		delaysDone := atomic.AddInt32(&delays, 1)
		if delaysDone == 1 {
			close(waitRead2)
			<-read2Done
		}
	}
	exec.Delay = delayFn

	te1 := trackableExecuter{execTracker: make(chan struct{})}
	te2 := trackableExecuter{execTracker: make(chan struct{})}

	// reader1 starts
	go func(te *trackableExecuter) {
		_, _ = exec.BatchFor(context.Background(), "k1", 50*time.Millisecond, te)
		close(read1Done)
	}(&te1)

	// reader2 starts
	go func(te *trackableExecuter) {
		<-waitRead2 // ensure we start AFTER r1 started a new batch
		_, err := exec.BatchFor(context.Background(), "k2", 0, te)
		if err != nil {
			t.Errorf("BatchFor error: %s", err)
		}
		close(read2Done)
	}(&te2)

	<-read1Done

	r1Succeeded := te1.WasExecuted()
	r2Succeeded := te2.WasExecuted()
	if !(r1Succeeded && r2Succeeded) {
		t.Errorf("both r1's and r2's exec functions should be executed but r1Succeeded=%t and r2Succeeded=%t", r1Succeeded, r2Succeeded)
	}
}

func TestExecutor_BatchFor(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			defer wg.Done()
			testReadAfterWrite(t)
			testBatchByKey(t)
			testBatchExpiration(t)
		}()
	}
	wg.Wait()
}
