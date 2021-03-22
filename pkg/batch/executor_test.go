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

const accessedOnce = 1
const accessedTwice = 2

type reader struct {
	Ch         chan bool
	IsExecuted bool
	Fn         ExecFn
}

type ExecFn func() (interface{}, error)

func (r *reader) Execute() (interface{}, error) {
	r.IsExecuted = true
	return r.Fn()
}

func (r *reader) Batched() {
	close(r.Ch)
}

type db struct {
	KvStore     sync.Map
	AccessCount int
}

type Dao interface {
	Insert(key string, val string)
	Get(key string) string
	GetCount() int
}

func (d *db) Insert(key string, val string) {
	d.KvStore.Store(key, val)
}

func (d *db) Get(key string) (interface{}, bool) {
	d.AccessCount++
	return d.KvStore.Load(key)
}

func (d *db) GetAccessCount() int {
	return d.AccessCount
}

func testReadAfterWrite(t *testing.T) {
	// Setup executor
	exec := batch.NewExecutor(logging.Default())
	go exec.Run(context.Background())
	// Prove the executor does not violate read-after-write consistency.
	// First, let's define read-after-write consistency:
	// 	Any read that started after a successful write has returned, must return the updated value.
	// To test this, let's simulate the following scenario:
	// 1. reader (r1) starts (Current version: v0)
	// 2. writer (w1) writes v1
	// 3. writer (w1) returns (Current version: v1)
	// 4. reader (r2) starts
	// 5. both readers (r1,r2) return with v1 as their response.
	var db = &db{sync.Map{}, 0}
	db.Insert("v", "v0")

	read1Done := make(chan bool)
	write1Done := make(chan bool)
	read2Done := make(chan bool)
	read2Batched := make(chan bool)

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
		r1, _ := exec.BatchFor("k", time.Millisecond*50, batch.BatchFn(func() (interface{}, error) {
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
		r := reader{Ch: read2Batched, Fn: func() (interface{}, error) {
			return nil, nil
		}}
		_, _ = exec.BatchFor("k", 50*time.Millisecond, &r)
		// We expect r2's exec function to no execute because it should join r1's batch
		if r.IsExecuted || db.GetAccessCount() != accessedOnce {
			t.Error("r2's exec function should not be called, only r1's")
		}
		close(read2Done)
	}()

	<-read1Done
	<-read2Done
}

func testBatchExpiration(t *testing.T) {
	// Setup executor
	exec := batch.NewExecutor(logging.Default())
	go exec.Run(context.Background())

	// Confirm that batches expire after a delay period, and hence requests don't hang
	// To test this, let's simulate the following scenario:
	// 1. reader (r1) makes request with key 'k'
	// 2. reader 1 returns
	// 3. reader (r2) makes request with key 'k'
	// 4. a new batch is used to making r2's db request
	var db = &db{sync.Map{}, 0}

	read1Done := make(chan bool)
	read2Done := make(chan bool)

	// reader1 starts
	go func() {
		exec.BatchFor("k", time.Millisecond*50, batch.BatchFn(func() (interface{}, error) {
			version, _ := db.Get("v")
			return version, nil
		}))
		ac := db.GetAccessCount()
		if ac != accessedOnce {
			t.Errorf("r1 should have triggered a single db call, but access count= #{ac}")
		}
		close(read1Done)

	}()

	go func() {
		<-read1Done // ensure r2 starts after r1 has returned
		exec.BatchFor("k", time.Millisecond*50, batch.BatchFn(func() (interface{}, error) {
			version, _ := db.Get("v")
			return version, nil
		}))
		ac := db.GetAccessCount()
		if ac != accessedTwice {
			t.Errorf("r2 should have triggered the second db call, but access count= #{ac}")
		}
		close(read2Done)
	}()

	<-read2Done
}

func testBatchByKey(t *testing.T) {
	// Setup executor
	exec := batch.NewExecutor(logging.Default())
	go exec.Run(context.Background())

	// Confirm that requests are batched by key.
	// To test this, let's simulate the following scenario:
	// 1. reader (r1) makes request with key 'k1'
	// 2. reader (r2) makes request with key 'k2' before r1's batch has been expired
	// 3. Two batches are created and two requests are executed
	read1Done := make(chan bool)
	read2Done := make(chan bool)
	req2Done := make(chan bool)

	// we pass a custom delay func that ensures r2 starts only after r1 started, and that r1 executes only
	// after r2 made a request
	waitRead2 := make(chan bool)
	delays := int32(0)
	delayFn := func(dur time.Duration) {
		delaysDone := atomic.AddInt32(&delays, 1)
		if delaysDone == 1 {
			close(waitRead2)
			<-req2Done
		}
	}
	exec.Delay = delayFn

	r1 := reader{Fn: func() (interface{}, error) {
		return nil, nil
	}}
	r2 := reader{Fn: func() (interface{}, error) {
		close(req2Done)
		return nil, nil
	}}

	// reader1 starts
	go func(r *reader) {
		_, _ = exec.BatchFor("k1", 50*time.Millisecond, r)
		close(read1Done)
	}(&r1)

	go func(r *reader) {
		<-waitRead2 // ensure we start AFTER r2 started
		//close(read2Started)
		exec.BatchFor("k2", 0, r)
		close(read2Done)
	}(&r2)

	<-read1Done
	<-read2Done

	r1Succeeded := r1.IsExecuted
	r2Succeeded := r2.IsExecuted
	if !(r1Succeeded && r2Succeeded) {
		t.Error("both r1 and r2's exec functions should be executed but r1Succeeded=#{r1Succeeded} " +
			"and r2Succeeded=#{r1Succeeded}")
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
