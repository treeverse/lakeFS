package testutil

import (
	"sync"
	"time"
)

type Request struct {
	Payload string
}

type Result struct {
	Error error
	Took  time.Duration
}

type WorkFn func(input chan Request, output chan Result)

type WorkerPool struct {
	parallelism int
	Input       chan Request
	Output      chan Result
	WorkFn      WorkFn

	wg   sync.WaitGroup
	done chan struct{}
}

func NewWorkerPool(parallelism int, workFn WorkFn) *WorkerPool {
	return &WorkerPool{
		parallelism: parallelism,
		WorkFn:      workFn,
		Input:       make(chan Request),
		Output:      make(chan Result),
		done:        make(chan struct{}),
	}
}

func (p *WorkerPool) Start() {
	// spawn workers
	p.wg.Add(p.parallelism)
	for i := 0; i < p.parallelism; i++ {
		go func() {
			p.WorkFn(p.Input, p.Output) // call the worker we were given
			p.wg.Done()
		}()
	}
	go func() {
		p.wg.Wait()
		p.done <- struct{}{}
	}()
}

func (p *WorkerPool) Done() chan struct{} {
	return p.done
}
