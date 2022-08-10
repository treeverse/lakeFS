package stress

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"
)

type Result struct {
	Error error
	Took  time.Duration
}

type WorkFn func(input chan string, output chan Result)

type (
	GeneratorAddFn func(string)
	GenerateFn     func(add GeneratorAddFn)
)

type WorkerPool struct {
	parallelism int
	Input       chan string
	Output      chan Result
	wg          sync.WaitGroup
	done        chan struct{}
}

func NewWorkerPool(parallelism int) *WorkerPool {
	return &WorkerPool{
		parallelism: parallelism,
		Input:       make(chan string),
		Output:      make(chan Result),
		done:        make(chan struct{}),
	}
}

func (p *WorkerPool) Start(workFn WorkFn) {
	// spawn workers
	p.wg.Add(p.parallelism)

	// use wait-group and a channel as barrier for calling the worker from all goroutines
	var startWG sync.WaitGroup
	startWG.Add(p.parallelism)
	startCh := make(chan struct{})

	for i := 0; i < p.parallelism; i++ {
		go func() {
			defer p.wg.Done()
			startWG.Done()
			<-startCh
			workFn(p.Input, p.Output) // call the worker we were given
		}()
	}
	startWG.Wait()
	close(startCh)

	go func() {
		p.wg.Wait()
		p.done <- struct{}{}
	}()
}

func (p *WorkerPool) Done() chan struct{} {
	return p.done
}

// Generator sets up a pool and a result collector
type Generator struct {
	name          string
	pool          *WorkerPool
	collector     *ResultCollector
	handleSignals []os.Signal
}

type GeneratorOption func(*Generator)

func WithSignalHandlersFor(sigs ...os.Signal) GeneratorOption {
	return func(generator *Generator) {
		generator.handleSignals = sigs
	}
}

func NewGenerator(name string, parallelism int, opts ...GeneratorOption) *Generator {
	pool := NewWorkerPool(parallelism)
	collector := NewResultCollector(pool.Output)
	g := &Generator{
		name:          name,
		pool:          pool,
		collector:     collector,
		handleSignals: []os.Signal{},
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

func (g *Generator) addResult(s string) {
	g.pool.Input <- s
}

func (g *Generator) Setup(fn GenerateFn) {
	go func() {
		fn(g.addResult)
		close(g.pool.Input)
	}()
}

// Run will start the worker goroutines and print out their
// progress every second. Upon completion (or on a SIGTERM), will also print a latency histogram
func (g *Generator) Run(fn WorkFn) {
	go g.collector.Collect()
	g.pool.Start(fn)

	termSignal := make(chan os.Signal, 1)
	if len(g.handleSignals) > 0 {
		signal.Notify(termSignal, g.handleSignals...)
	}

	collecting := true
	ticker := time.NewTicker(time.Second)
	for collecting {
		select {
		case <-ticker.C:
			fmt.Printf("%s - %s\n", g.name, g.collector.Stats())
		case <-g.pool.Done():
			collecting = false
		case <-termSignal:
			collecting = false
		}
	}
	fmt.Printf("%s\n\n", g.collector.Stats())
	fmt.Printf("Histogram (ms):\n%s\n", g.collector.Histogram())
}
