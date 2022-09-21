package catalog

import "sync"

type sharedWorkpool struct {
	incoming chan func()
	done     chan struct{}
	wg       *sync.WaitGroup
}

const workersToBufferRatio = 3

func newSharedWorkpool(workers int) *sharedWorkpool {
	incoming := make(chan func(), workers*workersToBufferRatio)
	done := make(chan struct{})
	wg := &sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case f := <-incoming:
					f()
				}
			}
		}()
	}

	return &sharedWorkpool{
		incoming: incoming,
		done:     done,
		wg:       wg,
	}
}

func (sw *sharedWorkpool) Close() error {
	close(sw.done)
	sw.wg.Wait()
	return nil
}
