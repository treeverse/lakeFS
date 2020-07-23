package dedup

import (
	"sync"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
)

const (
	dedupRemoveWorkers     = 1
	dedupRemoveChannelSize = 1000
	dedupRemoveWait        = 5 * time.Second
)

type dedupRemoveRequest struct {
	Timestamp time.Time
	Object    block.ObjectPointer
}

type Cleaner struct {
	block    block.Adapter
	ch       chan *catalog.DedupReport
	removeCh chan dedupRemoveRequest
	wg       sync.WaitGroup
}

// NewCleaner handles the delete of objects from block after dedup identified and updated by the cataloger
func NewCleaner(adapter block.Adapter, ch chan *catalog.DedupReport) *Cleaner {
	c := &Cleaner{
		block:    adapter,
		ch:       ch,
		removeCh: make(chan dedupRemoveRequest, dedupRemoveChannelSize),
	}
	c.startDedupRemove()
	return c
}

func (d *Cleaner) Close() error {
	close(d.removeCh)
	d.wg.Wait()
	return nil
}

func (d *Cleaner) startDedupRemove() {
	d.wg.Add(dedupRemoveWorkers)
	for i := 0; i < dedupRemoveWorkers; i++ {
		go func() {
			defer d.wg.Done()
			for req := range d.ch {
				// wait before before we delete the object
				timeDiff := req.Timestamp.Add(dedupRemoveWait).Sub(time.Now())
				time.Sleep(timeDiff)

				// delete the object
				obj := block.ObjectPointer{
					StorageNamespace: req.StorageNamespace,
					Identifier:       req.Entry.PhysicalAddress,
				}
				err := d.block.Remove(obj)
				if err != nil {
					dedupRemoveObjectFailedCounter.Inc()
				}
			}
		}()
	}
}
