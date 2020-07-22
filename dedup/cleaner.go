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
	dedupCheckChannelSize  = 5000
	dedupRemoveWait        = 5 * time.Second
)

type dedupRemoveRequest struct {
	Timestamp time.Time
	Object    block.ObjectPointer
}

type Cleaner struct {
	block    block.Adapter
	checkCh  chan *catalog.DedupResult
	removeCh chan dedupRemoveRequest
	wg       sync.WaitGroup
}

// NewDedupCleaner handles the delete of objects from block after dedup identified and updated by the cataloger
func NewDedupCleaner(adapter block.Adapter) *Cleaner {
	return &Cleaner{
		block:    adapter,
		checkCh:  make(chan *catalog.DedupResult, dedupCheckChannelSize),
		removeCh: make(chan dedupRemoveRequest, dedupRemoveChannelSize),
	}
}

func (d *Cleaner) Close() error {
	close(d.checkCh)
	close(d.removeCh)
	d.wg.Wait()
	return nil
}

func (d *Cleaner) Channel() chan *catalog.DedupResult {
	return d.checkCh
}

func (d *Cleaner) Start() {
	d.startDedupCheck()
	d.startDedupRemove()
}

func (d *Cleaner) startDedupRemove() {
	d.wg.Add(dedupRemoveWorkers)
	for i := 0; i < dedupRemoveWorkers; i++ {
		go func() {
			defer d.wg.Done()
			for req := range d.removeCh {
				// wait before you delete the object
				timeDiff := time.Since(req.Timestamp.Add(dedupRemoveWait))
				time.Sleep(timeDiff)
				// delete the object
				err := d.block.Remove(req.Object)
				if err != nil {
					dedupRemoveObjectFailedCounter.Inc()
				}
			}
		}()
	}
}

func (d *Cleaner) startDedupCheck() {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for dd := range d.checkCh {
			// if we have new physical address we can remove the entry address
			if dd.NewPhysicalAddress == "" {
				continue
			}
			// send request to delete the previous address
			select {
			case d.removeCh <- dedupRemoveRequest{
				Timestamp: time.Now(),
				Object: block.ObjectPointer{
					StorageNamespace: dd.StorageNamespace,
					Identifier:       dd.Entry.PhysicalAddress,
				},
			}:
			default:
				dedupRemoveObjectDroppedCounter.Inc()
			}
		}
	}()
}
