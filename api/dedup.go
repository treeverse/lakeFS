package api

import (
	"sync"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
)

const (
	dedupWorkers     = 1
	dedupChannelSize = 1000
)

type DedupHandler struct {
	block block.Adapter
	ch    chan *catalog.DedupResult
	wg    sync.WaitGroup
}

func NewDedupHandler(adapter block.Adapter) *DedupHandler {
	return &DedupHandler{
		block: adapter,
		ch:    make(chan *catalog.DedupResult, dedupChannelSize),
	}
}

func (d *DedupHandler) Close() error {
	close(d.ch)
	d.wg.Wait()
	return nil
}

func (d *DedupHandler) Channel() chan *catalog.DedupResult {
	return d.ch
}

func (d *DedupHandler) Start() {
	d.wg.Add(dedupWorkers)
	for i := 0; i < dedupWorkers; i++ {
		go func() {
			defer d.wg.Done()
			for dd := range d.ch {
				// for dedup with new physical address, delete the old
				if dd.NewPhysicalAddress == "" {
					continue
				}
				_ = d.block.Remove(block.ObjectPointer{
					StorageNamespace: dd.StorageNamespace,
					Identifier:       dd.Entry.PhysicalAddress,
				})
			}
		}()
	}
}
