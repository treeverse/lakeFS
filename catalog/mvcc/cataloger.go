package mvcc

import (
	"context"
	"sync"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/mvcc/params"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

type dedupRequest struct {
	Repository       string
	StorageNamespace string
	DedupID          string
	Entry            *catalog.Entry
	EntryCTID        string
}

// cataloger main catalog implementation based on mvcc
type cataloger struct {
	params.Catalog
	log                  logging.Logger
	db                   db.Database
	wg                   sync.WaitGroup
	cache                Cache
	dedupCh              chan *dedupRequest
	dedupReportEnabled   bool
	dedupReportCh        chan *catalog.DedupReport
	readEntryRequestChan chan *readRequest
	hooks                catalog.CatalogerHooks
}

const (
	dedupBatchSize         = 10
	dedupBatchTimeout      = 50 * time.Millisecond
	dedupChannelSize       = 5000
	dedupReportChannelSize = 5000

	defaultCatalogerCacheSize   = 1024
	defaultCatalogerCacheExpiry = 20 * time.Second
	defaultCatalogerCacheJitter = 5 * time.Second
	MaxReadQueue                = 10

	defaultBatchReadEntryMaxWait  = 15 * time.Second
	defaultBatchScanTimeout       = 500 * time.Microsecond
	defaultBatchDelay             = 1000 * time.Microsecond
	defaultBatchEntriesReadAtOnce = 64
	defaultBatchReaders           = 8

	defaultBatchWriteEntriesInsertSize = 10
)

type CatalogerOption func(*cataloger)

func WithCacheEnabled(b bool) CatalogerOption {
	return func(c *cataloger) {
		c.Cache.Enabled = b
	}
}

func WithDedupReportChannel(b bool) CatalogerOption {
	return func(c *cataloger) {
		c.dedupReportEnabled = b
	}
}

func WithParams(p params.Catalog) CatalogerOption {
	return func(c *cataloger) {
		if p.BatchRead.ScanTimeout != 0 {
			c.BatchRead.ScanTimeout = p.BatchRead.ScanTimeout
		}
		if p.BatchRead.Delay != 0 {
			c.BatchRead.Delay = p.BatchRead.Delay
		}
		if p.BatchRead.EntriesAtOnce != 0 {
			c.BatchRead.EntriesAtOnce = p.BatchRead.EntriesAtOnce
		}
		if p.BatchRead.EntryMaxWait != 0 {
			c.BatchRead.EntryMaxWait = p.BatchRead.EntryMaxWait
		}
		if p.BatchRead.Readers != 0 {
			c.BatchRead.Readers = p.BatchRead.Readers
		}
		if p.BatchWrite.EntriesInsertSize != 0 {
			c.BatchWrite.EntriesInsertSize = p.BatchWrite.EntriesInsertSize
		}
		if p.Cache.Size != 0 {
			c.Cache.Size = p.Cache.Size
		}
		if p.Cache.Expiry != 0 {
			c.Cache.Expiry = p.Cache.Expiry
		}
		if p.Cache.Jitter != 0 {
			c.Cache.Jitter = p.Cache.Jitter
		}
		c.Cache.Enabled = p.Cache.Enabled
	}
}

func NewCataloger(db db.Database, options ...CatalogerOption) catalog.Cataloger {
	c := &cataloger{
		log:                logging.Default().WithField("service_name", "cataloger"),
		db:                 db,
		dedupCh:            make(chan *dedupRequest, dedupChannelSize),
		dedupReportEnabled: true,
		Catalog: params.Catalog{
			BatchRead: params.BatchRead{
				EntryMaxWait:  defaultBatchReadEntryMaxWait,
				ScanTimeout:   defaultBatchScanTimeout,
				Delay:         defaultBatchDelay,
				EntriesAtOnce: defaultBatchEntriesReadAtOnce,
				Readers:       defaultBatchReaders,
			},
			BatchWrite: params.BatchWrite{
				EntriesInsertSize: defaultBatchWriteEntriesInsertSize,
			},
			Cache: params.Cache{
				Enabled: false,
				Size:    defaultCatalogerCacheSize,
				Expiry:  defaultCatalogerCacheExpiry,
				Jitter:  defaultCatalogerCacheJitter,
			},
		},
	}
	for _, opt := range options {
		opt(c)
	}
	if c.Cache.Enabled {
		c.cache = NewLRUCache(c.Cache.Size, c.Cache.Expiry, c.Cache.Jitter)
	} else {
		c.cache = &DummyCache{}
	}
	if c.dedupReportEnabled {
		c.dedupReportCh = make(chan *catalog.DedupReport, dedupReportChannelSize)
	}
	c.processDedupBatches()
	c.startReadOrchestrator()
	return c
}

func (c *cataloger) startReadOrchestrator() {
	c.readEntryRequestChan = make(chan *readRequest, MaxReadQueue)
	c.wg.Add(1)
	go c.readEntriesBatchOrchestrator()
}

func (c *cataloger) txOpts(ctx context.Context, opts ...db.TxOpt) []db.TxOpt {
	o := []db.TxOpt{
		db.WithContext(ctx),
		db.WithLogger(c.log),
	}
	return append(o, opts...)
}

func (c *cataloger) Close() error {
	if c != nil {
		close(c.dedupCh)
		close(c.readEntryRequestChan)
		c.wg.Wait()
		close(c.dedupReportCh)
	}
	return nil
}

func (c *cataloger) DedupReportChannel() chan *catalog.DedupReport {
	return c.dedupReportCh
}

func (c *cataloger) processDedupBatches() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		batch := make([]*dedupRequest, 0, dedupBatchSize)
		timer := time.NewTimer(dedupBatchTimeout)
		for {
			processBatch := false
			select {
			case req, ok := <-c.dedupCh:
				if !ok {
					return
				}
				batch = append(batch, req)
				l := len(batch)
				if l == 1 {
					timer.Reset(dedupBatchTimeout)
				}
				if l == dedupBatchSize {
					processBatch = true
				}
			case <-timer.C:
				if len(batch) > 0 {
					processBatch = true
				}
			}
			if processBatch {
				c.dedupBatch(batch)
				batch = batch[:0]
			}
		}
	}()
}

func (c *cataloger) dedupBatch(batch []*dedupRequest) {
	ctx := context.Background()
	dedupBatchSizeHistogram.Observe(float64(len(batch)))
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		addresses := make([]string, len(batch))
		for i, r := range batch {
			repoID, err := c.getRepositoryIDCache(tx, r.Repository)
			if err != nil {
				return nil, err
			}

			// add dedup record
			res, err := tx.Exec(`INSERT INTO catalog_object_dedup (repository_id, dedup_id, physical_address) values ($1, decode($2,'hex'), $3)
				ON CONFLICT DO NOTHING`,
				repoID, r.DedupID, r.Entry.PhysicalAddress)
			if err != nil {
				return nil, err
			}
			rowsAffected := res.RowsAffected()
			if rowsAffected == 1 {
				// new address was added - continue
				continue
			}

			// fill the address into the right location
			err = tx.GetPrimitive(&addresses[i], `SELECT physical_address FROM catalog_object_dedup WHERE repository_id=$1 AND dedup_id=decode($2,'hex')`,
				repoID, r.DedupID)
			if err != nil {
				return nil, err
			}

			// update the entry with new address physical address
			_, err = tx.Exec(`UPDATE catalog_entries SET physical_address=$2 WHERE ctid=$1 AND physical_address=$3`,
				r.EntryCTID, addresses[i], r.Entry.PhysicalAddress)
			if err != nil {
				return nil, err
			}
		}
		return addresses, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		c.log.WithError(err).Errorf("Dedup batch failed (%d requests)", len(batch))
		return
	}

	// call callbacks for each entry we updated
	if c.dedupReportEnabled {
		addresses := res.([]string)
		for i, r := range batch {
			if addresses[i] == "" {
				continue
			}
			report := &catalog.DedupReport{
				Timestamp:          time.Now(),
				Repository:         r.Repository,
				StorageNamespace:   r.StorageNamespace,
				Entry:              r.Entry,
				DedupID:            r.DedupID,
				NewPhysicalAddress: addresses[i],
			}
			select {
			case c.dedupReportCh <- report:
			default:
				dedupRemoveObjectDroppedCounter.Inc()
			}
		}
	}
}

func (c *cataloger) Hooks() *catalog.CatalogerHooks {
	return &c.hooks
}

func (c *cataloger) CreateTag(context.Context, string, string, string) error {
	return catalog.ErrFeatureNotSupported
}

func (c *cataloger) DeleteTag(context.Context, string, string) error {
	return catalog.ErrFeatureNotSupported
}

func (c *cataloger) ListTags(context.Context, string, int, string) ([]*catalog.Tag, bool, error) {
	return nil, false, catalog.ErrFeatureNotSupported
}

func (c *cataloger) GetTag(context.Context, string, string) (string, error) {
	return "", catalog.ErrFeatureNotSupported
}
