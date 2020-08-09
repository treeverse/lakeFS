package catalog

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const (
	CatalogerCommitter = ""

	DefaultPathDelimiter = "/"

	dedupBatchSize         = 10
	dedupBatchTimeout      = 50 * time.Millisecond
	dedupChannelSize       = 5000
	dedupReportChannelSize = 5000

	defaultCatalogerCacheSize   = 1024
	defaultCatalogerCacheExpiry = 20 * time.Second
	defaultCatalogerCacheJitter = 5 * time.Second
)

type DedupReport struct {
	Repository         string
	StorageNamespace   string
	DedupID            string
	Entry              *Entry
	NewPhysicalAddress string
	Timestamp          time.Time
}

type DedupParams struct {
	ID               string
	StorageNamespace string
}

type ExpireResult struct {
	Repository        string
	Branch            string
	PhysicalAddress   string
	InternalReference string
}

type RepositoryCataloger interface {
	CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) error
	GetRepository(ctx context.Context, repository string) (*Repository, error)
	DeleteRepository(ctx context.Context, repository string) error
	ListRepositories(ctx context.Context, limit int, after string) ([]*Repository, bool, error)
}

type BranchCataloger interface {
	CreateBranch(ctx context.Context, repository, branch string, sourceBranch string) (*CommitLog, error)
	DeleteBranch(ctx context.Context, repository, branch string) error
	ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*Branch, bool, error)
	BranchExists(ctx context.Context, repository string, branch string) (bool, error)
	GetBranchReference(ctx context.Context, repository, branch string) (string, error)
	ResetBranch(ctx context.Context, repository, branch string) error
}

var ErrExpired = errors.New("expired from storage")

// ExpiryRows is a database iterator over ExpiryResults.  Use Next to advance from row to row.
type ExpiryRows interface {
	io.Closer
	Next() bool
	Err() error
	// Read returns the current from ExpiryRows, or an error on failure.  Call it only after
	// successfully calling Next.
	Read() (*ExpireResult, error)
}

// GetEntryParams configures what entries GetEntry returns.
type GetEntryParams struct {
	// For entries to expired objects the Expired bit is set.  If true, GetEntry returns
	// successfully for expired entries, otherwise it returns the entry with ErrExpired.
	ReturnExpired bool
}

type CreateEntryParams struct {
	Dedup DedupParams
}

type EntryCataloger interface {
	// GetEntry returns the current entry for path in repository branch reference.  Returns
	// the entry with ExpiredError if it has expired from underlying storage.
	GetEntry(ctx context.Context, repository, reference string, path string, params GetEntryParams) (*Entry, error)
	CreateEntry(ctx context.Context, repository, branch string, entry Entry, params CreateEntryParams) error
	CreateEntries(ctx context.Context, repository, branch string, entries []Entry) error
	DeleteEntry(ctx context.Context, repository, branch string, path string) error
	ListEntries(ctx context.Context, repository, reference string, prefix, after string, delimiter string, limit int) ([]*Entry, bool, error)
	ResetEntry(ctx context.Context, repository, branch string, path string) error
	ResetEntries(ctx context.Context, repository, branch string, prefix string) error

	// QueryEntriesToExpire returns ExpiryRows iterating over all objects to expire on
	// repositoryName according to policy.
	QueryEntriesToExpire(ctx context.Context, repositoryName string, policy *Policy) (ExpiryRows, error)
	// MarkEntriesExpired marks all entries identified by expire as expired.  It is a batch operation.
	MarkEntriesExpired(ctx context.Context, repositoryName string, expireResults []*ExpireResult) error
	// MarkObjectsForDeletion marks objects in catalog_object_dedup as "deleting" if all
	// their entries are expired, and returns the new total number of objects marked (or an
	// error).  These objects are not yet safe to delete: there could be a race between
	// marking objects as expired deduping newly-uploaded objects.  See
	// DeleteOrUnmarkObjectsForDeletion for that actual deletion.
	MarkObjectsForDeletion(ctx context.Context, repositoryName string) (int64, error)
	// DeleteOrUnmarkObjectsForDeletion scans objects in catalog_object_dedup for objects
	// marked "deleting" and returns an iterator over physical addresses of those objects
	// all of whose referring entries are still expired.  If called after MarkEntriesExpired
	// and MarkObjectsForDeletion this is safe, because no further entries can refer to
	// expired objects.  It also removes the "deleting" mark from those objects that have an
	// entry _not_ marked as expiring and therefore were not on the returned rows.
	DeleteOrUnmarkObjectsForDeletion(ctx context.Context, repositoryName string) (StringRows, error)

	DedupReportChannel() chan *DedupReport
}

type MultipartUpdateCataloger interface {
	CreateMultipartUpload(ctx context.Context, repository, uploadID, path, physicalAddress string, creationTime time.Time) error
	GetMultipartUpload(ctx context.Context, repository, uploadID string) (*MultipartUpload, error)
	DeleteMultipartUpload(ctx context.Context, repository, uploadID string) error
}

type Committer interface {
	Commit(ctx context.Context, repository, branch string, message string, committer string, metadata Metadata) (*CommitLog, error)
	GetCommit(ctx context.Context, repository, reference string) (*CommitLog, error)
	ListCommits(ctx context.Context, repository, branch string, fromReference string, limit int) ([]*CommitLog, bool, error)
	RollbackCommit(ctx context.Context, repository, reference string) error
}

type Differ interface {
	Diff(ctx context.Context, repository, leftBranch string, rightBranch string) (Differences, error)
	DiffUncommitted(ctx context.Context, repository, branch string) (Differences, error)
}

type MergeResult struct {
	Differences Differences
	Reference   string
}

type Merger interface {
	Merge(ctx context.Context, repository, sourceBranch, destinationBranch string, committer string, message string, metadata Metadata) (*MergeResult, error)
}

type Cataloger interface {
	RepositoryCataloger
	BranchCataloger
	EntryCataloger
	Committer
	MultipartUpdateCataloger
	Differ
	Merger
	io.Closer
}

type dedupRequest struct {
	Repository       string
	StorageNamespace string
	DedupID          string
	Entry            *Entry
	EntryCTID        string
}

type CacheConfig struct {
	Enabled bool
	Size    int
	Expiry  time.Duration
	Jitter  time.Duration
}

// cataloger main catalog implementation based on mvcc
type cataloger struct {
	clock              clock.Clock
	log                logging.Logger
	db                 db.Database
	wg                 sync.WaitGroup
	cacheConfig        *CacheConfig
	cache              Cache
	dedupCh            chan *dedupRequest
	dedupReportEnabled bool
	dedupReportCh      chan *DedupReport
}

type CatalogerOption func(*cataloger)

var defaultCatalogerCacheConfig = &CacheConfig{
	Enabled: true,
	Size:    defaultCatalogerCacheSize,
	Expiry:  defaultCatalogerCacheExpiry,
	Jitter:  defaultCatalogerCacheJitter,
}

func WithClock(newClock clock.Clock) CatalogerOption {
	return func(c *cataloger) {
		c.clock = newClock
	}
}

func WithCacheConfig(config *CacheConfig) CatalogerOption {
	return func(c *cataloger) {
		c.cacheConfig = config
	}
}

func WithDedupReportChannel(b bool) CatalogerOption {
	return func(c *cataloger) {
		c.dedupReportEnabled = b
	}
}

func NewCataloger(db db.Database, options ...CatalogerOption) Cataloger {
	c := &cataloger{
		clock:              clock.New(),
		log:                logging.Default().WithField("service_name", "cataloger"),
		db:                 db,
		cacheConfig:        defaultCatalogerCacheConfig,
		dedupCh:            make(chan *dedupRequest, dedupChannelSize),
		dedupReportEnabled: true,
	}
	for _, opt := range options {
		opt(c)
	}
	if c.cacheConfig.Enabled {
		c.cache = NewLRUCache(c.cacheConfig.Size, c.cacheConfig.Expiry, c.cacheConfig.Jitter)
	} else {
		c.cache = &DummyCache{}
	}
	if c.dedupReportEnabled {
		c.dedupReportCh = make(chan *DedupReport, dedupReportChannelSize)
	}
	c.processDedupBatches()
	return c
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
		c.wg.Wait()
		close(c.dedupReportCh)
	}
	return nil
}

func (c *cataloger) DedupReportChannel() chan *DedupReport {
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
			if rowsAffected, err := res.RowsAffected(); err != nil {
				return nil, err
			} else if rowsAffected == 1 {
				// new address was added - continue
				continue
			}

			// fill the address into the right location
			err = tx.Get(&addresses[i], `SELECT physical_address FROM catalog_object_dedup WHERE repository_id=$1 AND dedup_id=decode($2,'hex')`,
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
			report := &DedupReport{
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
