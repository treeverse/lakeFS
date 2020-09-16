package onboard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const (
	DefaultWriteBatchSize = 2000
	DefaultWorkerCount    = 16
	TaskChannelCapacity   = 2 * DefaultWorkerCount
)

type RepoActions interface {
	ApplyImport(ctx context.Context, it Iterator, dryRun bool, stats *InventoryImportStats) error
	GetPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error)
	Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) (*catalog.CommitLog, error)
}

type CatalogRepoActions struct {
	WriteBatchSize int
	cataloger      catalog.Cataloger
	repository     string
	committer      string
	logger         logging.Logger
}

func NewCatalogActions(cataloger catalog.Cataloger, repository string, committer string, logger logging.Logger) RepoActions {
	return &CatalogRepoActions{cataloger: cataloger, repository: repository, committer: committer, logger: logger}
}

type task struct {
	f   func() error
	err *error
}

func worker(wg *sync.WaitGroup, tasks <-chan *task) {
	for task := range tasks {
		*task.err = task.f()
	}
	wg.Done()
}

func (c *CatalogRepoActions) ApplyImport(ctx context.Context, it Iterator, dryRun bool, stats *InventoryImportStats) error {
	var wg sync.WaitGroup
	batchSize := DefaultWriteBatchSize
	if c.WriteBatchSize > 0 {
		batchSize = c.WriteBatchSize
	}
	errs := make([]*error, 0)
	tasksChan := make(chan *task, TaskChannelCapacity)
	currentBatch := make([]catalog.Entry, 0, batchSize)
	for w := 0; w < DefaultWorkerCount; w++ {
		go worker(&wg, tasksChan)
	}
	wg.Add(DefaultWorkerCount)
	for it.Next() {
		diffObj := it.Get()
		obj := diffObj.Obj
		if diffObj.IsDeleted {
			if !dryRun {
				err := c.cataloger.DeleteEntry(ctx, c.repository, DefaultBranchName, obj.Key)
				if err != nil {
					return fmt.Errorf("failed to delete entry: %s (%w)", obj.Key, err)
				}
			}
			atomic.AddInt64(stats.Deleted, 1)
			continue
		}
		entry := catalog.Entry{
			Path:            obj.Key,
			PhysicalAddress: obj.PhysicalAddress,
			CreationDate:    obj.LastModified,
			Size:            obj.Size,
			Checksum:        obj.Checksum,
		}
		currentBatch = append(currentBatch, entry)
		if len(currentBatch) >= batchSize {
			previousBatch := currentBatch
			currentBatch = make([]catalog.Entry, 0, batchSize)
			if dryRun {
				atomic.AddInt64(stats.AddedOrChanged, int64(len(previousBatch)))
				continue
			}
			tsk := &task{
				f: func() error {
					err := c.cataloger.CreateEntries(ctx, c.repository, DefaultBranchName, previousBatch)
					if err == nil {
						atomic.AddInt64(stats.AddedOrChanged, int64(len(previousBatch)))
					}
					return err
				},
				err: new(error),
			}
			errs = append(errs, tsk.err)
			tasksChan <- tsk
		}
	}
	close(tasksChan)
	wg.Wait()
	if it.Err() != nil {
		return it.Err()
	}
	for _, err := range errs {
		if *err != nil {
			return *err
		}
	}
	if len(currentBatch) > 0 && !dryRun {
		err := c.cataloger.CreateEntries(ctx, c.repository, DefaultBranchName, currentBatch)
		if err != nil {
			return fmt.Errorf("failed to create batch of %d entries (%w)", len(currentBatch), err)
		}
	}
	atomic.AddInt64(stats.AddedOrChanged, int64(len(currentBatch)))
	return nil
}

func (c *CatalogRepoActions) GetPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error) {
	branchRef, err := c.cataloger.GetBranchReference(ctx, c.repository, DefaultBranchName)
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		return nil, err
	}
	if err == nil && branchRef != "" {
		commit, err = c.cataloger.GetCommit(ctx, c.repository, branchRef)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			return
		}
		if err == nil && commit != nil && commit.Committer == catalog.CatalogerCommitter {
			// branch initial commit, ignore
			return nil, nil
		}
	}
	return commit, nil
}

func (c *CatalogRepoActions) Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) (*catalog.CommitLog, error) {
	return c.cataloger.Commit(ctx, c.repository, DefaultBranchName,
		commitMsg,
		c.committer,
		metadata)
}
