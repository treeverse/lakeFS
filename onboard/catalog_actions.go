package onboard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const DefaultWriteBatchSize = 25000

type RepoActions interface {
	ApplyImport(ctx context.Context, it Iterator, dryRun bool) (*InventoryImportStats, error)
	GetPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error)
	Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) error
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
	err error
}

func worker(wg *sync.WaitGroup, tasks <-chan *task) {
	for task := range tasks {
		task.err = task.f()
		wg.Done()
	}
}

func (c *CatalogRepoActions) ApplyImport(ctx context.Context, it Iterator, dryRun bool) (*InventoryImportStats, error) {
	var stats InventoryImportStats
	var wg sync.WaitGroup
	batchSize := DefaultWriteBatchSize
	if c.WriteBatchSize > 0 {
		batchSize = c.WriteBatchSize
	}
	tasks := make([]*task, 0)
	tasksChan := make(chan *task)
	currentBatch := make([]catalog.Entry, 0, batchSize)
	for w := 0; w < 8; w++ {
		go worker(&wg, tasksChan)
	}
	for it.Next() {
		diffObj := it.Get()
		obj := diffObj.Obj
		if diffObj.IsDeleted {
			stats.Deleted += 1
			if !dryRun {
				err := c.cataloger.DeleteEntry(ctx, c.repository, DefaultBranchName, obj.Key)
				if err != nil {
					return nil, fmt.Errorf("failed to delete entry: %s (%w)", obj.Key, err)
				}
			}
			continue
		}
		entry := catalog.Entry{
			Path:            obj.Key,
			PhysicalAddress: obj.PhysicalAddress,
			CreationDate:    time.Unix(0, obj.LastModified*int64(time.Millisecond)),
			Size:            obj.Size,
			Checksum:        obj.Checksum,
		}
		currentBatch = append(currentBatch, entry)
		stats.AddedOrChanged += 1
		if len(currentBatch) >= batchSize {
			previousBatch := currentBatch
			currentBatch = make([]catalog.Entry, 0, batchSize)
			if dryRun {
				continue
			}
			tsk := &task{
				f: func() error {
					c.logger.Info("starting batch of create entries")
					defer c.logger.Info("finished batch of create entries")
					return c.cataloger.CreateEntries(ctx, c.repository, DefaultBranchName, previousBatch)
				},
			}
			tasks = append(tasks, tsk)
			wg.Add(1)
			tasksChan <- tsk
		}
	}
	close(tasksChan)
	wg.Wait()
	if it.Err() != nil {
		return nil, it.Err()
	}
	for _, t := range tasks {
		if t.err != nil {
			return nil, t.err
		}
	}
	if len(currentBatch) > 0 && !dryRun {
		c.logger.Info("starting last batch of create entries")
		err := c.cataloger.CreateEntries(ctx, c.repository, DefaultBranchName, currentBatch)
		if err != nil {
			return nil, fmt.Errorf("failed to create batch of %d entries (%w)", len(currentBatch), err)
		}
		c.logger.Info("finished last batch of create entries")
	}
	return &stats, nil
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

func (c *CatalogRepoActions) Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) error {
	_, err := c.cataloger.Commit(ctx, c.repository, DefaultBranchName,
		commitMsg,
		c.committer,
		metadata)
	return err
}
