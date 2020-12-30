package onboard

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const (
	DefaultWriteBatchSize = 25000
	DefaultWorkerCount    = 16
	TaskChannelCapacity   = 2 * DefaultWorkerCount
)

type RepoActions interface {
	cmdutils.ProgressReporter
	ApplyImport(ctx context.Context, it Iterator, dryRun bool) (*Stats, error)
	GetPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error)
	Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) (*catalog.CommitLog, error)
}

type MVCCCatalogRepoActions struct {
	WriteBatchSize  int
	cataloger       catalog.Cataloger
	repository      string
	committer       string
	logger          logging.Logger
	deletedProgress *cmdutils.Progress
	addedProgress   *cmdutils.Progress
	commitProgress  *cmdutils.Progress
}

func (c *MVCCCatalogRepoActions) Progress() []*cmdutils.Progress {
	return []*cmdutils.Progress{c.addedProgress, c.deletedProgress, c.commitProgress}
}

func NewCatalogActions(cataloger catalog.Cataloger, repository string, committer string, logger logging.Logger) *MVCCCatalogRepoActions {
	return &MVCCCatalogRepoActions{
		cataloger:       cataloger,
		repository:      repository,
		committer:       committer,
		logger:          logger,
		addedProgress:   cmdutils.NewActiveProgress("Objects Added or Changed", cmdutils.Spinner),
		deletedProgress: cmdutils.NewActiveProgress("Objects Deleted", cmdutils.Spinner),
		commitProgress:  cmdutils.NewProgress("Committing Changes", cmdutils.SpinnerNoCounter),
	}
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

func (c *MVCCCatalogRepoActions) ApplyImport(ctx context.Context, it Iterator, dryRun bool) (*Stats, error) {
	c.logger.Trace("start apply import")
	var stats Stats
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
			stats.Deleted += 1
			if !dryRun {
				err := c.cataloger.DeleteEntry(ctx, c.repository, catalog.DefaultImportBranchName, obj.Key)
				if err != nil {
					return nil, fmt.Errorf("failed to delete entry: %s (%w)", obj.Key, err)
				}
			}
			c.deletedProgress.Incr()
			continue
		}
		entry := catalog.Entry{
			Path:            obj.Key,
			PhysicalAddress: obj.PhysicalAddress,
			CreationDate:    *obj.LastModified,
			Size:            obj.Size,
			Checksum:        obj.Checksum,
		}
		currentBatch = append(currentBatch, entry)
		stats.AddedOrChanged += 1
		if len(currentBatch) >= batchSize {
			c.logger.Tracef("closing batch of %d entries", len(currentBatch))
			previousBatch := currentBatch
			currentBatch = make([]catalog.Entry, 0, batchSize)
			if dryRun {
				c.addedProgress.Add(int64(len(previousBatch)))
				continue
			}
			tsk := &task{
				f: func() error {
					c.logger.Tracef("writing %d entries to database", len(previousBatch))
					err := c.cataloger.CreateEntries(ctx, c.repository, catalog.DefaultImportBranchName, previousBatch)
					if err == nil {
						c.addedProgress.Add(int64(len(previousBatch)))
					}
					return err
				},
				err: new(error),
			}
			errs = append(errs, tsk.err)
			tasksChan <- tsk
		}
	}
	c.logger.Trace("closing import task channel")
	close(tasksChan)
	c.logger.Trace("waiting for import wait group")
	wg.Wait()
	if it.Err() != nil {
		return nil, it.Err()
	}
	for _, err := range errs {
		if *err != nil {
			return nil, *err
		}
	}
	if len(currentBatch) > 0 && !dryRun {
		err := c.cataloger.CreateEntries(ctx, c.repository, catalog.DefaultImportBranchName, currentBatch)
		if err != nil {
			return nil, fmt.Errorf("failed to create batch of %d entries (%w)", len(currentBatch), err)
		}
	}
	c.addedProgress.Add(int64(len(currentBatch)))
	c.addedProgress.SetCompleted(true)
	c.deletedProgress.SetCompleted(true)
	return &stats, nil
}

func (c *MVCCCatalogRepoActions) GetPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error) {
	branchRef, err := c.cataloger.GetBranchReference(ctx, c.repository, catalog.DefaultImportBranchName)
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		return nil, err
	}
	if err == nil && branchRef != "" {
		commit, err = c.cataloger.GetCommit(ctx, c.repository, branchRef)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			return
		}
		if err == nil && commit != nil && commit.Committer == catalog.DefaultCommitter {
			// branch initial commit, ignore
			return nil, nil
		}
	}
	return commit, nil
}

func (c *MVCCCatalogRepoActions) Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) (*catalog.CommitLog, error) {
	c.commitProgress.Activate()
	res, err := c.cataloger.Commit(ctx, c.repository, catalog.DefaultImportBranchName,
		commitMsg,
		c.committer,
		metadata)
	if err == nil {
		c.commitProgress.SetCompleted(true)
	}
	return res, err
}
