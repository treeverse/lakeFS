package onboard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const DefaultWriteBatchSize = 100000

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
}

func NewCatalogActions(cataloger catalog.Cataloger, repository string, committer string) RepoActions {
	return &CatalogRepoActions{cataloger: cataloger, repository: repository, committer: committer}
}

func (c *CatalogRepoActions) ApplyImport(ctx context.Context, it Iterator, dryRun bool) (*InventoryImportStats, error) {
	var stats InventoryImportStats
	batchSize := DefaultWriteBatchSize
	if c.WriteBatchSize > 0 {
		batchSize = c.WriteBatchSize
	}
	currentBatch := make([]catalog.Entry, 0, batchSize)
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
			err := c.cataloger.CreateEntries(ctx, c.repository, DefaultBranchName, previousBatch)
			if err != nil {
				return nil, fmt.Errorf("failed to create batch of %d entries (%w)", len(currentBatch), err)
			}
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}
	if len(currentBatch) > 0 && !dryRun {
		err := c.cataloger.CreateEntries(ctx, c.repository, DefaultBranchName, currentBatch)
		if err != nil {
			return nil, fmt.Errorf("failed to create batch of %d entries (%w)", len(currentBatch), err)
		}
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
