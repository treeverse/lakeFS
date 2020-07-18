package onboard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

type RepoActions interface {
	CreateAndDeleteObjects(ctx context.Context, objects []block.InventoryObject, objectsToDelete []block.InventoryObject) (err error)
	GetPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error)
	Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) error
}

type CatalogRepoActions struct {
	cataloger  catalog.Cataloger
	batchSize  int
	repository string
	committer  string
}

func NewCatalogActions(cataloger catalog.Cataloger, repository string, committer string, batchSize int) RepoActions {
	return &CatalogRepoActions{cataloger: cataloger, batchSize: batchSize, repository: repository, committer: committer}
}

func (c *CatalogRepoActions) CreateAndDeleteObjects(ctx context.Context, objects []block.InventoryObject, objectsToDelete []block.InventoryObject) (err error) {
	currentBatch := make([]catalog.Entry, 0, c.batchSize)
	for _, row := range objects {
		entry := catalog.Entry{
			Path:            row.Key,
			PhysicalAddress: row.PhysicalAddress,
			CreationDate:    time.Unix(0, row.LastModified*int64(time.Millisecond)),
			Size:            row.Size,
			Checksum:        row.Checksum,
		}
		currentBatch = append(currentBatch, entry)
		if len(currentBatch) >= c.batchSize {
			err = c.cataloger.CreateEntries(ctx, c.repository, DefaultBranchName, currentBatch)
			if err != nil {
				return fmt.Errorf("failed to create batch of %d entries (%w)", len(currentBatch), err)
			}
			currentBatch = make([]catalog.Entry, 0, c.batchSize)
		}
	}
	if len(currentBatch) > 0 {
		err = c.cataloger.CreateEntries(ctx, c.repository, DefaultBranchName, currentBatch)
		if err != nil {
			return fmt.Errorf("failed to create batch of %d entries (%w)", len(currentBatch), err)
		}
	}
	for _, row := range objectsToDelete {
		err = c.cataloger.DeleteEntry(ctx, c.repository, DefaultBranchName, row.Key)
		if err != nil {
			return fmt.Errorf("failed to delete entry %s: %w", row.Key, err)
		}
	}
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

func (c *CatalogRepoActions) Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) error {
	_, err := c.cataloger.Commit(ctx, c.repository, DefaultBranchName,
		commitMsg,
		c.committer,
		metadata)
	return err
}
