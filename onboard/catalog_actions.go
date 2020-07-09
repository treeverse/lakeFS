package onboard

import (
	"context"
	"errors"
	"fmt"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"time"
)

const DefaultBatchSize = 500

type RepoActions interface {
	CreateAndDeleteObjects(ctx context.Context, objects []InventoryObject, objectsToDelete []InventoryObject) (err error)
	GetPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error)
	Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) error
}

type CatalogRepoActions struct {
	cataloger  catalog.Cataloger
	BatchSize  int
	repository string
}

func NewCatalogActions(cataloger catalog.Cataloger, repository string) RepoActions {
	return &CatalogRepoActions{cataloger: cataloger, BatchSize: DefaultBatchSize, repository: repository}
}

func (c *CatalogRepoActions) CreateAndDeleteObjects(ctx context.Context, objects []InventoryObject, objectsToDelete []InventoryObject) (err error) {
	currentBatch := make([]catalog.Entry, 0, c.BatchSize)
	for _, row := range objects {
		if row.Error != nil {
			return fmt.Errorf("failed to read row from inventory: %v", row.Error)
		}
		entry := catalog.Entry{
			Path:            row.Key,
			PhysicalAddress: "s3://" + row.Bucket + "/" + row.Key,
			CreationDate:    time.Unix(0, row.LastModified*int64(time.Millisecond)),
			Size:            *row.Size,
			Checksum:        row.ETag,
		}
		currentBatch = append(currentBatch, entry)
		if len(currentBatch) >= c.BatchSize {
			err = c.cataloger.CreateEntries(ctx, c.repository, DefaultBranchName, currentBatch)
			if err != nil {
				return fmt.Errorf("failed to create batch of %d entries (%v)", len(currentBatch), err)
			}
			currentBatch = make([]catalog.Entry, 0, c.BatchSize)
		}
	}
	if len(currentBatch) > 0 {
		err = c.cataloger.CreateEntries(ctx, c.repository, DefaultBranchName, currentBatch)
		if err != nil {
			return fmt.Errorf("failed to create batch of %d entries (%v)", len(currentBatch), err)
		}
	}
	for _, row := range objectsToDelete {
		err = c.cataloger.DeleteEntry(ctx, c.repository, DefaultBranchName, row.Key)
		if err != nil {
			return fmt.Errorf("failed to delete entry %s: %v", row.Key, err)
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
		"lakeFS", // TODO get actual user
		metadata)
	return err
}
