package onboard

import (
	"context"
	"errors"
	errors2 "github.com/pkg/errors"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"time"
)

const DefaultBatchSize = 500

type ICatalogActions interface {
	createAndDeleteObjects(ctx context.Context, objects []InventoryObject, objectsToDelete []InventoryObject) (err error)
	getPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error)
	commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) error
}

type CatalogActions struct {
	cataloger  catalog.Cataloger
	batchSize  int
	repository string
}

func NewCatalogActions(cataloger catalog.Cataloger, repository string) ICatalogActions {
	return &CatalogActions{cataloger: cataloger, batchSize: DefaultBatchSize, repository: repository}
}

func (c *CatalogActions) createAndDeleteObjects(ctx context.Context, objects []InventoryObject, objectsToDelete []InventoryObject) (err error) {
	currentBatch := make([]catalog.Entry, 0, c.batchSize)
	for _, row := range objects {
		if row.Error != nil {
			return errors2.Errorf("failed to read row from inventory: %v", row.Error)
		}
		entry := catalog.Entry{
			Path:            row.Key,
			PhysicalAddress: "s3://" + row.Bucket + "/" + row.Key,
			CreationDate:    time.Unix(0, row.LastModified*int64(time.Millisecond)),
			Size:            *row.Size,
			Checksum:        row.ETag,
		}
		currentBatch = append(currentBatch, entry)
		if len(currentBatch) >= c.batchSize {
			err = c.cataloger.CreateEntries(ctx, c.repository, LauncherBranchName, currentBatch)
			if err != nil {
				return errors2.Errorf("failed to create batch of %d entries (%v)", len(currentBatch), err)
			}
			currentBatch = make([]catalog.Entry, 0, c.batchSize)
		}
	}
	if len(currentBatch) > 0 {
		err = c.cataloger.CreateEntries(ctx, c.repository, LauncherBranchName, currentBatch)
		if err != nil {
			return errors2.Errorf("failed to create batch of %d entries (%v)", len(currentBatch), err)
		}
	}
	for _, row := range objectsToDelete {
		err = c.cataloger.DeleteEntry(ctx, c.repository, LauncherBranchName, row.Key)
		if err != nil {
			return errors2.Errorf("failed to delete entry %s: %v", row.Key, err)
		}
	}
	return nil
}

func (c *CatalogActions) getPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error) {
	branchRef, err := c.cataloger.GetBranchReference(ctx, c.repository, LauncherBranchName)
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		return nil, err
	}
	if err == nil && branchRef != "" {
		commit, err = c.cataloger.GetCommit(ctx, c.repository, branchRef)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			return nil, err
		}
	}
	return commit, nil
}

func (c *CatalogActions) commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) error {
	_, err := c.cataloger.Commit(ctx, c.repository, LauncherBranchName,
		commitMsg,
		"lakeFS", // TODO get actual user
		metadata)
	return err
}
