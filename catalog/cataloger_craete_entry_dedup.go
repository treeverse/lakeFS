package catalog

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateEntryDedup(ctx context.Context, repository, branch string, entry Entry, dedupID string, dedupResultCh chan *DedupResult) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "path", IsValid: ValidatePath(entry.Path)},
	}); err != nil {
		return err
	}
	res, err := c.runDBJob(dbJobFunc(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}
		ctid, err := insertNewEntry(tx, branchID, &entry)
		if err != nil {
			return nil, err
		}
		return ctid, nil
	}))
	if err != nil {
		return err
	}

	// post request to dedup
	if dedupID != "" {
		c.dedupCh <- &dedupRequest{
			Repository:    repository,
			DedupID:       dedupID,
			Entry:         &entry,
			EntryCTID:     res.(string),
			DedupResultCh: dedupResultCh,
		}
	}
	return nil
}

func (c *cataloger) processDedups() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		var batch []*dedupRequest
		for {
			processBatch := false
			select {
			case req, ok := <-c.dedupCh:
				if !ok {
					return
				}
				batch = append(batch, req)
				if len(batch) == dedupBatchSize {
					processBatch = true
				}
			case <-time.After(dedupBatchTimeout):
				if len(batch) > 0 {
					processBatch = true
				}
			}
			if processBatch {
				c.dedupBatch(batch)
				batch = nil
			}
		}
	}()
}
