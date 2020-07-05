package catalog

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateEntryDedup(ctx context.Context, repository, branch string, entry Entry, dedupID string, dedupResultCh chan<- *DedupResult) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "path", IsValid: ValidatePath(entry.Path)},
	}); err != nil {
		return err
	}
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}
		ctid, err := insertNewEntry(tx, branchID, &entry)
		if err != nil {
			return nil, err
		}
		return ctid, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return err
	}

	// post request to dedup
	c.dedupCh <- &dedupRequest{
		Repository:    repository,
		DedupID:       dedupID,
		Entry:         &entry,
		EntryCTID:     res.(string),
		DedupResultCh: dedupResultCh,
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

func (c *cataloger) dedupBatch(batch []*dedupRequest) {
	ctx := context.Background()
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		addresses := make([]string, len(batch))
		for i, r := range batch {
			// get repository ID
			repoID, err := getRepositoryID(tx, r.Repository)
			if err != nil {
				return nil, err
			}
			// add dedup record
			res, err := tx.Exec(`INSERT INTO object_dedup (repository_id, dedup_id, physical_address) values ($1, decode($2,'hex'), $3)
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
			err = tx.Get(&addresses[i], `SELECT physical_address FROM object_dedup WHERE repository_id=$1 AND dedup_id=decode($2,'hex')`,
				repoID, r.DedupID)
			if err != nil {
				return nil, err
			}

			// update the entry with new address physical address
			_, err = tx.Exec(`UPDATE entries SET physical_address=$2 WHERE ctid=$1 AND physical_address=$3`,
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
	addresses := res.([]string)
	for i, r := range batch {
		if r.DedupResultCh != nil {
			r.DedupResultCh <- &DedupResult{
				Repository:         r.Repository,
				Entry:              r.Entry,
				DedupID:            r.DedupID,
				NewPhysicalAddress: addresses[i],
			}
		}
	}
}
