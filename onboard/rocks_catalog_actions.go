package onboard

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/graveler"

	"github.com/treeverse/lakefs/catalog/rocks"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/logging"
)

// RocksCatalogRepoActions is in-charge of importing data to lakeFS with Rocks implementation
// TODO: still missing progress reporting, possible diff comparison with previous commit
type RocksCatalogRepoActions struct {
	repoID    graveler.RepositoryID
	committer string
	logger    logging.Logger

	entryCatalog       *rocks.EntryCatalog
	createdMetaRangeID *graveler.MetaRangeID
}

func (c *RocksCatalogRepoActions) Progress() []*cmdutils.Progress {
	return nil
}

func NewRocksCatalogRepoActions(_ catalog.Cataloger, repository graveler.RepositoryID, committer string, logger logging.Logger) *RocksCatalogRepoActions {
	return &RocksCatalogRepoActions{
		entryCatalog: &rocks.EntryCatalog{},
		repoID:       repository,
		committer:    committer,
		logger:       logger,
	}
}

var ErrWrongIterator = errors.New("rocksCatalogRepoActions can only accept InventoryIterator")

func (c *RocksCatalogRepoActions) ApplyImport(ctx context.Context, it Iterator, dryRun bool) (*Stats, error) {
	c.logger.Trace("start apply import")
	invIt, ok := it.(*InventoryIterator)
	if !ok {
		return nil, ErrWrongIterator
	}

	var err error
	c.createdMetaRangeID, err = c.entryCatalog.WriteMetaRange(ctx, c.repoID, NewValueToEntryIterator(invIt))
	if err != nil {
		return nil, fmt.Errorf("write meta range: %w", err)
	}

	return &Stats{}, nil
}

func (c *RocksCatalogRepoActions) GetPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error) {
	// always build the metarange from scratch
	return nil, nil
}

var ErrNoMetaRange = errors.New("nothing to commit - meta-range wasn't created")

func (c *RocksCatalogRepoActions) Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) (*catalog.CommitLog, error) {
	if c.createdMetaRangeID == nil {
		return nil, ErrNoMetaRange
	}
	commitID, err := c.entryCatalog.CommitExistingMetaRange(ctx, c.repoID, catalog.DefaultImportBranchName, *c.createdMetaRangeID, c.committer, commitMsg, graveler.Metadata(metadata))
	if err != nil {
		return nil, fmt.Errorf("creating commit from existing metarange %s: %w", *c.createdMetaRangeID, err)
	}

	commit, err := c.entryCatalog.GetCommit(ctx, c.repoID, commitID)
	if err != nil {
		return nil, fmt.Errorf("getting current commit %s: %w", commitID, err)
	}
	commitLog := &catalog.CommitLog{
		Reference: commitID.String(),
		Committer: c.committer,
		Message:   commitMsg,
		Metadata:  metadata,
	}
	for _, parent := range commit.Parents {
		commitLog.Parents = append(commitLog.Parents, parent.String())
	}
	commitLog.CreationDate = commit.CreationDate

	return commitLog, nil
}
