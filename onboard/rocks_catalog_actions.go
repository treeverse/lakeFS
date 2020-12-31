package onboard

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
)

// RocksCatalogRepoActions is in-charge of importing data to lakeFS with Rocks implementation
type RocksCatalogRepoActions struct {
	repoID    graveler.RepositoryID
	committer string
	logger    logging.Logger

	entryCatalog       *rocks.EntryCatalog
	createdMetaRangeID *graveler.MetaRangeID
	progress           *cmdutils.Progress
	commit             *cmdutils.Progress
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
		progress:     cmdutils.NewActiveProgress("Objects imported", cmdutils.Spinner),
		commit:       cmdutils.NewActiveProgress("Commit progress", cmdutils.Spinner),
	}
}

var ErrWrongIterator = errors.New("rocksCatalogRepoActions can only accept InventoryIterator")

func (c *RocksCatalogRepoActions) ApplyImport(ctx context.Context, it Iterator, dryRun bool) (*Stats, error) {
	c.logger.Trace("start apply import")
	c.progress.Activate()

	invIt, ok := it.(*InventoryIterator)
	if !ok {
		return nil, ErrWrongIterator
	}

	var err error
	c.createdMetaRangeID, err = c.entryCatalog.WriteMetaRange(ctx, c.repoID, NewValueToEntryIterator(invIt, c.progress))
	if err != nil {
		return nil, fmt.Errorf("write meta range: %w", err)
	}

	c.progress.SetCompleted(true)
	return &Stats{
		AddedOrChanged: int(c.progress.Current()),
	}, nil
}

func (c *RocksCatalogRepoActions) GetPreviousCommit(ctx context.Context) (commit *catalog.CommitLog, err error) {
	// always build the metarange from scratch
	return nil, nil
}

var ErrNoMetaRange = errors.New("nothing to commit - meta-range wasn't created")

func (c *RocksCatalogRepoActions) Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) (string, error) {
	c.commit.Activate()
	defer c.commit.SetCompleted(true)

	if c.createdMetaRangeID == nil {
		return "", ErrNoMetaRange
	}
	commitID, err := c.entryCatalog.CommitExistingMetaRange(ctx, c.repoID, catalog.DefaultImportBranchName, *c.createdMetaRangeID, c.committer, commitMsg, graveler.Metadata(metadata))
	if err != nil {
		return "", fmt.Errorf("creating commit from existing metarange %s: %w", *c.createdMetaRangeID, err)
	}

	return string(commitID), nil
}
