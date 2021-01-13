package onboard

//go:generate mockgen -source=rocks_catalog_actions.go -destination=mock/rocks_catalog_actions.go -package=mock

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

	metaRanger         metaRangeManager
	createdMetaRangeID *graveler.MetaRangeID
	progress           *cmdutils.Progress
	commit             *cmdutils.Progress
	commitID           graveler.CommitID
}

func (c *RocksCatalogRepoActions) Progress() []*cmdutils.Progress {
	return []*cmdutils.Progress{c.commit, c.progress}
}

// metaRangeManager is a facet for EntryCatalog for rocks import commands
type metaRangeManager interface {
	WriteMetaRange(ctx context.Context, repositoryID graveler.RepositoryID, it rocks.EntryIterator) (*graveler.MetaRangeID, error)
	CommitExistingMetaRange(ctx context.Context, repositoryID graveler.RepositoryID, parentCommitID graveler.CommitID, metaRangeID graveler.MetaRangeID, committer string, message string, metadata graveler.Metadata) (graveler.CommitID, error)
	ListEntries(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, prefix, delimiter rocks.Path) (rocks.EntryListingIterator, error)
	UpdateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error)
}

func NewRocksCatalogRepoActions(metaRanger metaRangeManager, repository graveler.RepositoryID, commitID graveler.CommitID, committer string, logger logging.Logger) *RocksCatalogRepoActions {
	return &RocksCatalogRepoActions{
		metaRanger: metaRanger,
		repoID:     repository,
		commitID:   commitID,
		committer:  committer,
		logger:     logger,

		progress: cmdutils.NewActiveProgress("Objects imported", cmdutils.Spinner),
		commit:   cmdutils.NewActiveProgress("Commit progress", cmdutils.Spinner),
	}
}

var ErrWrongIterator = errors.New("rocksCatalogRepoActions can only accept InventoryIterator")

func (c *RocksCatalogRepoActions) ApplyImport(ctx context.Context, it Iterator, commitID graveler.CommitID, prefixes []string, _ bool) (*Stats, error) {
	c.logger.Trace("start apply import")
	c.progress.Activate()

	invIt, ok := it.(*InventoryIterator)
	if !ok {
		return nil, ErrWrongIterator
	}

	listIt, err := c.metaRanger.ListEntries(ctx, c.repoID, graveler.Ref(commitID), "", catalog.DefaultPathDelimiter)
	if err != nil {
		return nil, fmt.Errorf("listing commit: %w", err)
	}

	c.createdMetaRangeID, err = c.metaRanger.WriteMetaRange(ctx, c.repoID, newPrefixMergeIterator(NewValueToEntryIterator(invIt, c.progress), listIt, prefixes))
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
	commitID, err := c.metaRanger.CommitExistingMetaRange(ctx, c.repoID, c.commitID, *c.createdMetaRangeID, c.committer, commitMsg, graveler.Metadata(metadata))
	if err != nil {
		return "", fmt.Errorf("creating commit from existing metarange %s: %w", *c.createdMetaRangeID, err)
	}

	_, err = c.metaRanger.UpdateBranch(ctx, c.repoID, catalog.DefaultImportBranchName, graveler.Ref(c.commitID))
	if err != nil {
		return "", fmt.Errorf("updating branch: %w", err)
	}

	return string(commitID), nil
}
