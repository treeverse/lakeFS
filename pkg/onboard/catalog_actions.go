package onboard

//go:generate mockgen -source=catalog_actions.go -destination=mock/catalog_actions.go -package=mock

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type RepoActions interface {
	cmdutils.ProgressReporter
	ApplyImport(ctx context.Context, it Iterator, dryRun bool) (*Stats, error)
	Init(ctx context.Context, baseCommit graveler.CommitID) error
	Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) (commitRef string, err error)
}

// CatalogRepoActions is in-charge of importing data to lakeFS with Rocks implementation
type CatalogRepoActions struct {
	committer string

	defaultBranchID graveler.BranchID
	repositoryID    graveler.RepositoryID
	repository      *graveler.RepositoryRecord
	logger          logging.Logger
	entryCatalog    EntryCatalog
	prefixes        []string

	createdMetaRangeID *graveler.MetaRangeID
	previousCommitID   graveler.CommitID
	branchID           graveler.BranchID

	progress *cmdutils.Progress
	commit   *cmdutils.Progress
}

func (c *CatalogRepoActions) Progress() []*cmdutils.Progress {
	return []*cmdutils.Progress{c.commit, c.progress}
}

// EntryCatalog is a facet for a catalog.Store
type EntryCatalog interface {
	GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error)
	WriteMetaRangeByIterator(ctx context.Context, repository *graveler.RepositoryRecord, it graveler.ValueIterator) (*graveler.MetaRangeID, error)
	AddCommitToBranchHead(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, commit graveler.Commit) (graveler.CommitID, error)
	List(ctx context.Context, repository *graveler.RepositoryRecord, ref graveler.Ref) (graveler.ValueIterator, error)
	AddCommit(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error)
	UpdateBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error)
	GetBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) (*graveler.Branch, error)
	CreateBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error)
	GetCommit(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) (*graveler.Commit, error)
}

func NewCatalogRepoActions(config *Config, logger logging.Logger) *CatalogRepoActions {
	return &CatalogRepoActions{
		entryCatalog:    config.Store,
		repositoryID:    config.RepositoryID,
		defaultBranchID: config.DefaultBranchID,
		committer:       config.CommitUsername,
		logger:          logger,
		prefixes:        config.KeyPrefixes,
		progress:        cmdutils.NewActiveProgress("Objects imported", cmdutils.Spinner),
		commit:          cmdutils.NewActiveProgress("Commit progress", cmdutils.Spinner),
	}
}

var ErrWrongIterator = errors.New("rocksCatalogRepoActions can only accept InventoryIterator")

func (c *CatalogRepoActions) ApplyImport(ctx context.Context, it Iterator, _ bool) (*Stats, error) {
	c.logger.Trace("start apply import")

	c.progress.Activate()

	invIt, ok := it.(*InventoryIterator)
	if !ok {
		return nil, ErrWrongIterator
	}

	// If we operate on branches and if repo is empty, it's an empty iterator.
	// If we operate on commit (using plumbing) - then we point to previous commit.
	// Otherwise, we point to the default import branch
	importBase := graveler.Ref(c.branchID)
	if importBase == "" {
		importBase = graveler.Ref(c.previousCommitID)
	}
	listIt, err := c.entryCatalog.List(ctx, c.repository, importBase)
	if err != nil {
		return nil, fmt.Errorf("listing commit: %w", err)
	}
	defer listIt.Close()

	listingIterator := catalog.NewEntryListingIterator(catalog.NewValueToEntryIterator(listIt), "", "")
	c.createdMetaRangeID, err = c.entryCatalog.WriteMetaRangeByIterator(ctx, c.repository,
		catalog.NewEntryToValueIterator(newPrefixMergeIterator(
			NewValueToEntryIterator(invIt, c.progress), listingIterator, c.prefixes)))
	if err != nil {
		return nil, fmt.Errorf("write meta range: %w", err)
	}

	c.progress.SetCompleted(true)
	return &Stats{
		AddedOrChanged: int(c.progress.Current()),
	}, nil
}

func (c *CatalogRepoActions) Init(ctx context.Context, baseCommit graveler.CommitID) error {
	repository, err := c.entryCatalog.GetRepository(ctx, c.repositoryID)
	if err != nil {
		return err
	}
	c.repository = repository
	c.previousCommitID = baseCommit
	if baseCommit == "" {
		return c.initBranch(ctx)
	}
	return nil
}

func (c *CatalogRepoActions) initBranch(ctx context.Context) error {
	c.branchID = DefaultImportBranchName
	branch, err := c.entryCatalog.GetBranch(ctx, c.repository, DefaultImportBranchName)
	if err != nil {
		if !errors.Is(err, graveler.ErrBranchNotFound) {
			return err
		}
		// first import, let's create the branch
		branch, err = c.entryCatalog.CreateBranch(ctx, c.repository, DefaultImportBranchName, graveler.Ref(c.defaultBranchID))
		if err != nil {
			return fmt.Errorf("creating default branch %s: %w", DefaultImportBranchName, err)
		}
	}

	// this could still be empty
	c.previousCommitID = branch.CommitID
	return nil
}

var ErrNoMetaRange = errors.New("nothing to commit - meta-range wasn't created")

func (c *CatalogRepoActions) Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) (string, error) {
	c.commit.Activate()
	defer c.commit.SetCompleted(true)

	if c.createdMetaRangeID == nil {
		return "", ErrNoMetaRange
	}

	commit := graveler.NewCommit()
	commit.Committer = c.committer
	commit.Message = commitMsg
	commit.MetaRangeID = *c.createdMetaRangeID
	commit.Metadata = graveler.Metadata(metadata)
	if c.previousCommitID != "" {
		previousCommit, err := c.entryCatalog.GetCommit(ctx, c.repository, c.previousCommitID)
		if err != nil {
			return "", fmt.Errorf("getting previous commit %s: %w", c.previousCommitID, err)
		}
		commit.Parents = graveler.CommitParents{c.previousCommitID}
		commit.Generation = previousCommit.Generation + 1
	}
	var commitID graveler.CommitID
	var err error
	if c.branchID != "" {
		commitID, err = c.entryCatalog.AddCommitToBranchHead(ctx, c.repository, c.branchID, commit)
		if err != nil {
			return "", fmt.Errorf("creating commit from existing metarange %s: %w", *c.createdMetaRangeID, err)
		}
	} else {
		commitID, err = c.entryCatalog.AddCommit(ctx, c.repository, commit)
		if err != nil {
			return "", fmt.Errorf("adding commit %s: %w", *c.createdMetaRangeID, err)
		}
	}

	return string(commitID), nil
}
