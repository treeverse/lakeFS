package onboard

//go:generate mockgen -source=catalog_actions.go -destination=mock/catalog_actions.go -package=mock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
)

type RepoActions interface {
	cmdutils.ProgressReporter
	ApplyImport(ctx context.Context, it Iterator, dryRun bool) (*Stats, error)
	InitBranch(ctx context.Context) error
	Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) (commitRef string, err error)
}

// CatalogRepoActions is in-charge of importing data to lakeFS with Rocks implementation
type CatalogRepoActions struct {
	repoID    graveler.RepositoryID
	committer string
	logger    logging.Logger

	entryCataloger     entryCataloger
	createdMetaRangeID *graveler.MetaRangeID
	previousCommitID   graveler.CommitID

	progress *cmdutils.Progress
	commit   *cmdutils.Progress
	prefixes []string
	branchID graveler.BranchID
}

func (c *CatalogRepoActions) Progress() []*cmdutils.Progress {
	return []*cmdutils.Progress{c.commit, c.progress}
}

// entryCataloger is a facet for EntryCatalog for rocks import commands
type entryCataloger interface {
	WriteMetaRange(ctx context.Context, repositoryID graveler.RepositoryID, it catalog.EntryIterator) (*graveler.MetaRangeID, error)
	AddCommitToBranchHead(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, commit graveler.Commit) (graveler.CommitID, error)
	ListEntries(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, prefix, delimiter catalog.Path) (catalog.EntryListingIterator, error)
	UpdateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error)
	GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error)
	CreateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error)
}

func NewCatalogRepoActions(eCataloger entryCataloger, repository graveler.RepositoryID, committer string, logger logging.Logger, prefixes []string) *CatalogRepoActions {
	return &CatalogRepoActions{
		entryCataloger: eCataloger,
		repoID:         repository,
		committer:      committer,
		logger:         logger,
		prefixes:       prefixes,
		progress:       cmdutils.NewActiveProgress("Objects imported", cmdutils.Spinner),
		commit:         cmdutils.NewActiveProgress("Commit progress", cmdutils.Spinner),
		branchID:       catalog.DefaultImportBranchName, // TODO(itai): change this to any chosen commit by the user to support plumbing
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

	// if repo is empty, it's an empty iterator
	listIt, err := c.entryCataloger.ListEntries(ctx, c.repoID, graveler.Ref(c.branchID), "", "")
	if err != nil {
		return nil, fmt.Errorf("listing commit: %w", err)
	}

	c.createdMetaRangeID, err = c.entryCataloger.WriteMetaRange(ctx, c.repoID, newPrefixMergeIterator(NewValueToEntryIterator(invIt, c.progress), listIt, c.prefixes))
	if err != nil {
		return nil, fmt.Errorf("write meta range: %w", err)
	}

	c.progress.SetCompleted(true)
	return &Stats{
		AddedOrChanged: int(c.progress.Current()),
	}, nil
}

func (c *CatalogRepoActions) InitBranch(ctx context.Context) error {
	branch, err := c.entryCataloger.GetBranch(ctx, c.repoID, catalog.DefaultImportBranchName)
	if errors.Is(err, graveler.ErrBranchNotFound) {
		// first import, let's create the branch
		branch, err = c.entryCataloger.CreateBranch(ctx, c.repoID, catalog.DefaultImportBranchName, catalog.DefaultBranchName)
		if errors.Is(err, graveler.ErrCreateBranchNoCommit) {
			// no commits in repo, merge to master
			c.branchID = catalog.DefaultBranchName
			return nil
		}
		if err != nil {
			return fmt.Errorf("creating default branch %s: %w", catalog.DefaultImportBranchName, err)
		}
	}

	c.previousCommitID = branch.CommitID

	// returning nil since Rocks doesn't use the diff iterator
	return nil
}

var ErrNoMetaRange = errors.New("nothing to commit - meta-range wasn't created")

func (c *CatalogRepoActions) Commit(ctx context.Context, commitMsg string, metadata catalog.Metadata) (string, error) {
	c.commit.Activate()
	defer c.commit.SetCompleted(true)

	if c.createdMetaRangeID == nil {
		return "", ErrNoMetaRange
	}

	commit := graveler.Commit{
		Committer:    c.committer,
		Message:      commitMsg,
		MetaRangeID:  *c.createdMetaRangeID,
		CreationDate: time.Now(),
		Metadata:     graveler.Metadata(metadata),
	}
	if c.previousCommitID != "" {
		commit.Parents = graveler.CommitParents{c.previousCommitID}
	}
	commitID, err := c.entryCataloger.AddCommitToBranchHead(ctx, c.repoID, c.branchID, commit)
	if err != nil {
		return "", fmt.Errorf("creating commit from existing metarange %s: %w", *c.createdMetaRangeID, err)
	}
	return string(commitID), nil
}
