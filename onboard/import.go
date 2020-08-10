package onboard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/logging"
)

const (
	DefaultBranchName = "import-from-inventory"
	CommitMsgTemplate = "Import from %s"
)

type Importer struct {
	repository         string
	inventoryGenerator block.InventoryGenerator
	inventory          block.Inventory
	CatalogActions     RepoActions
	logger             logging.Logger
}

type InventoryImportStats struct {
	AddedOrChanged       int
	Deleted              int
	DryRun               bool
	PreviousInventoryURL string
	PreviousImportDate   time.Time
}

var ErrNoInventoryURL = errors.New("no inventory_url in commit Metadata")

func CreateImporter(ctx context.Context, logger logging.Logger, cataloger catalog.Cataloger, inventoryGenerator block.InventoryGenerator, username string, inventoryURL string, repository string) (importer *Importer, err error) {
	res := &Importer{
		repository:         repository,
		inventoryGenerator: inventoryGenerator,
		logger:             logger,
	}
	res.inventory, err = inventoryGenerator.GenerateInventory(ctx, logger, inventoryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create inventory: %w", err)
	}
	res.CatalogActions = NewCatalogActions(cataloger, repository, username)
	return res, nil
}

func (s *Importer) diffIterator(ctx context.Context, commit catalog.CommitLog) (Iterator, error) {
	previousInventoryURL := ExtractInventoryURL(commit.Metadata)
	if previousInventoryURL == "" {
		return nil, fmt.Errorf("%w. commit_ref=%s", ErrNoInventoryURL, commit.Reference)
	}
	previousInv, err := s.inventoryGenerator.GenerateInventory(ctx, s.logger, previousInventoryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create inventory for previous state: %w", err)
	}
	previousObjs := previousInv.Iterator()
	currentObjs := s.inventory.Iterator()
	return NewDiffIterator(previousObjs, currentObjs), nil
}

func (s *Importer) Import(ctx context.Context, dryRun bool) (*InventoryImportStats, error) {
	previousCommit, err := s.CatalogActions.GetPreviousCommit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get previous commit: %w", err)
	}
	var dataToImport Iterator
	if previousCommit == nil {
		// no previous commit, add whole inventory
		it := s.inventory.Iterator()
		dataToImport = NewInventoryIterator(it)
	} else {
		dataToImport, err = s.diffIterator(ctx, *previousCommit)
		if err != nil {
			return nil, err
		}
	}
	stats, err := s.CatalogActions.ApplyImport(ctx, dataToImport, dryRun)
	if err != nil {
		return nil, err
	}
	stats.DryRun = dryRun
	if previousCommit != nil {
		stats.PreviousImportDate = previousCommit.CreationDate
		stats.PreviousInventoryURL = previousCommit.Metadata["inventory_url"]
	}
	if !dryRun {
		commitMetadata := CreateCommitMetadata(s.inventory, *stats)
		err = s.CatalogActions.Commit(ctx, fmt.Sprintf(CommitMsgTemplate, s.inventory.SourceName()), commitMetadata)
		if err != nil {
			return nil, err
		}
	}
	return stats, nil
}
