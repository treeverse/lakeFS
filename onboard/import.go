package onboard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
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
	previousCommit     *catalog.CommitLog
	progress           []*cmdutils.Progress
}

type Config struct {
	CommitUsername     string
	InventoryURL       string
	Repository         string
	InventoryGenerator block.InventoryGenerator
	Cataloger          catalog.Cataloger
	CatalogActions     RepoActions
}

type Stats struct {
	AddedOrChanged       int
	Deleted              int
	DryRun               bool
	PreviousInventoryURL string
	CommitRef            string
	PreviousImportDate   time.Time
}

var ErrNoInventoryURL = errors.New("no inventory_url in commit Metadata")

func CreateImporter(ctx context.Context, logger logging.Logger, config *Config) (importer *Importer, err error) {
	res := &Importer{
		repository:         config.Repository,
		inventoryGenerator: config.InventoryGenerator,
		logger:             logger,
		CatalogActions:     config.CatalogActions,
	}
	if res.CatalogActions == nil {
		res.CatalogActions = NewCatalogActions(config.Cataloger, config.Repository, config.CommitUsername, logger)
	}
	previousCommit, err := res.CatalogActions.GetPreviousCommit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get previous commit: %w", err)
	}
	res.previousCommit = previousCommit
	res.inventory, err = config.InventoryGenerator.GenerateInventory(ctx, logger, config.InventoryURL, res.previousCommit != nil)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Importer) diffIterator(ctx context.Context, commit catalog.CommitLog) (Iterator, error) {
	previousInventoryURL := ExtractInventoryURL(commit.Metadata)
	if previousInventoryURL == "" {
		return nil, fmt.Errorf("%w. commit_ref=%s", ErrNoInventoryURL, commit.Reference)
	}
	previousInv, err := s.inventoryGenerator.GenerateInventory(ctx, s.logger, previousInventoryURL, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create inventory for previous state: %w", err)
	}
	previousObjs := previousInv.Iterator()
	currentObjs := s.inventory.Iterator()
	return NewDiffIterator(previousObjs, currentObjs), nil
}

func (s *Importer) Import(ctx context.Context, dryRun bool) (*Stats, error) {
	var dataToImport Iterator
	var err error
	if s.previousCommit == nil {
		it := s.inventory.Iterator()
		// no previous commit, add whole inventory
		dataToImport = NewInventoryIterator(it)
	} else {
		dataToImport, err = s.diffIterator(ctx, *s.previousCommit)
		if err != nil {
			return nil, err
		}
	}
	s.progress = append(dataToImport.Progress(), s.CatalogActions.Progress()...)
	stats, err := s.CatalogActions.ApplyImport(ctx, dataToImport, dryRun)
	if err != nil {
		return nil, err
	}
	stats.DryRun = dryRun
	if s.previousCommit != nil {
		stats.PreviousImportDate = s.previousCommit.CreationDate
		stats.PreviousInventoryURL = s.previousCommit.Metadata["inventory_url"]
	}
	if !dryRun {
		commitMetadata := CreateCommitMetadata(s.inventory, *stats)
		commitLog, err := s.CatalogActions.Commit(ctx, fmt.Sprintf(CommitMsgTemplate, s.inventory.SourceName()), commitMetadata)
		if err != nil {
			return nil, err
		}
		stats.CommitRef = commitLog.Reference
	}
	return stats, nil
}

func (s *Importer) Progress() []*cmdutils.Progress {
	return s.progress
}
