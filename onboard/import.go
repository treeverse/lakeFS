package onboard

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
)

const (
	CommitMsgTemplate       = "Import from %s"
	DefaultImportBranchName = "import-from-inventory"
)

type Importer struct {
	inventoryGenerator block.InventoryGenerator
	inventory          block.Inventory
	CatalogActions     RepoActions
	logger             logging.Logger
	progress           []*cmdutils.Progress
	prefixes           []string
}

type Config struct {
	CommitUsername     string
	InventoryURL       string
	RepositoryID       graveler.RepositoryID
	DefaultBranchID    graveler.BranchID
	InventoryGenerator block.InventoryGenerator
	Store              EntryCatalog
	CatalogActions     RepoActions
	KeyPrefixes        []string

	// BaseCommit is available only for import-plumbing command
	BaseCommit graveler.CommitID
}

type Stats struct {
	AddedOrChanged     int
	DryRun             bool
	CommitRef          string
	PreviousImportDate time.Time
}

func CreateImporter(ctx context.Context, logger logging.Logger, config *Config) (importer *Importer, err error) {
	res := &Importer{
		inventoryGenerator: config.InventoryGenerator,
		logger:             logger,
		CatalogActions:     config.CatalogActions,
	}

	if res.CatalogActions == nil {
		res.CatalogActions = NewCatalogRepoActions(config, logger)
	}

	if err := res.CatalogActions.Init(ctx, config.BaseCommit); err != nil {
		return nil, fmt.Errorf("init catalog actions: %w", err)
	}

	res.inventory, err = config.InventoryGenerator.GenerateInventory(ctx, logger, config.InventoryURL, true, config.KeyPrefixes)
	res.prefixes = config.KeyPrefixes
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Importer) Import(ctx context.Context, dryRun bool) (*Stats, error) {
	var dataToImport Iterator
	var err error
	it := s.inventory.Iterator()
	// no previous commit, add whole inventory
	dataToImport = NewInventoryIterator(it)

	s.progress = append(dataToImport.Progress(), s.CatalogActions.Progress()...)
	stats, err := s.CatalogActions.ApplyImport(ctx, dataToImport, dryRun)
	if err != nil {
		return nil, err
	}
	stats.DryRun = dryRun
	if !dryRun {
		commitMetadata := CreateCommitMetadata(s.inventory, *stats, s.prefixes)
		stats.CommitRef, err = s.CatalogActions.Commit(ctx, fmt.Sprintf(CommitMsgTemplate, s.inventory.SourceName()), commitMetadata)
		if err != nil {
			return nil, err
		}
	}
	return stats, nil
}

func (s *Importer) Progress() []*cmdutils.Progress {
	return s.progress
}
