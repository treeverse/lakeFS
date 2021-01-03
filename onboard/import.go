package onboard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/logging"
)

const CommitMsgTemplate = "Import from %s"

type Importer struct {
	repository         string
	inventoryGenerator block.InventoryGenerator
	inventory          block.Inventory
	CatalogActions     RepoActions
	logger             logging.Logger
	previousCommit     *catalog.CommitLog
	progress           []*cmdutils.Progress
	prefixes           []string
	rocks              bool
}

type Config struct {
	CommitUsername     string
	InventoryURL       string
	Repository         string
	InventoryGenerator block.InventoryGenerator
	Cataloger          catalog.Cataloger
	CatalogActions     RepoActions
	KeyPrefixes        []string
	Rocks              bool
}

type Stats struct {
	AddedOrChanged       int
	Deleted              int
	DryRun               bool
	PreviousInventoryURL string
	CommitRef            string
	PreviousImportDate   time.Time
}

var (
	ErrNoInventoryURL           = errors.New("no inventory_url in commit Metadata")
	ErrInventoryAlreadyImported = errors.New("given inventory was already imported")
	ErrIncompatiblePrefixes     = errors.New("prefix filter should cover at least the same keys as the previous import")
)

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
	shouldSort := res.previousCommit != nil
	res.inventory, err = config.InventoryGenerator.GenerateInventory(ctx, logger, config.InventoryURL, shouldSort, config.KeyPrefixes)
	res.prefixes = config.KeyPrefixes
	if !res.validatePrefixes() {
		return nil, ErrIncompatiblePrefixes
	}
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
	if previousInventoryURL == s.inventory.InventoryURL() {
		return nil, fmt.Errorf("%w. commit_ref=%s", ErrInventoryAlreadyImported, commit.Reference)
	}
	previousPrefixes := ExtractPrefixes(commit.Metadata)
	previousInv, err := s.inventoryGenerator.GenerateInventory(ctx, s.logger, previousInventoryURL, true, previousPrefixes)
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
	if s.rocks || s.previousCommit == nil {
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
		commitMetadata := CreateCommitMetadata(s.inventory, *stats, s.prefixes)
		stats.CommitRef, err = s.CatalogActions.Commit(ctx, fmt.Sprintf(CommitMsgTemplate, s.inventory.SourceName()), commitMetadata)
		if err != nil {
			return nil, err
		}
	}
	return stats, nil
}

// validatePrefixes validates that the new set of prefixes covers all strings covered by the previous set.
func (s *Importer) validatePrefixes() bool {
	if s.previousCommit == nil {
		return true
	}
	previousPrefixes := ExtractPrefixes(s.previousCommit.Metadata)
	for _, p1 := range previousPrefixes {
		ok := false
		for _, p2 := range s.prefixes {
			if strings.HasPrefix(p1, p2) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func (s *Importer) Progress() []*cmdutils.Progress {
	return s.progress
}
