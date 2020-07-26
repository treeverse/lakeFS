package onboard

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
)

const (
	DefaultBranchName = "import-from-inventory"
	CommitMsgTemplate = "Import from %s"
	DefaultBatchSize  = 500
)

type Importer struct {
	repository         string
	batchSize          int
	inventoryGenerator block.InventoryGenerator
	inventory          block.Inventory
	InventoryDiffer    func(leftInv []block.InventoryObject, rightInv []block.InventoryObject) *InventoryDiff
	CatalogActions     RepoActions
}

func CreateImporter(cataloger catalog.Cataloger, inventoryGenerator block.InventoryGenerator, username string, inventoryURL string, repository string) (importer *Importer, err error) {
	res := &Importer{
		repository:         repository,
		batchSize:          DefaultBatchSize,
		inventoryGenerator: inventoryGenerator,
		InventoryDiffer:    CalcDiff,
	}
	res.inventory, err = inventoryGenerator.GenerateInventory(inventoryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create inventory: %w", err)
	}
	res.CatalogActions = NewCatalogActions(cataloger, repository, username, DefaultBatchSize)
	return res, nil
}

func (s *Importer) diffFromCommit(ctx context.Context, commit catalog.CommitLog) (diff *InventoryDiff, err error) {
	previousInventoryURL := ExtractInventoryURL(commit.Metadata)
	if previousInventoryURL == "" {
		err = fmt.Errorf("no inventory_url in commit Metadata. commit_ref=%s", commit.Reference)
		return
	}
	previousInv, err := s.inventoryGenerator.GenerateInventory(previousInventoryURL)
	if err != nil {
		err = fmt.Errorf("failed to create inventory for previous state: %w", err)
		return
	}
	previousObjs, err := previousInv.Objects(ctx, true)
	if err != nil {
		return
	}
	currentObjs, err := s.inventory.Objects(ctx, true)
	if err != nil {
		return
	}
	diff = s.InventoryDiffer(previousObjs, currentObjs)
	diff.PreviousInventoryURL = previousInventoryURL
	diff.PreviousImportDate = commit.CreationDate
	return
}

func (s *Importer) Import(ctx context.Context, dryRun bool) (*InventoryDiff, error) {
	diff, err := s.dataToImport(ctx)
	if err != nil {
		return nil, err
	}
	diff.DryRun = dryRun
	if dryRun {
		return diff, nil
	}
	err = s.CatalogActions.CreateAndDeleteObjects(ctx, diff.AddedOrChanged, diff.Deleted)
	if err != nil {
		return nil, err
	}
	commitMetadata := CreateCommitMetadata(s.inventory, *diff)
	err = s.CatalogActions.Commit(ctx, fmt.Sprintf(CommitMsgTemplate, s.inventory.SourceName()), commitMetadata)
	if err != nil {
		return nil, err
	}
	return diff, nil
}

func (s *Importer) dataToImport(ctx context.Context) (diff *InventoryDiff, err error) {
	var commit *catalog.CommitLog
	commit, err = s.CatalogActions.GetPreviousCommit(ctx)
	if err != nil {
		return
	}
	if commit == nil {
		// no previous commit, add whole inventory
		var objects []block.InventoryObject
		objects, err = s.inventory.Objects(ctx, false)
		if err != nil {
			return
		}
		diff = &InventoryDiff{AddedOrChanged: objects}
	} else {
		// has previous commit, add/delete according to diff
		diff, err = s.diffFromCommit(ctx, *commit)
		if err != nil {
			return nil, err
		}
	}
	return
}
