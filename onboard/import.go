package onboard

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/catalog"
)

const (
	DefaultBranchName = "import_from_inventory"
	CommitMsgTemplate = "Import from %s"
)

type Importer struct {
	Repository       string
	Inventory        Inventory
	batchSize        int
	InventoryFactory InventoryFactory
	InventoryDiffer  func(leftInv []InventoryObject, rightInv []InventoryObject) *InventoryDiff
	CatalogActions   RepoActions
}

func CreateImporter(cataloger catalog.Cataloger, inventoryFactory InventoryFactory, inventoryURL string, repository string) (importer *Importer, err error) {
	res := &Importer{
		Repository:       repository,
		batchSize:        DefaultBatchSize,
		InventoryFactory: inventoryFactory,
		InventoryDiffer:  CalcDiff,
	}
	res.Inventory, err = inventoryFactory.NewInventory(inventoryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create inventory: %v", err)
	}
	res.CatalogActions = NewCatalogActions(cataloger, repository)
	return res, nil
}

func (s *Importer) diffFromCommit(ctx context.Context, commit catalog.CommitLog) (diff *InventoryDiff, err error) {
	previousInventoryURL := ExtractInventoryURL(commit.Metadata)
	if previousInventoryURL == "" {
		err = fmt.Errorf("no manifest_url in commit Metadata. commit_ref=%s", commit.Reference)
		return
	}
	previousInv, err := s.InventoryFactory.NewInventory(previousInventoryURL)
	if err != nil {
		err = fmt.Errorf("failed to create inventory for previous state: %v", err)
		return
	}
	err = previousInv.Fetch(ctx, true)
	if err != nil {
		return
	}
	err = s.Inventory.Fetch(ctx, true)
	if err != nil {
		return
	}
	diff = s.InventoryDiffer(previousInv.Objects(), s.Inventory.Objects())
	diff.PreviousInventoryURL = previousInventoryURL
	diff.PreviousImportDate = commit.CreationDate
	return
}

func (s *Importer) Import(ctx context.Context) (*InventoryDiff, error) {
	diff, err := s.DataToImport(ctx, false)
	if err != nil {
		return nil, err
	}
	err = s.CatalogActions.CreateAndDeleteObjects(ctx, diff.AddedOrChanged, diff.Deleted)
	if err != nil {
		return nil, err
	}
	commitMetadata := s.Inventory.CreateCommitMetadata(*diff)
	err = s.CatalogActions.Commit(ctx, fmt.Sprintf(CommitMsgTemplate, s.Inventory.SourceName()), commitMetadata)
	if err != nil {
		return nil, err
	}
	return diff, nil
}

func (s *Importer) DataToImport(ctx context.Context, dryRun bool) (diff *InventoryDiff, err error) {
	commit, err := s.CatalogActions.GetPreviousCommit(ctx)
	if err != nil {
		return
	}
	if commit == nil {
		// no previous commit, add whole inventory
		err = s.Inventory.Fetch(ctx, false)
		if err != nil {
			return
		}
		diff = &InventoryDiff{AddedOrChanged: s.Inventory.Objects()}
	} else {
		// has previous commit, add/delete according to diff
		diff, err = s.diffFromCommit(ctx, *commit)
		if err != nil {
			return nil, err
		}
		diff.DryRun = dryRun
	}
	return
}
