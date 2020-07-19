package onboard

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"time"
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
	InventoryDiffer    func(leftInv <-chan *block.InventoryObject, rightInv <-chan *block.InventoryObject) <-chan *ObjectImport
	CatalogActions     RepoActions
}

type InventoryImportStats struct {
	AddedOrChanged       int
	Deleted              int
	DryRun               bool
	PreviousInventoryURL string
	PreviousImportDate   time.Time
}

type ObjectImport struct {
	Obj      block.InventoryObject
	ToDelete bool
}

type InventoryImport struct {
	channel    <-chan *ObjectImport
	errChannel <-chan error
	stats      InventoryImportStats
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

func (s *Importer) diffFromCommit(ctx context.Context, commit catalog.CommitLog) (*InventoryImport, error) {
	previousInventoryURL := ExtractInventoryURL(commit.Metadata)
	if previousInventoryURL == "" {
		return nil, fmt.Errorf("no inventory_url in commit Metadata. commit_ref=%s", commit.Reference)
	}
	previousInv, err := s.inventoryGenerator.GenerateInventory(previousInventoryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create inventory for previous state: %w", err)
	}
	previousObjs, err := previousInv.Objects(ctx)
	if err != nil {
		return nil, err
	}
	currentObjs, err := s.inventory.Objects(ctx)
	if err != nil {
		return nil, err
	}
	return &InventoryImport{
		channel: s.InventoryDiffer(previousObjs, currentObjs),
		stats: InventoryImportStats{
			PreviousInventoryURL: previousInventoryURL,
			PreviousImportDate:   commit.CreationDate,
		},
	}, nil

}

func (s *Importer) Import(ctx context.Context, dryRun bool) (*InventoryImportStats, error) {
	inventoryImport, err := s.dataToImport(ctx)
	if err != nil {
		return nil, err
	}
	if dryRun {
		for objectImport := range inventoryImport.channel {
			if objectImport.ToDelete {
				inventoryImport.stats.Deleted += 1
			} else {
				inventoryImport.stats.AddedOrChanged += 1
			}
		}
		inventoryImport.stats.DryRun = true
		return &inventoryImport.stats, nil
	}
	stats, err := s.CatalogActions.CreateAndDeleteObjects(ctx, inventoryImport.channel)
	if err != nil {
		return nil, err
	}
	commitMetadata := CreateCommitMetadata(s.inventory, *stats)
	err = s.CatalogActions.Commit(ctx, fmt.Sprintf(CommitMsgTemplate, s.inventory.SourceName()), commitMetadata)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (s *Importer) dataToImport(ctx context.Context) (*InventoryImport, error) {
	commit, err := s.CatalogActions.GetPreviousCommit(ctx)
	if err != nil {
		return nil, err
	}
	var in <-chan *block.InventoryObject
	if commit == nil {
		out := make(chan *ObjectImport)
		// no previous commit, add whole inventory
		in, err = s.inventory.Objects(ctx)
		if err != nil {
			return nil, err
		}
		go func() {
			defer close(out)
			for o := range in {
				out <- &ObjectImport{
					Obj: *o,
				}
			}
		}()
		return &InventoryImport{channel: out}, nil
	} else {
		inventoryImport, err := s.diffFromCommit(ctx, *commit)
		if err != nil {
			return nil, err
		}
		return inventoryImport, nil
	}
}
