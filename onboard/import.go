package onboard

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/catalog"
	"strconv"
)

const (
	LauncherBranchName        = "launcher"
	LauncherCommitMsgTemplate = "Import from %s"
)

type Importer struct {
	s3               s3iface.S3API
	repository       string
	inventory        IInventory
	batchSize        int
	inventoryCreator func(s3 s3iface.S3API, manifest *Manifest) IInventory
	inventoryDiffer  func(leftInv []InventoryObject, rightInv []InventoryObject) Diff
	catalogActions   ICatalogActions
}

func CreateImporter(s3 s3iface.S3API, cataloger catalog.Cataloger, manifestURL string, repository string) (*Importer, error) {
	manifest, err := LoadManifest(manifestURL, s3)
	if err != nil {
		return nil, err
	}
	res := &Importer{s3: s3, repository: repository, inventory: NewInventory(s3, manifest), batchSize: DefaultBatchSize, inventoryCreator: NewInventory, inventoryDiffer: CalcDiff}
	res.catalogActions = NewCatalogActions(cataloger, repository)
	return res, nil
}

func (s *Importer) diffFromCommit(ctx context.Context, commit catalog.CommitLog) (diff Diff, err error) {
	previousManifestURL := commit.Metadata["manifest_url"]
	if previousManifestURL == "" {
		err = fmt.Errorf("no manifest_url in commit Metadata. commit_ref=%s", commit.Reference)
		return
	}
	previousManifest, err := LoadManifest(previousManifestURL, s.s3)
	if err != nil {
		err = fmt.Errorf("failed to load manifest for previous state, manifest url: %s", previousManifestURL)
		return
	}
	previousInv := s.inventoryCreator(s.s3, previousManifest)
	err = previousInv.Fetch(ctx, true)
	if err != nil {
		return
	}
	err = s.inventory.Fetch(ctx, true)
	if err != nil {
		return
	}
	diff = s.inventoryDiffer(previousInv.Objects(), s.inventory.Objects())
	return
}

func (s *Importer) createMetadata(addedCount, deletedCount int) catalog.Metadata {
	return catalog.Metadata{
		"manifest_url":             s.inventory.Manifest().URL,
		"source_bucket":            s.inventory.Manifest().SourceBucket,
		"added_or_changed_objects": strconv.Itoa(addedCount),
		"deleted_objects":          strconv.Itoa(deletedCount),
	}
}

func (s *Importer) Import(ctx context.Context) error {
	var rows, rowsToDelete []InventoryObject
	var diff Diff
	commit, err := s.catalogActions.getPreviousCommit(ctx)
	if err != nil {
		return err
	}
	if commit == nil {
		// no previous commit, add whole inventory
		err = s.inventory.Fetch(ctx, false)
		if err != nil {
			return err
		}
		rows = s.inventory.Objects()
	} else {
		// has previous commit, add/delete according to diff
		diff, err = s.diffFromCommit(ctx, *commit)
		if err != nil {
			return err
		}
		rows = diff.AddedOrChanged
		rowsToDelete = diff.Deleted
	}
	err = s.catalogActions.createAndDeleteObjects(ctx, rows, rowsToDelete)
	if err != nil {
		return err
	}
	commitMetadata := s.createMetadata(len(rows), len(rowsToDelete))
	return s.catalogActions.commit(ctx, fmt.Sprintf(LauncherCommitMsgTemplate, s.inventory.Manifest().SourceBucket), commitMetadata)
}
